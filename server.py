#!/usr/bin/env python3
"""Moziketo downloader — Spotify URL → MP3 → Arvan S3 → public download link."""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import re
import shutil
import subprocess
import tempfile
import urllib.parse
import urllib.request
import uuid
from functools import lru_cache
from pathlib import Path

import boto3
from botocore.config import Config
from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field, HttpUrl
from pydantic_settings import BaseSettings, SettingsConfigDict

from jobs import (
    PlayJob,
    buffer_bytes,
    find_growing_audio,
    iter_stream_chunks,
    job_stem,
    job_store,
    media_type_for_path,
    start_download_thread,
    start_resolve_thread,
)

logger = logging.getLogger("moz-downloader")

SPOTIFY_TRACK_RE = re.compile(
    r"(?:open\.spotify\.com/track/|spotify:track:)([A-Za-z0-9]+)",
    re.IGNORECASE,
)


class Settings(BaseSettings):
    model_config = SettingsConfigDict(extra="ignore")

    moz_relay_secret: str = Field(alias="MOZ_RELAY_SECRET")
    moz_public_base_url: str = Field(
        default="https://dl.moziketo.ir/music",
        alias="MOZ_PUBLIC_BASE_URL",
    )
    moz_download_dir: Path = Field(
        default=Path("/var/lib/moz-downloader/cache"),
        alias="MOZ_DOWNLOAD_DIR",
    )
    moz_ytdlp: str = Field(default="yt-dlp", alias="MOZ_YTDLP")
    spotify_client_id: str = Field(default="", alias="SPOTIFY_CLIENT_ID")
    spotify_client_secret: str = Field(default="", alias="SPOTIFY_CLIENT_SECRET")
    s3_endpoint: str = Field(alias="S3_ENDPOINT")
    s3_bucket: str = Field(alias="S3_BUCKET")
    s3_access_key: str = Field(alias="S3_ACCESS_KEY")
    s3_secret_key: str = Field(alias="S3_SECRET_KEY")
    s3_upload_acl: str = Field(default="public-read", alias="S3_UPLOAD_ACL")
    s3_presign_ttl_seconds: int = Field(default=3600, alias="S3_PRESIGN_TTL_SECONDS")


@lru_cache
def get_settings() -> Settings:
    return Settings()


class IngestRequest(BaseModel):
    url: HttpUrl
    title: str | None = Field(default=None, max_length=300)
    artist: str | None = Field(default=None, max_length=300)
    key: str | None = Field(
        default=None,
        description="S3 object basename without path, e.g. my-track.mp3",
        max_length=200,
    )


class IngestResponse(BaseModel):
    status: str = "ok"
    download_url: str
    presigned_url: str | None = None
    s3_key: str
    title: str | None = None
    artist: str | None = None
    size_bytes: int
    spotify_track_id: str | None = None


class HealthResponse(BaseModel):
    status: str
    service: str = "moz-downloader"
    s3: str
    ytdlp: bool
    ffmpeg: bool


class PlayRequest(BaseModel):
    url: HttpUrl
    title: str | None = Field(default=None, max_length=300)
    artist: str | None = Field(default=None, max_length=300)
    key: str | None = Field(default=None, max_length=200)


class PlayResponse(BaseModel):
    status: str = "starting"
    job_id: str
    stream_url: str
    status_url: str
    title: str
    artist: str
    spotify_track_id: str
    s3_key: str


class JobStatusResponse(BaseModel):
    job_id: str
    status: str
    title: str
    artist: str
    spotify_track_id: str
    s3_key: str
    size_bytes: int = 0
    buffer_bytes: int = 0
    direct_ready: bool = False
    download_url: str | None = None
    presigned_url: str | None = None
    error: str | None = None


app = FastAPI(title="Moziketo Downloader", version="0.5.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://pwa.moziketo.ir",
        "https://moziketo.ir",
        "http://localhost:3000",
        "http://127.0.0.1:3000",
        "http://localhost:8765",
        "http://127.0.0.1:8765",
    ],
    allow_methods=["GET", "POST", "OPTIONS", "HEAD"],
    allow_headers=["*"],
    expose_headers=["Accept-Ranges", "Content-Range", "Content-Length", "Content-Type"],
)


def _verify_secret(
    x_moziketo_relay_secret: str | None = Header(default=None),
) -> None:
    settings = get_settings()
    if not x_moziketo_relay_secret or x_moziketo_relay_secret != settings.moz_relay_secret:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Unauthorized")


def _s3_client(settings: Settings):
    return boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint,
        aws_access_key_id=settings.s3_access_key,
        aws_secret_access_key=settings.s3_secret_key,
        config=Config(signature_version="s3v4"),
    )


def _spotify_track_id(url: str) -> str | None:
    match = SPOTIFY_TRACK_RE.search(url)
    return match.group(1) if match else None


def _slugify(*parts: str) -> str:
    raw = "-".join(p.strip() for p in parts if p and p.strip())
    slug = re.sub(r"[^a-zA-Z0-9._-]+", "-", raw).strip("-._").lower()
    return slug or uuid.uuid4().hex


def _safe_key(
    name: str | None,
    *,
    spotify_id: str | None,
    title: str | None = None,
    artist: str | None = None,
) -> str:
    if name:
        base = re.sub(r"[^a-zA-Z0-9._-]+", "-", name.strip()).strip("-._")
        if base:
            return base if base.endswith(".mp3") else f"{base}.mp3"
    if title and artist:
        return f"{_slugify(artist, title)}.mp3"
    if spotify_id:
        return f"{spotify_id}.mp3"
    return f"{uuid.uuid4().hex}.mp3"


YTDLP_EXTRACT_ARGS = ["--extractor-args", "youtube:player_client=android,web"]


def _ytdlp_bin(settings: Settings) -> str:
    if Path(settings.moz_ytdlp).is_file():
        return settings.moz_ytdlp
    found = shutil.which("yt-dlp") or shutil.which(settings.moz_ytdlp)
    if found:
        return found
    raise RuntimeError("yt-dlp not found")


def _http_json(url: str, *, headers: dict[str, str] | None = None) -> dict:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (compatible; MoziketoDownloader/0.3)",
            "Accept": "application/json",
            **(headers or {}),
        },
    )
    with urllib.request.urlopen(req, timeout=25) as resp:
        return json.loads(resp.read())


def _spotify_oembed_metadata(url: str) -> tuple[str | None, str | None]:
    """Public Spotify oEmbed — title only (no auth, works from IR VPS)."""
    oembed_url = (
        "https://open.spotify.com/oembed?"
        + urllib.parse.urlencode({"url": url.split("?", 1)[0]})
    )
    try:
        data = _http_json(oembed_url)
    except Exception:
        return None, None
    title = str(data.get("title") or "").strip() or None
    return title, None


def _spotify_embed_metadata(track_id: str) -> tuple[str | None, str | None]:
    """Parse embed page JSON — track + artist names."""
    embed_url = f"https://open.spotify.com/embed/track/{track_id}"
    req = urllib.request.Request(
        embed_url,
        headers={"User-Agent": "Mozilla/5.0 (compatible; MoziketoDownloader/0.3)"},
    )
    try:
        with urllib.request.urlopen(req, timeout=25) as resp:
            html = resp.read().decode("utf-8", errors="ignore")
    except Exception:
        return None, None
    names = re.findall(r'"name":"([^"]+)"', html)
    if len(names) >= 2:
        return names[0].strip() or None, names[1].strip() or None
    if len(names) == 1:
        return names[0].strip() or None, None
    return None, None


def _spotify_api_metadata(settings: Settings, track_id: str) -> tuple[str | None, str | None]:
    if not settings.spotify_client_id or not settings.spotify_client_secret:
        return None, None
    try:
        token_req = urllib.request.Request(
            "https://accounts.spotify.com/api/token",
            data=b"grant_type=client_credentials",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            method="POST",
        )
        auth = base64.b64encode(
            f"{settings.spotify_client_id}:{settings.spotify_client_secret}".encode()
        ).decode()
        token_req.add_header("Authorization", f"Basic {auth}")
        with urllib.request.urlopen(token_req, timeout=20) as resp:
            token = json.loads(resp.read()).get("access_token")
        if not token:
            return None, None

        track_req = urllib.request.Request(
            f"https://api.spotify.com/v1/tracks/{track_id}",
            headers={"Authorization": f"Bearer {token}"},
        )
        with urllib.request.urlopen(track_req, timeout=20) as resp:
            data = json.loads(resp.read())
    except Exception:
        return None, None
    title = str(data.get("name") or "").strip() or None
    artists = data.get("artists") or []
    artist = str(artists[0].get("name") or "").strip() if artists else None
    return title, artist


def _resolve_metadata(
    settings: Settings,
    *,
    url: str,
    track_id: str | None,
    title_hint: str | None,
    artist_hint: str | None,
) -> tuple[str, str]:
    if title_hint and artist_hint:
        return title_hint.strip(), artist_hint.strip()

    title: str | None = title_hint.strip() if title_hint else None
    artist: str | None = artist_hint.strip() if artist_hint else None

    if not title or not artist:
        oembed_title, _ = _spotify_oembed_metadata(url)
        title = title or oembed_title

    if track_id and (not title or not artist):
        embed_title, embed_artist = _spotify_embed_metadata(track_id)
        title = title or embed_title
        artist = artist or embed_artist

    if track_id and (not title or not artist):
        api_title, api_artist = _spotify_api_metadata(settings, track_id)
        title = title or api_title
        artist = artist or api_artist

    if title and artist:
        return title, artist
    if title:
        return title, artist or "Unknown Artist"
    if track_id:
        return f"Spotify Track {track_id[:8]}", "Unknown Artist"

    raise RuntimeError("Could not resolve Spotify track metadata from URL")


def _resolve_downloaded_file(stem: Path, *, final_mp3: Path) -> Path:
    if final_mp3.is_file():
        return final_mp3
    found = find_growing_audio(stem)
    if found is None:
        raise RuntimeError("yt-dlp produced no audio file")
    return found


def _to_mp3(source: Path, dest: Path) -> None:
    if source.suffix.lower() == ".mp3":
        if source != dest:
            shutil.move(str(source), str(dest))
        return
    ffmpeg = shutil.which("ffmpeg")
    if not ffmpeg:
        shutil.move(str(source), str(dest))
        return
    proc = subprocess.run(
        [ffmpeg, "-y", "-i", str(source), "-vn", "-codec:a", "libmp3lame", "-q:a", "4", str(dest)],
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr[-800:] or "ffmpeg mp3 convert failed")
    if source != dest and source.is_file():
        source.unlink(missing_ok=True)


def _guess_direct_media_type(url: str) -> str:
    lower = url.lower()
    if "mime=audio%2Fmp4" in lower or "mime=audio/mp4" in lower or ".m4a" in lower:
        return "audio/mp4"
    if "mime=audio%2Fwebm" in lower or "mime=audio/webm" in lower or ".webm" in lower:
        return "audio/webm"
    return "audio/mp4"


def _ytdlp_resolve_direct_url(settings: Settings, *, query: str) -> tuple[str, str]:
    """Resolve YouTube CDN URL via yt-dlp -g (no download)."""
    cmd = [
        _ytdlp_bin(settings),
        f"ytsearch1:{query}",
        "--no-playlist",
        "-f",
        "140/ba/b",
        "-g",
        *YTDLP_EXTRACT_ARGS,
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=45, check=False)
    if proc.returncode != 0:
        raise RuntimeError((proc.stderr or proc.stdout or "yt-dlp resolve failed")[-800:])
    line = proc.stdout.strip().split("\n")[0].strip()
    if not line.startswith("http"):
        raise RuntimeError("yt-dlp did not return a stream URL")
    return line, _guess_direct_media_type(line)


def _resolve_direct_worker(settings: Settings, job: PlayJob) -> None:
    url, media_type = _ytdlp_resolve_direct_url(
        settings, query=f"{job.artist} {job.title}"
    )
    job.direct_url = url
    job.direct_media_type = media_type
    job.status = "streaming"
    logger.info("direct stream ready job=%s media=%s", job.job_id[:8], media_type)


def _ytdlp_to_file(settings: Settings, *, query: str, out_path: Path) -> None:
    """Blocking ingest — full mp3 for S3 upload."""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    stem = out_path.with_suffix("")
    cmd = [
        _ytdlp_bin(settings),
        f"ytsearch1:{query}",
        "--no-playlist",
        "-f",
        "ba/b",
        "--no-part",
        "-o",
        str(stem.with_suffix(".%(ext)s")),
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr[-800:] or proc.stdout[-800:] or "yt-dlp failed")
    downloaded = _resolve_downloaded_file(stem, final_mp3=out_path)
    _to_mp3(downloaded, out_path)


def _ytdlp_play_download(settings: Settings, job: PlayJob) -> None:
    """Progressive best-audio download — file grows while stream reads."""
    stem = job_stem(job)
    stem.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        _ytdlp_bin(settings),
        f"ytsearch1:{job.artist} {job.title}",
        "--no-playlist",
        "-f",
        "ba/b",
        "--no-part",
        "--concurrent-fragments",
        "4",
        "--extractor-args",
        "youtube:player_client=android,web",
        "-o",
        str(stem.with_suffix(".%(ext)s")),
    ]
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )
    _, stderr = proc.communicate()
    if proc.returncode != 0:
        raise RuntimeError((stderr or "")[-800:] or "yt-dlp failed")
    downloaded = _resolve_downloaded_file(stem, final_mp3=job.temp_path)
    _to_mp3(downloaded, job.temp_path)


def _download_spotify(
    settings: Settings,
    url: str,
    dest_dir: Path,
    *,
    title_hint: str | None,
    artist_hint: str | None,
) -> tuple[Path, str, str]:
    dest_dir.mkdir(parents=True, exist_ok=True)
    track_id = _spotify_track_id(url)
    title, artist = _resolve_metadata(
        settings,
        url=url,
        track_id=track_id,
        title_hint=title_hint,
        artist_hint=artist_hint,
    )
    out_path = dest_dir / "audio.mp3"
    _ytdlp_to_file(settings, query=f"{artist} {title}", out_path=out_path)
    return out_path, title, artist


def _download_job_file(settings: Settings, job: PlayJob) -> None:
    _ytdlp_play_download(settings, job)


def _upload_job_file(settings: Settings, job: PlayJob) -> tuple[str, str | None]:
    return _upload_s3(settings, local_path=job.temp_path, key=job.s3_key)


def _parse_range_header(range_header: str, file_size: int) -> tuple[int, int]:
    if not range_header.startswith("bytes="):
        raise ValueError("unsupported range")
    start_s, _, end_s = range_header.removeprefix("bytes=").partition("-")
    start = int(start_s) if start_s else 0
    end = int(end_s) if end_s else max(file_size - 1, start)
    return start, min(end, max(file_size - 1, start))


def _job_to_status(job: PlayJob) -> JobStatusResponse:
    return JobStatusResponse(
        job_id=job.job_id,
        status=job.status,
        title=job.title,
        artist=job.artist,
        spotify_track_id=job.spotify_id,
        s3_key=job.s3_key,
        size_bytes=job.size_bytes,
        buffer_bytes=buffer_bytes(job),
        direct_ready=bool(job.direct_url),
        download_url=job.download_url,
        presigned_url=job.presigned_url,
        error=job.error,
    )


def _upload_s3(settings: Settings, *, local_path: Path, key: str) -> tuple[str, str | None]:
    client = _s3_client(settings)
    extra: dict[str, str] = {"ContentType": "audio/mpeg"}
    acl = settings.s3_upload_acl.strip()
    if acl and acl.lower() != "private":
        extra["ACL"] = acl
    with local_path.open("rb") as fh:
        client.put_object(Bucket=settings.s3_bucket, Key=key, Body=fh, **extra)

    public = f"{settings.moz_public_base_url.rstrip('/')}/{Path(key).name}"
    presigned: str | None = None
    if acl.lower() == "private":
        presigned = client.generate_presigned_url(
            "get_object",
            Params={"Bucket": settings.s3_bucket, "Key": key},
            ExpiresIn=settings.s3_presign_ttl_seconds,
        )
    return public, presigned


def _probe_s3(settings: Settings) -> str:
    try:
        _s3_client(settings).head_bucket(Bucket=settings.s3_bucket)
        return "ok"
    except Exception:
        return "error"


def _ingest_sync(
    settings: Settings,
    url: str,
    key_hint: str | None,
    *,
    title_hint: str | None,
    artist_hint: str | None,
) -> IngestResponse:
    spotify_id = _spotify_track_id(url)
    if spotify_id is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Only Spotify track URLs are supported",
        )

    settings.moz_download_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="moz-ingest-", dir=settings.moz_download_dir) as tmp:
        tmp_path = Path(tmp)
        local_file, title, artist = _download_spotify(
            settings,
            url,
            tmp_path,
            title_hint=title_hint,
            artist_hint=artist_hint,
        )
        filename = _safe_key(key_hint, spotify_id=spotify_id, title=title, artist=artist)
        s3_key = f"music/{filename}"
        download_url, presigned_url = _upload_s3(settings, local_path=local_file, key=s3_key)

        return IngestResponse(
            download_url=download_url,
            presigned_url=presigned_url,
            s3_key=s3_key,
            title=title,
            artist=artist,
            size_bytes=local_file.stat().st_size,
            spotify_track_id=spotify_id,
        )


@app.get("/health", response_model=HealthResponse, dependencies=[Depends(_verify_secret)])
async def health() -> HealthResponse:
    settings = get_settings()
    s3_status = await asyncio.to_thread(_probe_s3, settings)
    return HealthResponse(
        status="ok" if s3_status == "ok" else "degraded",
        s3=s3_status,
        ytdlp=Path(settings.moz_ytdlp).is_file() or shutil.which("yt-dlp") is not None,
        ffmpeg=shutil.which("ffmpeg") is not None,
    )


@app.post("/v1/play", response_model=PlayResponse, dependencies=[Depends(_verify_secret)])
async def play(body: PlayRequest, request: Request) -> PlayResponse:
    """Start download in background; return stream URL immediately (~1s)."""
    settings = get_settings()
    url = str(body.url).split("?", 1)[0]
    spotify_id = _spotify_track_id(url)
    if spotify_id is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Only Spotify track URLs are supported",
        )

    try:
        title, artist = await asyncio.to_thread(
            _resolve_metadata,
            settings,
            url=url,
            track_id=spotify_id,
            title_hint=body.title,
            artist_hint=body.artist,
        )
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=str(exc)[:500]) from exc

    filename = _safe_key(body.key, spotify_id=spotify_id, title=title, artist=artist)
    s3_key = f"music/{filename}"
    jobs_dir = settings.moz_download_dir / "jobs"
    jobs_dir.mkdir(parents=True, exist_ok=True)
    temp_path = jobs_dir / f"{uuid.uuid4().hex}.mp3"

    job = job_store.create(
        spotify_url=url,
        spotify_id=spotify_id,
        title=title,
        artist=artist,
        s3_key=s3_key,
        temp_path=temp_path,
    )
    start_resolve_thread(job, resolve_fn=lambda j: _resolve_direct_worker(settings, j))
    start_download_thread(
        job,
        download_fn=lambda j: _download_job_file(settings, j),
        upload_fn=lambda j: _upload_job_file(settings, j),
    )

    base = str(request.base_url).rstrip("/")
    stream_url = f"{base}/v1/stream/{job.job_id}?token={job.stream_token}"
    status_url = f"{base}/v1/jobs/{job.job_id}"

    return PlayResponse(
        job_id=job.job_id,
        stream_url=stream_url,
        status_url=status_url,
        title=title,
        artist=artist,
        spotify_track_id=spotify_id,
        s3_key=s3_key,
    )


@app.get("/v1/jobs/{job_id}", response_model=JobStatusResponse, dependencies=[Depends(_verify_secret)])
async def job_status(job_id: str) -> JobStatusResponse:
    job = job_store.get(job_id)
    if job is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found")
    return _job_to_status(job)


def _wait_for_direct(job: PlayJob, *, timeout: float = 25.0) -> str | None:
    if job.direct_url:
        return job.direct_url
    job.direct_ready.wait(timeout=timeout)
    return job.direct_url


@app.get("/v1/stream/{job_id}")
async def stream_job(
    job_id: str,
    request: Request,
    token: str | None = Query(default=None),
):
    """Proxy YouTube CDN URL (fast) or fall back to growing file on disk."""
    job = job_store.verify_stream(job_id, token)
    if job is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Unauthorized")

    range_header = request.headers.get("range")
    filename = job.s3_key.rsplit("/", 1)[-1]
    headers = {
        "Accept-Ranges": "bytes",
        "Cache-Control": "no-cache",
        "Content-Disposition": f'inline; filename="{filename}"',
    }

    direct_url = await asyncio.to_thread(_wait_for_direct, job)
    if direct_url:
        upstream_headers = {"User-Agent": "Mozilla/5.0 (compatible; MoziketoDownloader/0.5)"}
        if range_header:
            upstream_headers["Range"] = range_header

        def _open_upstream():
            req = urllib.request.Request(direct_url, headers=upstream_headers)
            return urllib.request.urlopen(req, timeout=120)

        upstream = await asyncio.to_thread(_open_upstream)
        for name in ("Content-Range", "Content-Length", "Accept-Ranges"):
            value = upstream.headers.get(name)
            if value:
                headers[name] = value

        def _direct_gen():
            try:
                while True:
                    chunk = upstream.read(65536)
                    if not chunk:
                        break
                    yield chunk
            finally:
                upstream.close()

        return StreamingResponse(
            _direct_gen(),
            status_code=upstream.status,
            media_type=job.direct_media_type,
            headers=headers,
        )

    start = 0
    stem = job_stem(job)
    active = find_growing_audio(stem)
    if range_header and active is not None:
        try:
            start, _ = _parse_range_header(range_header, active.stat().st_size)
        except ValueError:
            start = 0

    def _file_gen():
        yield from iter_stream_chunks(job, start=start)

    media_type = media_type_for_path(active or job.temp_path)
    if range_header and start > 0:
        headers["Content-Range"] = f"bytes {start}-*/"
        return StreamingResponse(
            _file_gen(),
            status_code=206,
            media_type=media_type,
            headers=headers,
        )

    return StreamingResponse(_file_gen(), media_type=media_type, headers=headers)


@app.post("/v1/ingest", response_model=IngestResponse, dependencies=[Depends(_verify_secret)])
async def ingest(body: IngestRequest) -> IngestResponse:
    settings = get_settings()
    url = str(body.url)
    try:
        return await asyncio.to_thread(
            _ingest_sync,
            settings,
            url,
            body.key,
            title_hint=body.title,
            artist_hint=body.artist,
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("ingest failed for %s", url)
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=str(exc)[:500],
        ) from exc


def main() -> None:
    import uvicorn

    host = os.environ.get("MOZ_RELAY_HOST", "0.0.0.0")
    port = int(os.environ.get("MOZ_RELAY_PORT", "8787"))
    uvicorn.run("server:app", host=host, port=port, log_level="info")


if __name__ == "__main__":
    main()
