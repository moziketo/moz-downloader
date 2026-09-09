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
from fastapi import Depends, FastAPI, Header, HTTPException, status
from pydantic import BaseModel, Field, HttpUrl
from pydantic_settings import BaseSettings, SettingsConfigDict

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


app = FastAPI(title="Moziketo Downloader", version="0.2.0")


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


def _safe_key(name: str | None, *, spotify_id: str | None) -> str:
    if name:
        base = re.sub(r"[^a-zA-Z0-9._-]+", "-", name.strip()).strip("-._")
        if base:
            return base if base.endswith(".mp3") else f"{base}.mp3"
    if spotify_id:
        return f"{spotify_id}.mp3"
    return f"{uuid.uuid4().hex}.mp3"


def _ytdlp_bin(settings: Settings) -> str:
    if Path(settings.moz_ytdlp).is_file():
        return settings.moz_ytdlp
    found = shutil.which("yt-dlp") or shutil.which(settings.moz_ytdlp)
    if found:
        return found
    raise RuntimeError("yt-dlp not found")


def _spotify_api_metadata(settings: Settings, track_id: str) -> tuple[str | None, str | None]:
    if not settings.spotify_client_id or not settings.spotify_client_secret:
        return None, None
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

    if track_id:
        title, artist = _spotify_api_metadata(settings, track_id)
        if title and artist:
            return title, artist

    raise RuntimeError(
        "Could not resolve Spotify metadata — pass title and artist, "
        "or configure SPOTIFY_CLIENT_ID/SPOTIFY_CLIENT_SECRET"
    )


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
    query = f"{artist} {title}"

    out_path = dest_dir / "audio.mp3"
    cmd = [
        _ytdlp_bin(settings),
        f"ytsearch1:{query}",
        "--no-playlist",
        "-x",
        "--audio-format",
        "mp3",
        "--audio-quality",
        "0",
        "-o",
        str(out_path.with_suffix(".%(ext)s")),
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr[-800:] or proc.stdout[-800:] or "yt-dlp failed")
    if out_path.is_file():
        return out_path, title, artist
    files = sorted(dest_dir.glob("audio.*"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not files:
        raise RuntimeError("yt-dlp produced no mp3 file")
    return files[0], title, artist


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

    filename = _safe_key(key_hint, spotify_id=spotify_id)
    s3_key = f"music/{filename}"

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
