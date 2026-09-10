"""In-memory play jobs — download thread + stream-while-downloading."""

from __future__ import annotations

import logging
import secrets
import threading
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Literal

logger = logging.getLogger("moz-downloader.jobs")

JobStatus = Literal["starting", "streaming", "downloading", "uploading", "ready", "failed"]

MEDIA_TYPE_BY_SUFFIX = {
    ".mp3": "audio/mpeg",
    ".m4a": "audio/mp4",
    ".mp4": "audio/mp4",
    ".webm": "audio/webm",
    ".opus": "audio/opus",
    ".ogg": "audio/ogg",
}


class ByteBroadcast:
    """Thread-safe growing buffer — yt-dlp pipe feeds, HTTP stream reads in chunks."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._cv = threading.Condition(self._lock)
        self._buf = bytearray()
        self._closed = False
        self._error: str | None = None

    def feed(self, data: bytes) -> None:
        if not data:
            return
        with self._cv:
            self._buf.extend(data)
            self._cv.notify_all()

    def close(self, error: str | None = None) -> None:
        with self._cv:
            self._closed = True
            self._error = error
            self._cv.notify_all()

    def __len__(self) -> int:
        with self._lock:
            return len(self._buf)

    def iter_chunks(self, *, start: int = 0, chunk_size: int = 16384):
        offset = start
        idle = 0
        max_idle = 667
        while True:
            with self._cv:
                while offset >= len(self._buf) and not self._closed:
                    self._cv.wait(timeout=0.03)
                    idle += 1
                    if idle > max_idle:
                        raise TimeoutError("pipe stream timed out waiting for audio")
                if offset >= len(self._buf):
                    if self._error:
                        raise RuntimeError(self._error)
                    break
                end = min(offset + chunk_size, len(self._buf))
                chunk = bytes(self._buf[offset:end])
                offset = end
                idle = 0
            yield chunk


@dataclass
class PlayJob:
    job_id: str
    stream_token: str
    spotify_url: str
    spotify_id: str
    title: str
    artist: str
    s3_key: str
    temp_path: Path
    status: JobStatus = "starting"
    download_url: str | None = None
    presigned_url: str | None = None
    size_bytes: int = 0
    direct_url: str | None = None
    direct_media_type: str = "audio/mp4"
    error: str | None = None
    direct_ready: threading.Event = field(default_factory=threading.Event)
    download_done: threading.Event = field(default_factory=threading.Event)
    pipe_media_type: str = "audio/mp4"
    broadcast: ByteBroadcast = field(default_factory=lambda: ByteBroadcast())
    _thread: threading.Thread | None = field(default=None, repr=False)
    _resolve_thread: threading.Thread | None = field(default=None, repr=False)


class JobStore:
    def __init__(self) -> None:
        self._jobs: dict[str, PlayJob] = {}
        self._lock = threading.Lock()

    def create(
        self,
        *,
        spotify_url: str,
        spotify_id: str,
        title: str,
        artist: str,
        s3_key: str,
        temp_path: Path,
    ) -> PlayJob:
        job = PlayJob(
            job_id=uuid.uuid4().hex,
            stream_token=secrets.token_urlsafe(24),
            spotify_url=spotify_url,
            spotify_id=spotify_id,
            title=title,
            artist=artist,
            s3_key=s3_key,
            temp_path=temp_path,
            status="starting",
        )
        with self._lock:
            self._jobs[job.job_id] = job
        return job

    def get(self, job_id: str) -> PlayJob | None:
        with self._lock:
            return self._jobs.get(job_id)

    def verify_stream(self, job_id: str, token: str | None) -> PlayJob | None:
        job = self.get(job_id)
        if job is None or not token or token != job.stream_token:
            return None
        return job


job_store = JobStore()


def job_stem(job: PlayJob) -> Path:
    """Base path without extension — growing download may be .m4a/.webm first."""
    return job.temp_path.with_suffix("")


def find_growing_audio(stem: Path) -> Path | None:
    """Return the largest in-progress audio file for this job stem."""
    if stem.is_file() and stem.stat().st_size > 0:
        return stem
    if stem.with_suffix(".mp3").is_file():
        return stem.with_suffix(".mp3")
    candidates = [
        p
        for p in stem.parent.glob(stem.name + ".*")
        if p.is_file() and p.stat().st_size > 0 and not p.name.endswith(".part")
    ]
    if not candidates:
        return None
    return max(candidates, key=lambda p: p.stat().st_size)


def media_type_for_path(path: Path) -> str:
    return MEDIA_TYPE_BY_SUFFIX.get(path.suffix.lower(), "audio/mpeg")


def buffer_bytes(job: PlayJob) -> int:
    """Bytes available for progressive stream (pipe buffer or growing file)."""
    pipe_len = len(job.broadcast)
    if pipe_len > 0:
        return pipe_len
    audio = find_growing_audio(job_stem(job))
    if audio is None:
        return 0
    try:
        return audio.stat().st_size
    except OSError:
        return 0


def iter_pipe_or_file_chunks(
    job: PlayJob, *, start: int = 0, chunk_size: int = 16384
):
    """Prefer in-memory pipe chunks; fall back to growing file on disk."""
    if not job.download_done.is_set() or len(job.broadcast) > 0:
        try:
            yield from job.broadcast.iter_chunks(start=start, chunk_size=chunk_size)
            return
        except TimeoutError:
            if job.status == "failed":
                raise RuntimeError(job.error or "download failed") from None
    yield from iter_stream_chunks(job, start=start, chunk_size=chunk_size)


def start_resolve_thread(job: PlayJob, *, resolve_fn: Callable[[PlayJob], None]) -> None:
    def _worker() -> None:
        try:
            resolve_fn(job)
        except Exception as exc:
            logger.exception("direct resolve failed for %s", job.job_id)
            job.error = str(exc)[:500]
            if job.status == "starting":
                job.status = "failed"
        finally:
            job.direct_ready.set()

    thread = threading.Thread(
        target=_worker, name=f"moz-resolve-{job.job_id[:8]}", daemon=True
    )
    job._resolve_thread = thread
    thread.start()


def start_download_thread(
    job: PlayJob,
    *,
    download_fn: Callable[[PlayJob], None],
    upload_fn: Callable[[PlayJob], tuple[str, str | None]],
) -> None:
    def _worker() -> None:
        if job.status != "streaming":
            job.status = "downloading"
        try:
            download_fn(job)
            job.size_bytes = job.temp_path.stat().st_size
            job.status = "uploading"
            download_url, presigned_url = upload_fn(job)
            job.download_url = download_url
            job.presigned_url = presigned_url
            job.status = "ready"
        except Exception as exc:
            logger.exception("play job %s failed", job.job_id)
            job.status = "failed"
            job.error = str(exc)[:500]
        finally:
            job.download_done.set()

    thread = threading.Thread(target=_worker, name=f"moz-job-{job.job_id[:8]}", daemon=True)
    job._thread = thread
    thread.start()


def iter_stream_chunks(job: PlayJob, *, start: int = 0, chunk_size: int = 32768):
    """Yield audio bytes while the download thread writes the temp file."""
    offset = start
    idle_rounds = 0
    max_idle = 667  # ~20s at 0.03s sleep — yt-dlp extract can be slow under load
    stem = job_stem(job)

    while True:
        if job.status == "failed":
            raise RuntimeError(job.error or "download failed")

        audio_path = find_growing_audio(stem)
        if audio_path is None:
            if job.download_done.is_set():
                break
            time.sleep(0.03)
            idle_rounds += 1
            if idle_rounds > max_idle:
                raise TimeoutError("stream timed out waiting for audio")
            continue

        size = audio_path.stat().st_size
        if offset >= size:
            if job.download_done.is_set():
                break
            time.sleep(0.03)
            idle_rounds += 1
            if idle_rounds > max_idle:
                raise TimeoutError("stream stalled")
            continue

        idle_rounds = 0
        to_read = min(chunk_size, size - offset)
        with audio_path.open("rb") as fh:
            fh.seek(offset)
            data = fh.read(to_read)
        if not data:
            if job.download_done.is_set():
                break
            time.sleep(0.03)
            continue
        offset += len(data)
        yield data
