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

JobStatus = Literal["starting", "downloading", "uploading", "ready", "failed"]

MEDIA_TYPE_BY_SUFFIX = {
    ".mp3": "audio/mpeg",
    ".m4a": "audio/mp4",
    ".mp4": "audio/mp4",
    ".webm": "audio/webm",
    ".opus": "audio/opus",
    ".ogg": "audio/ogg",
}


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
    error: str | None = None
    download_done: threading.Event = field(default_factory=threading.Event)
    _thread: threading.Thread | None = field(default=None, repr=False)


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


def start_download_thread(
    job: PlayJob,
    *,
    download_fn: Callable[[PlayJob], None],
    upload_fn: Callable[[PlayJob], tuple[str, str | None]],
) -> None:
    def _worker() -> None:
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
    max_idle = 400
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
