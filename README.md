# Moziketo Downloader

**Ingest service** for [موزیکتو](https://moziketo.ir) — Spotify URL → MP3 → Arvan S3 → download link.

| Repo | Role |
|------|------|
| [moziketo-wave](https://github.com/danielhej/moziketo-wave) | FastAPI backend — calls this service |
| [moz](https://github.com/thereisnofork/moz) | Next.js frontend |
| **moz-downloader** (this) | Spotify ingest + S3 upload |

## Server

| Item | Value |
|------|-------|
| Hostname | `moz-downloader` |
| IP | `130.185.120.239` |
| SSH | `ssh moz-downloader` |
| Path | `/opt/moz-downloader` |
| Port | `8787` |

## Flow

```
moziketo-wave  →  POST /v1/ingest  →  moz-downloader
                                         ↓ yt-dlp (YouTube search)
                                         ↓ Arvan S3 music/{key}.mp3
                    ← download_url ←
```

## API

All routes require header `X-Moziketo-Relay-Secret`.

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | S3 + yt-dlp + ffmpeg status |
| POST | `/v1/ingest` | Download Spotify track → S3 |

### Ingest request

```json
{
  "url": "https://open.spotify.com/track/..."
}
```

Only the Spotify URL is required — title/artist are resolved automatically via Spotify oEmbed + embed page. Optional: `key` (S3 filename), `title`, `artist` overrides.

### Response

```json
{
  "status": "ok",
  "download_url": "https://dl.moziketo.ir/music/slug.mp3",
  "presigned_url": "https://s3.ir-thr-at1.arvanstorage.ir/...",
  "s3_key": "music/slug.mp3",
  "title": "...",
  "artist": "...",
  "size_bytes": 5562117,
  "spotify_track_id": "..."
}
```

## Wave integration

Backend proxy (admin only):

```http
POST https://api.moziketo.ir/api/v1/admin/ingest/download
X-Admin-Key: ...
```

Configure in `moziketo-wave` `wave.env`:

```env
DOWNLOADER_URL=http://130.185.120.239:8787
DOWNLOADER_SECRET=<same as MOZ_RELAY_SECRET>
```

## Local dev

```bash
git clone git@github.com:danielhej/moz-downloader.git
cd moz-downloader
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # fill S3 + secret
uvicorn server:app --reload --port 8787
```

## Production (systemd)

```bash
ssh moz-downloader
sudo mkdir -p /opt/moz-downloader /var/lib/moz-downloader/cache
# CI deploys server.py + requirements; first-time:
python3 -m venv /opt/moz-downloader/venv
/opt/moz-downloader/venv/bin/pip install -r requirements.txt
cp .env.example /opt/moz-downloader/.env
sudo cp deploy/moz-downloader.service /etc/systemd/system/
sudo systemctl enable --now moz-downloader
```

## CI / Deploy

- **CI** — ruff on `server.py`
- **Deploy** — rsync to `moz-downloader` VPS, `pip install`, restart `moz-downloader` service

GitHub Secrets:

| Secret | Value |
|--------|-------|
| `DOWNLOADER_DEPLOY_HOST` | `130.185.120.239` |
| `DOWNLOADER_DEPLOY_USER` | `deploy` |
| `SSH_PRIVATE_KEY` | CI deploy key (authorized on server) |
