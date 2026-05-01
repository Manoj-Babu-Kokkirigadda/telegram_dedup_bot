# Telegram Dedup Bot

A production-grade Telegram bot that automatically detects and deletes duplicate
media in groups and channels. Supports **photos, videos, animations (GIFs),
video notes, audio files, voice messages, and documents** using SHA-256 exact
hashing, perceptual hashing (pHash) for images, video thumbnail pHash, and
audio soft fingerprinting.

---

## Supported Media Types

| Media Type         | SHA-256 | Perceptual / Secondary   | Auto-Delete | Notes |
|--------------------|---------|-------------------------|-------------|-------|
| Photos / Images    | ✅      | pHash (imagehash)       | ✅          | Configurable Hamming threshold |
| Videos             | ✅      | First-frame thumbnail pHash | ✅      | Requires ffmpeg |
| Animations (GIF)   | ✅      | First-frame thumbnail pHash | ✅      | Treated as video |
| Video notes        | ✅      | First-frame thumbnail pHash | ✅      | Circular video bubbles |
| Audio files        | ✅      | Duration + size soft fingerprint | ⚠️ Warns only | SOFT confidence — logs but does not delete |
| Voice messages     | ✅      | Duration + size soft fingerprint | ⚠️ Warns only | Same as audio |
| Documents (PDF etc)| ✅      | File name + size suspicious check | ⚠️ SOFT match only | Same name/size but different hash → logged, not deleted |
| Stickers           | ❌      | Skipped                 | ❌          | Logged and ignored per policy |

---

## Prerequisites

- **Python 3.11+** (or Docker 20.10+)
- **ffmpeg** (required for video thumbnail extraction and audio duration)
- A Telegram bot token from [@BotFather](https://t.me/BotFather)
- Bot must be added to the target group with:
  - **Delete messages** permission
  - **Read messages** permission
  - Admin status (for admin commands)

---

## Quick Start

```bash
git clone <your-repo> telegram_dedup_bot
cd telegram_dedup_bot

python -m venv .venv
source .venv/bin/activate   # On Windows: .venv\Scripts\activate
pip install -r requirements.txt

cp .env.example .env
# Edit .env and paste your BOT_TOKEN from @BotFather

python -m bot.main
```

Add the bot to a group, **promote it to admin**, and grant the
*Delete messages* permission. Send the same photo twice — the second one
is removed within a few seconds.

---

## Docker Quick Start

### SQLite (default profile)

```bash
cp .env.example .env
# Edit .env and set BOT_TOKEN
docker compose --profile sqlite up -d
```

### PostgreSQL

```bash
cp .env.example .env
# Set BOT_TOKEN, then add:
# DATABASE_URL=postgresql+asyncpg://dedup:dedup_secret@postgres/dedup
# PG_USER=dedup
# PG_PASSWORD=dedup_secret
# PG_DATABASE=dedup
docker compose --profile postgres up -d
```

---

## PostgreSQL Migration

1. Provision a PostgreSQL database (or use the docker-compose postgres service).
2. Install the async driver (already in `requirements.txt`):
   ```bash
   pip install asyncpg
   ```
3. Set the URL in `.env`:
   ```env
   DATABASE_URL=postgresql+asyncpg://user:password@host:5432/dedup
   ```
4. Run Alembic migrations:
   ```bash
   python -m alembic upgrade head
   ```
5. Restart the bot.

Migrations are idempotent — running `alembic upgrade head` twice is safe.

---

## Bot Permissions Required

The bot must be promoted to **administrator** in each group with the following permissions:

| Permission       | Required? | Purpose |
|------------------|-----------|---------|
| Delete messages  | ✅ Yes    | Remove duplicate media |
| Read messages    | ✅ Yes    | Detect incoming media |
| (All others)     | ❌ No     | Not needed |

---

## Admin Commands Reference

All commands require the caller to be a **chat administrator**. Non-admins receive:
`⛔ This command is for group administrators only.`

| Command | Description |
|---|---|
| `/dedup_on` | Enable duplicate detection for this chat |
| `/dedup_off` | Disable duplicate detection for this chat |
| `/set_threshold image <0-20>` | Set image pHash similarity threshold (lower = stricter) |
| `/set_threshold video <0-20>` | Set video thumbnail pHash similarity threshold |
| `/dedup_status` | Show: enabled/disabled, both thresholds, stored hashes, duplicates deleted |
| `/clear_hashes` | Wipe stored hashes (requires reply with `CONFIRM` within 60 seconds) |
| `/dedup_stats` | Global + per-chat stats, top 5 duplicate senders, duplicates by media type |

---

## Configuration (`.env`)

| Key | Default | Purpose |
|---|---|---|
| `BOT_TOKEN` | *(required)* | Telegram bot token from @BotFather |
| `DATABASE_URL` | `sqlite+aiosqlite:///./dedup.db` | Async SQLAlchemy URL |
| `PHASH_THRESHOLD` | `10` | Image pHash Hamming-distance threshold (0–20) |
| `VIDEO_PHASH_THRESHOLD` | `8` | Video thumbnail pHash threshold (0–20) |
| `NOTIFY_ON_DELETE` | `true` | Send chat notification when duplicate is deleted |
| `DOWNLOAD_DIR` | `./downloads` | Temp directory for media downloads |
| `LOG_LEVEL` | `INFO` | Python log level (DEBUG/INFO/WARNING/ERROR) |
| `MAX_MEDIA_PER_MINUTE` | `50` | Rate limit per chat before dedup pauses |
| `RATE_LIMIT_PAUSE_SECONDS` | `30` | Duration of pause when rate limit is hit |

---

## Deploying to a VPS with systemd

Create `/etc/systemd/system/telegram-dedup-bot.service`:

```ini
[Unit]
Description=Telegram Dedup Bot
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=botuser
WorkingDirectory=/opt/telegram_dedup_bot
EnvironmentFile=/opt/telegram_dedup_bot/.env
ExecStart=/opt/telegram_dedup_bot/.venv/bin/python -m bot.main
Restart=on-failure
RestartSec=5
KillSignal=SIGTERM
TimeoutStopSec=30

[Install]
WantedBy=multi-user.target
```

Enable and start:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now telegram-dedup-bot
sudo journalctl -u telegram-dedup-bot -f
```

---

## Project layout

```
telegram_dedup_bot/
├── bot/
│   ├── main.py                 # Entry point, logging, shutdown, heartbeat
│   ├── handlers/
│   │   ├── media.py            # Media handler + rate limiter + admin checker
│   │   └── admin.py            # Admin command handlers
│   ├── core/
│   │   ├── hasher.py           # SHA-256, pHash, video thumbnail, audio duration
│   │   ├── detector.py         # Duplicate detection orchestration (all types)
│   │   └── cache.py            # Per-chat TTL cache (sha256 + secondary)
│   └── db/
│       ├── models.py           # SQLAlchemy ORM models
│       ├── database.py         # Async engine + session factory
│       ├── crud.py             # All DB operations + duplicate_log
│       └── migrations/         # Alembic migrations
│           ├── env.py
│           └── versions/
├── tests/                      # pytest suite (20 tests)
├── downloads/                  # Transient media (git-ignored)
├── logs/                       # Rotating log files + heartbeat
├── data/                       # SQLite DB (git-ignored)
├── Dockerfile
├── docker-compose.yml
├── alembic.ini
├── .env.example
├── requirements.txt
└── README.md
```

---

## Running the test suite

```bash
pytest -q
```

20 tests cover CRUD, hashing, cache behaviour, exact + perceptual detection,
threshold edges, temp-file cleanup on error, and concurrent async writes.

---

## How Detection Works

1. **Download** incoming media to `DOWNLOAD_DIR` under a random filename.
2. **SHA-256** the full file. If `(chat_id, file_hash)` already exists → exact duplicate, delete.
3. **Type-specific secondary detection**:
   - **Photos**: compute pHash → compare Hamming distance against chat threshold
   - **Videos/Animations/Video notes**: extract first-frame thumbnail via ffmpeg → compute pHash → compare
   - **Audio/Voice**: extract duration via ffprobe + file size → flag as SOFT duplicate if both match within tolerance
   - **Documents**: store file_name, mime_type, file_size → flag as SOFT if name+size match but hash differs
4. **Stickers**: skipped entirely, logged.
5. Otherwise insert the new hash row and update the in-memory TTL cache.
6. The temp file is removed in a `finally` block — always, even on error.

---

## Graceful Shutdown

The bot handles `SIGTERM` and `SIGINT` signals via python-telegram-bot's
built-in `stop_signals`. On shutdown:

1. Finishes processing the current media item
2. Cancels the heartbeat writer
3. Flushes and disposes the database engine
4. Deletes any files in `DOWNLOAD_DIR` older than 5 minutes
5. Logs `"Bot shut down cleanly."`

---

## License

MIT
