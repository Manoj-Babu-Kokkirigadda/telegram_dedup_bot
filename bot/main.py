"""Bot entry point."""
from __future__ import annotations

import asyncio
import logging
import os
import signal
import time
from http.server import HTTPServer, BaseHTTPRequestHandler
from logging.handlers import RotatingFileHandler
from pathlib import Path
from threading import Thread

from dotenv import load_dotenv
from telegram import BotCommand
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    filters,
)

from bot.core.cache import HashCache
from bot.db.database import dispose_engine, init_db, init_engine
from bot.handlers import admin as admin_handlers
from bot.handlers import media as media_handlers

logger = logging.getLogger(__name__)
_heartbeat_task: asyncio.Task | None = None

ADMIN_COMMANDS = [
    BotCommand("start", "Welcome message and command list"),
    BotCommand("help", "Show command reference"),
    BotCommand("dedup_on", "Enable duplicate detection for this chat"),
    BotCommand("dedup_off", "Disable duplicate detection for this chat"),
    BotCommand("set_threshold", "Set image/video similarity threshold"),
    BotCommand("dedup_status", "Show enabled/disabled state and stats"),
    BotCommand("dedup_stats", "Show global + per-chat duplicate statistics"),
    BotCommand("clean", "Delete all tracked duplicate messages"),
    BotCommand("clear_hashes", "Wipe stored hashes (requires CONFIRM)"),
]


async def _heartbeat_loop(path: str, interval: int = 60) -> None:
    """Write a timestamp to a file every `interval` seconds."""
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    while True:
        try:
            with open(path, "w") as f:
                f.write(str(time.time()))
        except OSError:
            pass
        await asyncio.sleep(interval)


def _cleanup_downloads(download_dir: str, older_than_seconds: float = 300) -> None:
    """Delete files in download_dir older than `older_than_seconds`."""
    now = time.time()
    for entry in Path(download_dir).iterdir():
        if entry.is_file():
            try:
                if now - entry.stat().st_mtime > older_than_seconds:
                    entry.unlink()
                    logger.info("Cleaned up stale download: %s", entry.name)
            except OSError as exc:
                logger.warning("Could not remove %s: %s", entry, exc)


def _configure_logging(log_level: str) -> None:
    os.makedirs("logs", exist_ok=True)
    handler = RotatingFileHandler(
        "logs/bot.log", maxBytes=5 * 1024 * 1024, backupCount=3
    )
    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )
    handler.setFormatter(fmt)

    stream = logging.StreamHandler()
    stream.setFormatter(fmt)

    root = logging.getLogger()
    root.setLevel(getattr(logging, log_level.upper(), logging.INFO))
    root.addHandler(handler)
    root.addHandler(stream)
    logging.getLogger("httpx").setLevel(logging.WARNING)


async def _post_init(app: Application) -> None:
    global _heartbeat_task
    await init_db()
    heartbeat_path = "logs/heartbeat.txt"
    _heartbeat_task = asyncio.create_task(_heartbeat_loop(heartbeat_path))
    await app.bot.set_my_commands(commands=ADMIN_COMMANDS)
    logger.info("Database initialised.")


async def _post_shutdown(app: Application) -> None:
    global _heartbeat_task
    if _heartbeat_task is not None:
        _heartbeat_task.cancel()
        try:
            await _heartbeat_task
        except asyncio.CancelledError:
            pass
    await dispose_engine()
    download_dir = os.environ.get("DOWNLOAD_DIR", "./downloads")
    _cleanup_downloads(download_dir)
    logger.info("Bot shut down cleanly.")


def build_application() -> Application:
    load_dotenv()

    token = os.environ.get("BOT_TOKEN")
    if not token:
        raise RuntimeError("BOT_TOKEN missing — set it in .env")

    db_url = os.environ.get("DATABASE_URL", "sqlite+aiosqlite:///./dedup.db")
    download_dir = os.environ.get("DOWNLOAD_DIR", "./downloads")
    log_level = os.environ.get("LOG_LEVEL", "INFO")
    max_media_per_min = int(os.environ.get("MAX_MEDIA_PER_MINUTE", "50"))
    rate_pause_seconds = int(os.environ.get("RATE_LIMIT_PAUSE_SECONDS", "30"))
    notify_on_delete = os.environ.get("NOTIFY_ON_DELETE", "true").lower() == "true"

    _configure_logging(log_level)
    init_engine(db_url)

    cache = HashCache()
    media_handlers.configure(
        cache=cache,
        downloads=download_dir,
        max_media_per_min=max_media_per_min,
        rate_pause_seconds=rate_pause_seconds,
        max_user_media_per_min=20,
        send_delete_notification=notify_on_delete,
    )

    app = (
        Application.builder()
        .token(token)
        .post_init(_post_init)
        .post_shutdown(_post_shutdown)
        .build()
    )

    # Start + Help
    app.add_handler(CommandHandler("start", admin_handlers.cmd_start))
    app.add_handler(CommandHandler("help", admin_handlers.cmd_help))

    # Admin commands
    app.add_handler(CommandHandler("dedup_on", admin_handlers.cmd_dedup_on))
    app.add_handler(CommandHandler("dedup_off", admin_handlers.cmd_dedup_off))
    app.add_handler(CommandHandler("set_threshold", admin_handlers.cmd_set_threshold))
    app.add_handler(CommandHandler("dedup_status", admin_handlers.cmd_dedup_status))
    app.add_handler(CommandHandler("clear_hashes", admin_handlers.cmd_clear_hashes))
    app.add_handler(CommandHandler("dedup_stats", admin_handlers.cmd_dedup_stats))
    app.add_handler(CommandHandler("clean", admin_handlers.cmd_clean))

    app.add_handler(
        MessageHandler(
            filters.TEXT & filters.Regex(r"^CONFIRM$"),
            admin_handlers.cmd_confirm_clear,
        )
    )

    app.add_handler(
        MessageHandler(
            (
                filters.PHOTO
                | filters.VIDEO
                | filters.VIDEO_NOTE
                | filters.ANIMATION
                | filters.AUDIO
                | filters.Document.ALL
            )
            & ~filters.COMMAND,
            media_handlers.handle_media,
        )
    )
    return app


class _HealthHandler(BaseHTTPRequestHandler):
    """Dummy HTTP server to satisfy Render's port requirement."""
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.end_headers()
        self.wfile.write(b"ok")
    def log_message(self, format, *args):
        pass


def _start_dummy_server(port: int) -> None:
    Thread(target=lambda: HTTPServer(("0.0.0.0", port), _HealthHandler).serve_forever(), daemon=True).start()
    logger.info("Dummy health server on port %d", port)


def main() -> None:
    port = int(os.environ.get("PORT", "10000"))
    _start_dummy_server(port)

    app = build_application()
    logger = logging.getLogger(__name__)
    logger.info("Starting Telegram dedup bot…")

    app.run_polling(stop_signals=(signal.SIGINT, signal.SIGTERM), allowed_updates=None)


if __name__ == "__main__":
    asyncio.set_event_loop_policy(None)
    main()
