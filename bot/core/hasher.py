"""SHA-256 + perceptual hash helpers."""
from __future__ import annotations

import hashlib
import logging
import os
import subprocess
from pathlib import Path

logger = logging.getLogger(__name__)


def compute_sha256(file_path: str) -> str:
    """Stream-read the file and return its hex SHA-256 digest."""
    h = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def compute_phash(file_path: str) -> str | None:
    """Return a 64-bit perceptual hash for an image, or None if not an image."""
    try:
        from PIL import Image
        import imagehash
    except ImportError:  # pragma: no cover
        logger.error("Pillow / imagehash not installed; pHash disabled")
        return None

    try:
        with Image.open(file_path) as img:
            img.load()
            return str(imagehash.phash(img))
    except Exception as exc:
        logger.debug("pHash skipped for %s: %s", file_path, exc)
        return None


def extract_video_thumbnail(file_path: str, output_path: str) -> bool:
    """Extract the first frame of a video/GIF as a JPEG thumbnail using ffmpeg.

    Returns True on success, False on failure.
    """
    try:
        result = subprocess.run(
            [
                "ffmpeg",
                "-i", file_path,
                "-ss", "00:00:00.000",
                "-frames:v", "1",
                "-y",
                output_path,
            ],
            capture_output=True,
            timeout=30,
        )
        if result.returncode != 0:
            logger.warning(
                "ffmpeg thumbnail extraction failed for %s: %s",
                file_path, result.stderr.decode(errors="replace"),
            )
            return False
        return os.path.isfile(output_path) and os.path.getsize(output_path) > 0
    except FileNotFoundError:
        logger.error("ffmpeg not found on PATH; video thumbnail pHash unavailable")
        return False
    except subprocess.TimeoutExpired:
        logger.error("ffmpeg timed out extracting thumbnail for %s", file_path)
        return False
    except Exception as exc:
        logger.error("Unexpected error extracting video thumbnail: %s", exc)
        return False


def compute_video_phash(file_path: str) -> str | None:
    """Extract first-frame thumbnail and compute its pHash."""
    from PIL import Image
    import imagehash

    tmp_thumb = file_path + "_thumb.jpg"
    try:
        if not extract_video_thumbnail(file_path, tmp_thumb):
            return None
        with Image.open(tmp_thumb) as img:
            img.load()
            return str(imagehash.phash(img))
    except ImportError:
        logger.error("Pillow / imagehash not installed; video pHash disabled")
        return None
    except Exception as exc:
        logger.debug("Video pHash skipped for %s: %s", file_path, exc)
        return None
    finally:
        try:
            if os.path.isfile(tmp_thumb):
                os.remove(tmp_thumb)
        except OSError:
            pass


def get_audio_duration(file_path: str) -> float | None:
    """Return audio duration in seconds via ffprobe, or None on failure."""
    try:
        result = subprocess.run(
            [
                "ffprobe",
                "-v", "error",
                "-show_entries", "format=duration",
                "-of", "default=noprint_wrappers=1:nokey=1",
                file_path,
            ],
            capture_output=True,
            text=True,
            timeout=15,
        )
        if result.returncode == 0 and result.stdout.strip():
            return float(result.stdout.strip())
    except FileNotFoundError:
        logger.error("ffprobe not found on PATH; audio duration unavailable")
    except (subprocess.TimeoutExpired, ValueError) as exc:
        logger.debug("Could not get audio duration for %s: %s", file_path, exc)
    return None


def hamming_distance_hex(a: str, b: str) -> int:
    """Hamming distance between two hex-encoded hash strings of equal length."""
    if len(a) != len(b):
        max_len = max(len(a), len(b))
        a = a.zfill(max_len)
        b = b.zfill(max_len)
    return bin(int(a, 16) ^ int(b, 16)).count("1")


def file_exists(path: str) -> bool:
    return Path(path).is_file()
