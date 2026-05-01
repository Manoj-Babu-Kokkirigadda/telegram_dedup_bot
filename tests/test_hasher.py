"""Tests for hashing helpers."""
from __future__ import annotations

import hashlib
from pathlib import Path

from bot.core.hasher import compute_phash, compute_sha256, hamming_distance_hex


def test_sha256_known_value(tmp_path: Path):
    f = tmp_path / "x.bin"
    f.write_bytes(b"test")
    expected = hashlib.sha256(b"test").hexdigest()
    assert compute_sha256(str(f)) == expected
    # And matches the well-known SHA-256 of "test"
    assert (
        expected
        == "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"
    )


def test_phash_returns_none_for_non_image(tmp_path: Path):
    f = tmp_path / "note.txt"
    f.write_text("not an image")
    assert compute_phash(str(f)) is None


def test_phash_returns_string_for_image(tmp_path: Path):
    from PIL import Image

    p = tmp_path / "img.png"
    img = Image.new("RGB", (64, 64), color=(120, 200, 50))
    img.save(p)
    h = compute_phash(str(p))
    assert isinstance(h, str)
    assert len(h) >= 8


def test_hamming_distance():
    assert hamming_distance_hex("ff", "ff") == 0
    assert hamming_distance_hex("ff", "00") == 8
    assert hamming_distance_hex("0f", "00") == 4
