from __future__ import annotations

import hashlib
import math
import re

TOKEN_RE = re.compile(r"[a-z0-9_]+")


def _tokens(text: str) -> list[str]:
    return TOKEN_RE.findall(text.lower())


def embed_text(text: str, dim: int = 128) -> list[float]:
    vec = [0.0] * dim
    for token in _tokens(text):
        digest = hashlib.sha256(token.encode("utf-8")).digest()
        idx = int.from_bytes(digest[:4], "big") % dim
        sign = 1.0 if (digest[4] % 2 == 0) else -1.0
        vec[idx] += sign

    norm = math.sqrt(sum(v * v for v in vec))
    if norm == 0:
        return vec
    return [v / norm for v in vec]


def cosine_similarity(a: list[float], b: list[float]) -> float:
    if len(a) != len(b):
        return 0.0
    return float(sum(x * y for x, y in zip(a, b)))
