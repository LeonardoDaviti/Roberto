from __future__ import annotations

import hashlib


def build_cache_key(model: str, prompt: str, tweet_ids: list[str]) -> str:
    normalized_ids = ",".join(sorted(tweet_ids))
    raw = f"{model}\n{normalized_ids}\n{prompt}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()
