"""Mock processing functions for the two-stage pipeline.

These produce **deterministic** results for a given input so that
content-based caching works correctly: the same content always
yields the same summary and tags.
"""

import re
from collections import Counter

# Minimal English stop-word list for tag extraction
_STOP_WORDS: frozenset[str] = frozenset({
    "this", "that", "with", "have", "from", "they", "will", "would",
    "there", "their", "what", "about", "which", "when", "your", "them",
    "been", "were", "more", "some", "such", "only", "than", "then",
    "also", "into", "just", "like", "make", "many", "most", "over",
    "take", "these", "very", "where", "while", "would", "have", "has",
    "had", "does", "done", "what", "when", "where", "which", "whose",
    "here", "thus", "then", "than", "same", "other", "each", "both",
})


def generate_summary(content: str) -> str:
    """Produce a deterministic mock summary from *content*.

    Takes the first sentence (or first 200 characters) and wraps it
    in a prefix that makes it clearly machine-generated.
    """
    # Split on first sentence boundary
    sentences = re.split(r"[.!?]+", content)
    first = sentences[0].strip() if sentences else content[:200]
    if len(first) > 200:
        first = first[:200] + "…"
    return f"Summary: {first}"


def generate_tags(content: str) -> list[str]:
    """Produce deterministic mock keyword tags from *content*.

    Extracts the five most frequent meaningful words (> 4 chars,
    not in the stop-word list) as tags.
    """
    words = re.findall(r"[a-zA-Z]{4,}", content.lower())
    filtered = [w for w in words if w not in _STOP_WORDS]
    counter = Counter(filtered)
    return [word for word, _ in counter.most_common(5)]
