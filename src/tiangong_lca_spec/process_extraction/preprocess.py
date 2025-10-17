"""Utilities for pre-processing markdown JSON documents."""

from __future__ import annotations

import json
import re
from typing import Iterable

from tiangong_lca_spec.core.exceptions import ProcessExtractionError

THINK_PATTERN = re.compile(r"<think>.*?</think>", flags=re.DOTALL)
SECTION_PATTERN = re.compile(
    r"^(#+\s*)(references|appendix|acknowledgements)\b.*", flags=re.IGNORECASE | re.MULTILINE
)


def preprocess_paper(md_json: str, max_length: int = 120_000) -> str:
    """Parse and clean markdown fragments serialized as JSON."""
    try:
        parsed = json.loads(md_json)
    except json.JSONDecodeError as exc:
        raise ProcessExtractionError("Paper JSON is not valid") from exc

    fragments = list(_iter_fragments(parsed))
    if not fragments:
        raise ProcessExtractionError("Paper JSON contains no textual fragments")

    text = "\n\n".join(fragment.strip() for fragment in fragments if fragment and fragment.strip())
    text = THINK_PATTERN.sub("", text)
    text = _remove_sections(text)

    if len(text) > max_length:
        text = text[:max_length]

    return text


def _iter_fragments(parsed: object) -> Iterable[str]:
    if isinstance(parsed, list):
        for item in parsed:
            if isinstance(item, str):
                yield item
    elif isinstance(parsed, dict):
        for value in parsed.values():
            if isinstance(value, str):
                yield value
            elif isinstance(value, list):
                for child in value:
                    if isinstance(child, str):
                        yield child
    elif isinstance(parsed, str):
        yield parsed


def _remove_sections(text: str) -> str:
    lines = text.splitlines()
    filtered: list[str] = []
    skip = False
    for line in lines:
        if SECTION_PATTERN.match(line):
            skip = True
        if not skip:
            filtered.append(line)
    return "\n".join(filtered)
