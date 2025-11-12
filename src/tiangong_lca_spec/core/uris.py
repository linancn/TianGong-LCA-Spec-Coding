"""Helpers for constructing Tiangong portal dataset URLs."""

from __future__ import annotations

from typing import Final

TIDAS_PORTAL_BASE: Final[str] = "https://lcdn.tiangong.earth"

_SUFFIX_MAP: Final[dict[str, str]] = {
    "process": "showProcess.xhtml",
    "flow": "showProductFlow.xhtml",
    "source": "showSource.xhtml",
}


def build_portal_uri(dataset_kind: str, uuid_value: str, version: str) -> str:
    """Return the public portal URL for a dataset."""
    if not uuid_value:
        return ""
    suffix = _SUFFIX_MAP.get(dataset_kind, "showDataSet.xhtml")
    version_clean = (version or "").strip() or "01.01.000"
    return f"{TIDAS_PORTAL_BASE}/{suffix}?uuid={uuid_value}&version={version_clean}"
