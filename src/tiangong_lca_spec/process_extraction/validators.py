"""Strict validators for Stage 2 exchange outputs."""

from __future__ import annotations

from typing import Any, Iterable, Sequence

FORBIDDEN_VALUES = {
    "",
    "-",
    "na",
    "n/a",
    "unspecified",
    "tbd",
    "glo",
    "global",
    "cn",
}

SHORT_ACRONYM_LIMIT = 3

REQUIRED_HINT_FIELDS: tuple[str, ...] = (
    "basename",
    "treatment",
    "mix_location",
    "source_or_pathway",
    "en_synonyms",
    "state_purity",
    "flow_properties",
    "usage_context",
)


def validate_exchanges_strict(
    exchanges: Sequence[dict[str, Any]],
    *,
    geography: str | None = None,
) -> list[str]:
    """Return a list of validation errors for the provided exchanges."""

    errors: list[str] = []
    geography_code = (geography or "").strip()
    geography_upper = geography_code.upper()

    for index, exchange in enumerate(exchanges, start=1):
        prefix = f"exchange #{index}"
        name = _coerce_str(exchange.get("exchangeName"))
        if not name:
            errors.append(f"{prefix}: `exchangeName` is required.")
            continue
        if _is_placeholder(name):
            errors.append(f"{prefix}: `exchangeName` uses placeholder value '{name}'.")
        hints = _extract_flow_hints(exchange)
        if hints is None:
            errors.append(f"{prefix} ({name}): missing `flowHints` object with required fields.")
            continue

        basename = hints.get("basename", "")
        if not basename or _is_placeholder(basename):
            errors.append(f"{prefix} ({name}): `basename` must spell out the full flow name (e.g., 'Liquid nitrogen').")
        elif not _names_consistent(name, basename):
            errors.append(
                f"{prefix} ({name}): `basename` ('{basename}') must match or be a more formal version of `exchangeName`."
            )

        for field in REQUIRED_HINT_FIELDS:
            value = hints.get(field)
            if value is None:
                errors.append(f"{prefix} ({name}): missing `{field}`.")
                continue
            if field == "en_synonyms":
                _validate_synonyms(value, prefix, name, errors, basename)
                continue
            value_str = _coerce_str(value)
            if not value_str:
                errors.append(f"{prefix} ({name}): `{field}` must be a non-empty string.")
            elif _is_placeholder(value_str):
                errors.append(f"{prefix} ({name}): `{field}` uses placeholder value '{value_str}'.")
            elif field == "mix_location":
                _validate_mix_location(value_str, prefix, name, errors, geography_upper)
            elif field == "source_or_pathway":
                _validate_source(value_str, prefix, name, errors, geography_upper)

    return errors


def _extract_flow_hints(exchange: dict[str, Any]) -> dict[str, Any] | None:
    hints = exchange.get("flowHints") or exchange.get("hints")
    if isinstance(hints, dict):
        return hints
    return None


def _validate_synonyms(
    value: Any,
    prefix: str,
    name: str,
    errors: list[str],
    basename: str,
) -> None:
    if isinstance(value, list):
        synonyms = [_coerce_str(item) for item in value if _coerce_str(item)]
    elif isinstance(value, str):
        if ";" in value:
            parts = value.split(";")
        else:
            parts = value.split(",")
        synonyms = [part.strip() for part in parts if part.strip()]
    else:
        synonyms = []
    if not synonyms:
        errors.append(f"{prefix} ({name}): `en_synonyms` must list at least one synonym.")
        return
    if _is_placeholder(synonyms[0]) or (basename and not _names_consistent(basename, synonyms[0])):
        errors.append(
            f"{prefix} ({name}): first entry of `en_synonyms` must repeat the full flow name (e.g., '{basename}')."
        )
    for idx, synonym in enumerate(synonyms):
        if idx == 0:
            continue
        if synonym.strip().lower() in {"", "-", "na", "n/a", "tbd", "unspecified"}:
            errors.append(f"{prefix} ({name}): `en_synonyms` contains placeholder '{synonym}'.")


def _validate_mix_location(
    value: str,
    prefix: str,
    name: str,
    errors: list[str],
    geography_upper: str,
) -> None:
    if geography_upper and geography_upper not in {"", "GLO"} and geography_upper not in value.upper():
        errors.append(
            f"{prefix} ({name}): `mix_location` ('{value}') must reference the geography code ({geography_upper})."
        )


def _validate_source(
    value: str,
    prefix: str,
    name: str,
    errors: list[str],
    geography_upper: str,
) -> None:
    if geography_upper and geography_upper not in {"", "GLO"} and geography_upper not in value.upper():
        errors.append(
            f"{prefix} ({name}): `source_or_pathway` ('{value}') must mention the geography ({geography_upper})."
        )


def _names_consistent(primary: str, secondary: str) -> bool:
    p = primary.strip().lower()
    s = secondary.strip().lower()
    return p == s or p in s or s in p


def _coerce_str(value: Any) -> str:
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, (int, float)):
        return str(value)
    return ""


def _is_placeholder(text: str) -> bool:
    stripped = text.strip()
    lowered = stripped.lower()
    if not lowered:
        return True
    if lowered in FORBIDDEN_VALUES:
        return True
    token = stripped.replace("-", "")
    if len(token) <= SHORT_ACRONYM_LIMIT and token.upper() == token and token.isalnum():
        return True
    return False


def is_placeholder_value(text: str) -> bool:
    """Public helper exposing the placeholder check for reuse across stages."""

    if not isinstance(text, str):
        return True
    return _is_placeholder(text)
