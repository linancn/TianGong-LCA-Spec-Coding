"""Shared utilities for staged Tiangong LCA workflow scripts."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
import time
import tomllib
from pathlib import Path
from typing import Any

from openai import APIConnectionError, APIStatusError, OpenAI


class OpenAIResponsesLLM:
    """Minimal wrapper around the OpenAI Responses API with lightweight disk caching."""

    def __init__(
        self,
        api_key: str,
        model: str,
        timeout: int = 2400,
        cache_dir: Path | None = Path(".cache/openai"),
        use_cache: bool = True,
    ) -> None:
        self._client = OpenAI(api_key=api_key, timeout=timeout)
        self._model = model
        self._cache_dir = Path(cache_dir) if use_cache and cache_dir else None
        if self._cache_dir:
            self._cache_dir.mkdir(parents=True, exist_ok=True)

    def invoke(self, input_data: dict[str, Any]) -> str:
        prompt = input_data.get("prompt") or ""
        context = input_data.get("context")
        response_format = input_data.get("response_format")
        text_config = input_data.get("text")
        if isinstance(context, (dict, list)):
            user_content = json.dumps(context, ensure_ascii=False)
        else:
            user_content = str(context) if context is not None else ""
        payload = [
            {"role": "system", "content": [{"type": "input_text", "text": str(prompt)}]},
            {"role": "user", "content": [{"type": "input_text", "text": user_content}]},
        ]

        text_options = dict(text_config or {})
        if response_format:
            text_options["format"] = response_format

        cache_path = self._cache_lookup(payload, text_options)
        if cache_path and cache_path.exists():
            cached = json.loads(cache_path.read_text(encoding="utf-8"))
            return cached["output"]

        last_error: Exception | None = None
        for attempt in range(3):
            try:
                kwargs: dict[str, Any] = {"model": self._model, "input": payload}
                if text_options:
                    kwargs["text"] = text_options
                response = self._client.responses.create(**kwargs)
                output = self._extract_output(response)
                if cache_path:
                    self._cache_store(cache_path, {"output": output})
                return output
            except (APIConnectionError, APIStatusError) as exc:
                last_error = exc
                if attempt == 2:
                    raise
                time.sleep(5 * (attempt + 1))
        if last_error:
            raise last_error
        raise RuntimeError("OpenAI invocation failed without response")

    def _cache_lookup(
        self, payload: list[dict[str, Any]], text_options: dict[str, Any]
    ) -> Path | None:
        if not self._cache_dir:
            return None
        cache_material = {
            "model": self._model,
            "payload": payload,
            "text_options": text_options,
        }
        digest = hashlib.sha256(
            json.dumps(cache_material, ensure_ascii=False, sort_keys=True).encode("utf-8")
        ).hexdigest()
        return self._cache_dir / f"{digest}.json"

    def _cache_store(self, path: Path, payload: dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(
            "w", dir=path.parent, encoding="utf-8", delete=False
        ) as tmp:
            json.dump(payload, tmp, ensure_ascii=False, indent=2)
            tmp.flush()
            os.fsync(tmp.fileno())
            temp_name = tmp.name
        os.replace(temp_name, path)

    @staticmethod
    def _extract_output(response: Any) -> str:
        if getattr(response, "output_text", None):
            return response.output_text
        parts: list[str] = []
        for item in getattr(response, "output", []) or []:
            if item.get("type") == "message":
                for content in item["content"]:
                    if content.get("type") == "output_text":
                        parts.append(content.get("text", ""))
        return "\n".join(parts)


def load_secrets(path: Path) -> tuple[str, str]:
    """Load OpenAI API credentials from the secrets file."""
    secrets = tomllib.loads(path.read_text(encoding="utf-8"))
    openai_cfg = secrets.get("OPENAI", {})
    api_key = openai_cfg.get("API_KEY") or openai_cfg.get("api_key")
    model = openai_cfg.get("MODEL") or openai_cfg.get("model") or "gpt-5"
    if not api_key:
        raise SystemExit(f"OpenAI API key missing in {path}")
    return api_key, model


def load_paper(path: Path) -> str:
    """Load the paper content, accepting raw markdown or JSON fragments."""
    raw = path.read_text(encoding="utf-8")
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return raw
    if isinstance(parsed, dict) and "result" in parsed:
        fragments = [
            item.get("text", "")
            for item in parsed["result"]
            if isinstance(item, dict) and item.get("text")
        ]
        return json.dumps(fragments, ensure_ascii=False)
    return raw


def dump_json(data: Any, path: Path) -> None:
    """Write JSON to disk with UTF-8 encoding, creating parent directories as needed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
