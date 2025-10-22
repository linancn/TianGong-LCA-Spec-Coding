"""Shared utilities for staged Tiangong LCA workflow scripts."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

import tomllib
from openai import APIConnectionError, APIStatusError, OpenAI


class OpenAIResponsesLLM:
    """Minimal wrapper around the OpenAI Responses API."""

    def __init__(self, api_key: str, model: str, timeout: int = 1200) -> None:
        self._client = OpenAI(api_key=api_key, timeout=timeout)
        self._model = model

    def invoke(self, input_data: dict[str, Any]) -> str:
        prompt = input_data.get("prompt") or ""
        context = input_data.get("context")
        if isinstance(context, (dict, list)):
            user_content = json.dumps(context, ensure_ascii=False)
        else:
            user_content = str(context) if context is not None else ""
        payload = [
            {"role": "system", "content": [{"type": "input_text", "text": str(prompt)}]},
            {"role": "user", "content": [{"type": "input_text", "text": user_content}]},
        ]

        last_error: Exception | None = None
        for attempt in range(3):
            try:
                response = self._client.responses.create(model=self._model, input=payload)
                if getattr(response, "output_text", None):
                    return response.output_text
                parts: list[str] = []
                for item in getattr(response, "output", []) or []:
                    if item.get("type") == "message":
                        for content in item["content"]:
                            if content.get("type") == "output_text":
                                parts.append(content.get("text", ""))
                return "\n".join(parts)
            except (APIConnectionError, APIStatusError) as exc:
                last_error = exc
                if attempt == 2:
                    raise
                time.sleep(5 * (attempt + 1))
        if last_error:
            raise last_error
        raise RuntimeError("OpenAI invocation failed without response")


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
