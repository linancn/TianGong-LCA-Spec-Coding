from __future__ import annotations

import json
from typing import Any

import tomllib
from openai import OpenAI


class Translator:
    """Thin wrapper around OpenAI chat completions for short translations."""

    def __init__(self, *, model: str | None = None, secrets_path: str = ".secrets/secrets.toml") -> None:
        self._model = model
        self._client = self._make_client(secrets_path)

    def _make_client(self, secrets_path: str) -> OpenAI:
        client_kwargs: dict[str, Any] = {}
        try:
            with open(secrets_path, "rb") as fh:
                secrets = tomllib.load(fh)
            openai_conf = secrets.get("openai", {})
            api_key = openai_conf.get("api_key")
            if api_key:
                client_kwargs["api_key"] = api_key
            if not self._model and openai_conf.get("model"):
                self._model = openai_conf.get("model")
        except Exception:
            pass
        if not self._model:
            self._model = "gpt-4o-mini"
        return OpenAI(**client_kwargs)

    def translate(self, text: str, target_lang: str) -> str | None:
        """Translate to target_lang ('en' or 'zh')."""
        if not text or target_lang not in {"en", "zh"}:
            return None
        prompt = (
            f"Translate the following text into concise {'English' if target_lang=='en' else 'Chinese'}, "
            "keep technical terms: "
            f"{text}"
        )
        system = "Translate text concisely, preserve technical terms."
        try:
            resp = self._client.chat.completions.create(
                model=self._model,
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user", "content": prompt},
                ],
                temperature=0,
                max_completion_tokens=200,
            )
            content = resp.choices[0].message.content if resp.choices else None
            if not content:
                return None
            return content.strip()
        except Exception:
            return None


__all__ = ["Translator"]
