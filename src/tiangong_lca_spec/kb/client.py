"""HTTP client for interacting with the Tiangong knowledge base datasets."""

from __future__ import annotations

import json
from contextlib import AbstractContextManager
from pathlib import Path
from typing import Iterable

import httpx

from .config import KnowledgeBaseConfig, MetadataFieldDefinition


class KnowledgeBaseClient(AbstractContextManager["KnowledgeBaseClient"]):
    """Minimal wrapper around the dataset/document endpoints."""

    def __init__(self, config: KnowledgeBaseConfig) -> None:
        self._config = config
        self._client = httpx.Client(
            base_url=config.base_url,
            timeout=config.request_timeout,
            headers=config.authorization_header,
        )

    # Context manager API -----------------------------------------------------
    def __enter__(self) -> "KnowledgeBaseClient":
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:  # type: ignore[override]
        self.close()

    def close(self) -> None:
        """Close the underlying HTTP client."""
        self._client.close()

    # Dataset operations ------------------------------------------------------
    def fetch_dataset_info(self) -> dict:
        """Return dataset metadata used to determine the indexing behaviour."""
        response = self._client.get(f"datasets/{self._config.dataset_id}")
        response.raise_for_status()
        return response.json()

    def ensure_metadata_fields(self, definitions: Iterable[MetadataFieldDefinition]) -> dict[str, str]:
        """Ensure required metadata fields exist and return their IDs."""
        endpoint = f"datasets/{self._config.dataset_id}/metadata"
        response = self._client.get(endpoint)
        response.raise_for_status()
        payload = response.json()
        existing = {item["name"]: item["id"] for item in payload.get("doc_metadata", []) if isinstance(item, dict)}

        for definition in definitions:
            if definition.name in existing:
                continue
            create_resp = self._client.post(
                endpoint,
                json={"type": definition.type, "name": definition.name},
            )
            create_resp.raise_for_status()
            created = create_resp.json()
            existing[created["name"]] = created["id"]
        return existing

    # Document operations -----------------------------------------------------
    def upload_document(self, file_path: Path, payload: dict) -> dict:
        """Upload a document and return the API response payload."""
        endpoint = f"datasets/{self._config.dataset_id}/document/create-by-file"
        try:
            with file_path.open("rb") as binary:
                files = {"file": (file_path.name, binary, "application/pdf")}
                response = self._client.post(endpoint, data={"data": json.dumps(payload)}, files=files)
                response.raise_for_status()
                return response.json()
        except FileNotFoundError as exc:
            raise SystemExit(f"Attachment not found: {file_path}") from exc

    def attach_metadata(self, document_id: str, metadata_entries: list[dict]) -> dict | None:
        """Attach metadata to a document when entries are available."""
        if not metadata_entries:
            return None
        endpoint = f"datasets/{self._config.dataset_id}/documents/metadata"
        response = self._client.post(
            endpoint,
            json={"operation_data": [{"document_id": document_id, "metadata_list": metadata_entries}]},
        )
        response.raise_for_status()
        return response.json()
