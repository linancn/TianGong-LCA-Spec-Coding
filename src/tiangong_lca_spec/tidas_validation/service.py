"""Public facade for running TIDAS validation."""

from __future__ import annotations

from typing import Any

from tiangong_lca_spec.core.config import Settings, get_settings
from tiangong_lca_spec.core.exceptions import TidasValidationError
from tiangong_lca_spec.core.logging import get_logger
from tiangong_lca_spec.core.models import ProcessDataset, TidasValidationFinding

from .client import TidasClient

LOGGER = get_logger(__name__)


class TidasValidationService:
    def __init__(self, settings: Settings | None = None, *, client: TidasClient | None = None) -> None:
        self._settings = settings or get_settings()
        self._client = client or TidasClient(self._settings)

    def validate(self, datasets: list[ProcessDataset]) -> list[TidasValidationFinding]:
        LOGGER.info("tidas_validation.start", dataset_count=len(datasets))
        payload = [_as_dict(dataset) for dataset in datasets]
        findings = self._client.validate(payload)
        return [_hydrate_finding(item) for item in findings]

    def close(self) -> None:
        self._client.close()


def _as_dict(dataset: ProcessDataset | dict[str, Any]) -> dict[str, Any]:
    if isinstance(dataset, ProcessDataset):
        return dataset.as_dict()
    if isinstance(dataset, dict):
        return dataset
    raise TidasValidationError("Unexpected dataset type")


def _hydrate_finding(item: dict[str, Any]) -> TidasValidationFinding:
    return TidasValidationFinding(
        severity=item.get("severity", "info"),
        message=item.get("message", ""),
        path=item.get("path"),
        suggestion=item.get("suggestion"),
    )


def validate_with_tidas(process_datasets: list[ProcessDataset]) -> list[TidasValidationFinding]:
    service = TidasValidationService()
    try:
        return service.validate(process_datasets)
    finally:
        service.close()
