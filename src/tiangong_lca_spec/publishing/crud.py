"""Thin wrappers to publish flows and processes via Database_CRUD_Tool."""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping

from tiangong_lca_spec.core.config import Settings, get_settings
from tiangong_lca_spec.core.exceptions import SpecCodingError
from tiangong_lca_spec.core.logging import get_logger
from tiangong_lca_spec.core.mcp_client import MCPToolClient
from tiangong_lca_spec.workflow.artifacts import flow_compliance_declarations

LOGGER = get_logger(__name__)

DATABASE_TOOL_NAME = "Database_CRUD_Tool"


def _utc_timestamp() -> str:
    """Return ISO timestamp in the format required by Supabase validator."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _coerce_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, dict):
        text = value.get("#text")
        if isinstance(text, str):
            return text.strip()
    return str(value).strip()


def _parse_flowsearch_hints(comment: str | None) -> dict[str, list[str] | str]:
    """Parse 'FlowSearch hints:' into a dict of list values."""
    if not comment:
        return {}
    text = comment.strip()
    prefix = "FlowSearch hints:"
    if text.startswith(prefix):
        text = text[len(prefix) :].strip()
    segments = [segment.strip() for segment in text.split("|") if segment.strip()]
    output: dict[str, list[str] | str] = {}
    for segment in segments:
        key, _, value = segment.partition("=")
        key = key.strip()
        value = value.strip()
        if not key:
            continue
        if not value or value == "NA":
            output[key] = []
            continue
        parts = [item.strip() for item in value.split(";") if item.strip()]
        output[key] = parts or [value]
    return output


def _derive_language_pairs(hints: Mapping[str, list[str] | str], fallback: str) -> tuple[str, str]:
    en_candidates = [item for item in hints.get("en_synonyms", []) or [] if isinstance(item, str)]
    zh_candidates = [item for item in hints.get("zh_synonyms", []) or [] if isinstance(item, str)]
    en = en_candidates[0] if en_candidates else fallback
    zh = zh_candidates[0] if zh_candidates else en
    return en, zh


def _infer_flow_type(exchange: Mapping[str, Any], hints: Mapping[str, list[str] | str]) -> str:
    name = _coerce_text(exchange.get("exchangeName"))
    direction = _coerce_text(exchange.get("exchangeDirection")).lower()
    text_parts = [
        _coerce_text(exchange.get("generalComment")),
        _coerce_text(hints.get("usage_context")),
        _coerce_text(hints.get("state_purity")),
        name.lower(),
    ]
    text = " ".join(text_parts).lower()
    if any(
        keyword in text
        for keyword in ("emission", "flue gas", "to air", "to water", "wastewater", "effluent")
    ):
        return "Elementary flow"
    if "waste" in text or direction == "output" and "slag" in text:
        return "Waste flow"
    return "Product flow"


def _build_elementary_classification(hints: Mapping[str, list[str] | str]) -> dict[str, Any]:
    usage = _coerce_text(hints.get("usage_context"))
    usage_lower = usage.lower()
    if "air" in usage_lower or "vent" in usage_lower:
        path = ["Emissions", "Emissions to air", "Emissions to air, unspecified"]
    elif "water" in usage_lower or "effluent" in usage_lower:
        path = ["Emissions", "Emissions to water", "Emissions to water, unspecified"]
    elif "soil" in usage_lower:
        path = ["Emissions", "Emissions to soil", "Emissions to soil, unspecified"]
    else:
        path = ["Emissions", "Emissions to unspecified"]
    categories = []
    for level, label in enumerate(path):
        categories.append({"@level": str(level), "#text": label})
    return {"common:elementaryFlowCategorization": {"common:category": categories}}


def _build_product_classification() -> dict[str, Any]:
    return {
        "common:classification": {
            "common:class": [
                {
                    "@level": "0",
                    "@classId": "1",
                    "#text": "Ores and minerals; electricity, gas and water",
                }
            ]
        }
    }


def _extract_general_comment(exchange: Mapping[str, Any]) -> str:
    comment = exchange.get("generalComment")
    if isinstance(comment, dict):
        text = comment.get("#text")
        if isinstance(text, str):
            return text.strip()
    if isinstance(comment, str):
        return comment.strip()
    return ""


def _resolve_unit(exchange: Mapping[str, Any]) -> str:
    return _coerce_text(exchange.get("unit"))


def _build_flow_properties(unit: str) -> dict[str, Any]:
    """Return a ILCD flow property block. Currently defaulted to mass property."""
    reference_property = {
        "@type": "flow property data set",
        "@refObjectId": "93a60a56-a3c8-11da-a746-0800200b9a66",
        "@uri": "../flowproperties/93a60a56-a3c8-11da-a746-0800200b9a66.xml",
        "@version": "01.00.000",
        "common:shortDescription": {
            "@xml:lang": "en",
            "#text": "Mass",
        },
    }
    if unit and unit.lower() in {"kwh", "mj", "gj"}:
        # We do not have access to the energy flow property UUID within the current tables.
        # Mass property is used as placeholder; downstream reviewers must adjust.
        LOGGER.warning(
            "flow_publish.energy_property_placeholder",
            unit=unit,
            note="Flow property defaults to mass; please update energy reference manually.",
        )
    return {
        "flowProperty": {
            "@dataSetInternalID": "0",
            "meanValue": "1.0",
            "referenceToFlowPropertyDataSet": reference_property,
        }
    }


def _language_entry(text: str, lang: str = "en") -> dict[str, Any]:
    return {"@xml:lang": lang, "#text": text}


@dataclass
class FlowPublishPlan:
    """Single flow payload ready for publication."""

    uuid: str
    exchange_name: str
    process_name: str
    dataset: Mapping[str, Any]
    exchange_ref: Mapping[str, Any]


class DatabaseCrudClient:
    """Client wrapper over Database_CRUD_Tool for flows/processes CRUD."""

    def __init__(
        self,
        settings: Settings | None = None,
        *,
        mcp_client: MCPToolClient | None = None,
    ) -> None:
        self._settings = settings or get_settings()
        self._mcp = mcp_client or MCPToolClient(self._settings)
        self._server_name = self._settings.flow_search_service_name

    def insert_flow(self, dataset: Mapping[str, Any]) -> dict[str, Any]:
        return self._invoke(
            {
                "operation": "insert",
                "table": "flows",
                "jsonOrdered": dataset,
            }
        )

    def insert_process(self, dataset: Mapping[str, Any]) -> dict[str, Any]:
        return self._invoke(
            {
                "operation": "insert",
                "table": "processes",
                "jsonOrdered": dataset,
            }
        )

    def delete(self, table: str, record_id: str, version: str) -> dict[str, Any]:
        return self._invoke(
            {
                "operation": "delete",
                "table": table,
                "id": record_id,
                "version": version,
            }
        )

    def _invoke(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        LOGGER.debug("crud.invoke", table=payload.get("table"), operation=payload.get("operation"))
        raw = self._mcp.invoke_json_tool(self._server_name, DATABASE_TOOL_NAME, payload)
        if isinstance(raw, str):
            return json.loads(raw)
        if isinstance(raw, dict):
            return raw
        raise SpecCodingError("Unexpected payload returned from Database_CRUD_Tool")

    def close(self) -> None:
        self._mcp.close()


class FlowPublisher:
    """Build and optionally publish flow datasets for unmatched exchanges."""

    def __init__(
        self,
        *,
        settings: Settings | None = None,
        crud_client: DatabaseCrudClient | None = None,
        dry_run: bool = True,
    ) -> None:
        self._settings = settings or get_settings()
        self._crud = crud_client or DatabaseCrudClient(self._settings)
        self._dry_run = dry_run
        self._prepared: list[FlowPublishPlan] = []

    def prepare_from_alignment(
        self, alignment: Iterable[Mapping[str, Any]]
    ) -> list[FlowPublishPlan]:
        """Generate publication plans for every unmatched exchange."""
        plans: list[FlowPublishPlan] = []
        for entry in alignment:
            process_name = entry.get("process_name") or "Unknown process"
            origin = entry.get("origin_exchanges") or {}
            for exchanges in origin.values():
                for exchange in exchanges or []:
                    ref = exchange.get("referenceToFlowDataSet") or {}
                    if not isinstance(ref, dict):
                        continue
                    if not ref.get("unmatched:placeholder"):
                        continue
                    plan = self._build_plan(exchange, process_name)
                    plans.append(plan)
        self._prepared = plans
        LOGGER.info("flow_publish.plans_ready", count=len(plans))
        return plans

    def publish(self) -> list[dict[str, Any]]:
        """Execute inserts for the prepared plans."""
        results: list[dict[str, Any]] = []
        for plan in self._prepared:
            if self._dry_run:
                LOGGER.info(
                    "flow_publish.dry_run",
                    exchange=plan.exchange_name,
                    process=plan.process_name,
                    uuid=plan.uuid,
                )
                continue
            result = self._crud.insert_flow({"flowDataSet": plan.dataset})
            results.append(result)
        return results

    def close(self) -> None:
        self._crud.close()

    def _build_plan(self, exchange: Mapping[str, Any], process_name: str) -> FlowPublishPlan:
        exchange_name = _coerce_text(exchange.get("exchangeName")) or "Unnamed exchange"
        comment = _extract_general_comment(exchange)
        hints = _parse_flowsearch_hints(comment)
        uuid_value = str(uuid.uuid4())
        en_name, zh_name = _derive_language_pairs(hints, exchange_name)
        flow_type = _infer_flow_type(exchange, hints)
        classification = (
            _build_elementary_classification(hints)
            if flow_type == "Elementary flow"
            else _build_product_classification()
        )
        comment_entries = [_language_entry(comment or f"Auto-generated for {exchange_name}")]
        unit = _resolve_unit(exchange)
        dataset = {
            "@xmlns": "http://lca.jrc.it/ILCD/Flow",
            "@xmlns:common": "http://lca.jrc.it/ILCD/Common",
            "@xmlns:ecn": "http://eplca.jrc.ec.europa.eu/ILCD/Extensions/2018/ECNumber",
            "@xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
            "@locations": "../ILCDLocations.xml",
            "@version": "1.1",
            "@xsi:schemaLocation": "http://lca.jrc.it/ILCD/Flow ../../schemas/ILCD_FlowDataSet.xsd",
            "flowInformation": {
                "dataSetInformation": {
                    "common:UUID": uuid_value,
                    "name": {
                        "baseName": [
                            _language_entry(en_name, "en"),
                            _language_entry(zh_name, "zh"),
                        ]
                    },
                    "common:synonyms": [
                        _language_entry("; ".join(hints.get("en_synonyms", []) or [en_name]), "en"),
                        _language_entry("; ".join(hints.get("zh_synonyms", []) or [zh_name]), "zh"),
                    ],
                    "common:generalComment": comment_entries,
                    "classificationInformation": classification,
                },
                "quantitativeReference": {
                    "referenceToReferenceFlowProperty": "0",
                },
            },
            "modellingAndValidation": {
                "LCIMethod": {
                    "typeOfDataSet": flow_type,
                },
                "complianceDeclarations": flow_compliance_declarations(),
            },
            "administrativeInformation": {
                "dataEntryBy": {
                    "common:timeStamp": _utc_timestamp(),
                    "common:referenceToDataSetFormat": {
                        "@type": "source data set",
                        "@refObjectId": "a97a0155-0234-4b87-b4ce-a45da52f2a40",
                        "@uri": "../sources/a97a0155-0234-4b87-b4ce-a45da52f2a40.xml",
                        "@version": "01.00.000",
                        "common:shortDescription": _language_entry("ILCD format"),
                    },
                    "common:referenceToPersonOrEntityEnteringTheData": {
                        "@type": "contact data set",
                        "@refObjectId": "1f8176e3-86ba-49d1-bab7-4eca2741cdc1",
                        "@uri": "../contacts/1f8176e3-86ba-49d1-bab7-4eca2741cdc1.xml",
                        "@version": "01.00.005",
                        "common:shortDescription": [_language_entry("Yin, Linlin")],
                    },
                },
                "publicationAndOwnership": {
                    "common:dataSetVersion": "01.00.000",
                    "common:referenceToOwnershipOfDataSet": {
                        "@type": "contact data set",
                        "@refObjectId": "f4b4c314-8c4c-4c83-968f-5b3c7724f6a8",
                        "@uri": "../contacts/f4b4c314-8c4c-4c83-968f-5b3c7724f6a8.xml",
                        "@version": "01.00.000",
                        "common:shortDescription": [
                            _language_entry("Tiangong LCA Data Working Group")
                        ],
                    },
                },
            },
            "flowProperties": _build_flow_properties(unit),
        }
        exchange_ref = {
            "@type": "flow data set",
            "@uri": f"https://tiangong.earth/flows/{uuid_value}",
            "@refObjectId": uuid_value,
            "@version": "01.00.000",
            "common:shortDescription": _language_entry(exchange_name),
        }
        return FlowPublishPlan(
            uuid=uuid_value,
            exchange_name=exchange_name,
            process_name=process_name,
            dataset=dataset,
            exchange_ref=exchange_ref,
        )


class ProcessPublisher:
    """Publish final process datasets once validation passes."""

    def __init__(
        self,
        *,
        settings: Settings | None = None,
        crud_client: DatabaseCrudClient | None = None,
        dry_run: bool = True,
    ) -> None:
        self._settings = settings or get_settings()
        self._crud = crud_client or DatabaseCrudClient(self._settings)
        self._dry_run = dry_run

    def publish(self, datasets: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        for dataset in datasets:
            payload = {"processDataSet": dataset}
            process_info = dataset.get("processInformation", {})
            name_block = process_info.get("dataSetInformation", {}).get("name", {})
            process_name = _coerce_text(name_block.get("baseName"))
            if self._dry_run:
                LOGGER.info("process_publish.dry_run", name=process_name)
                continue
            result = self._crud.insert_process(payload)
            results.append(result)
        return results

    def close(self) -> None:
        self._crud.close()
