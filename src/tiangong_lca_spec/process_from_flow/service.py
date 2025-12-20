"""High-level facade and LangGraph assembly for building processes from a reference flow."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, TypedDict
from uuid import uuid4

from langgraph.graph import END, StateGraph
from tidas_sdk import create_process

from tiangong_lca_spec.core.config import Settings, get_settings
from tiangong_lca_spec.core.json_utils import parse_json_response
from tiangong_lca_spec.core.logging import get_logger
from tiangong_lca_spec.core.models import FlowCandidate, FlowQuery
from tiangong_lca_spec.core.uris import build_local_dataset_uri, build_portal_uri
from tiangong_lca_spec.flow_alignment.selector import (
    CandidateSelector,
    LanguageModelProtocol,
    LLMCandidateSelector,
    SimilarityCandidateSelector,
)
from tiangong_lca_spec.flow_search import search_flows
from tiangong_lca_spec.process_extraction.extractors import ProcessClassifier
from tiangong_lca_spec.process_extraction.tidas_mapping import (
    COMPLIANCE_DEFAULT_PREFERENCES,
    ILCD_ENTRY_LEVEL_REFERENCE_ID,
    ILCD_ENTRY_LEVEL_REFERENCE_VERSION,
)

from .prompts import EXCHANGES_PROMPT, PROCESS_SPLIT_PROMPT, TECH_DESCRIPTION_PROMPT

LOGGER = get_logger(__name__)

FlowSearchFn = Callable[[FlowQuery], tuple[list[FlowCandidate], list[object]]]


class ProcessFromFlowState(TypedDict, total=False):
    flow_path: str
    flow_dataset: dict[str, Any]
    flow_summary: dict[str, Any]
    operation: str
    stop_after: str
    technical_description: str
    assumptions: list[str]
    scope: str
    processes: list[dict[str, Any]]
    process_exchanges: list[dict[str, Any]]
    matched_process_exchanges: list[dict[str, Any]]
    process_datasets: list[dict[str, Any]]


def _ensure_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        parsed = parse_json_response(value)
        if isinstance(parsed, dict):
            return parsed
    raise ValueError("Expected a JSON object")


def _language_entry(text: str, lang: str = "en") -> dict[str, str]:
    return {"@xml:lang": lang, "#text": text}


def _pick_lang(value: Any, *, prefer: str = "en") -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        text = value.strip()
        return text or None
    if isinstance(value, dict):
        text = value.get("#text") or value.get("text") or value.get("@value")
        if isinstance(text, str) and text.strip():
            return text.strip()
        for nested in value.values():
            candidate = _pick_lang(nested, prefer=prefer)
            if candidate:
                return candidate
        return None
    if isinstance(value, list):
        preferred = None
        fallback = None
        for item in value:
            if isinstance(item, dict):
                lang = str(item.get("@xml:lang") or "").strip().lower()
                text = item.get("#text")
                if isinstance(text, str) and text.strip():
                    if lang == prefer.lower() and preferred is None:
                        preferred = text.strip()
                    if fallback is None:
                        fallback = text.strip()
            else:
                if fallback is None:
                    fallback = _pick_lang(item, prefer=prefer)
        return preferred or fallback
    return str(value).strip() or None


def _flow_summary(flow_dataset: dict[str, Any]) -> dict[str, Any]:
    flow = flow_dataset.get("flowDataSet") if isinstance(flow_dataset.get("flowDataSet"), dict) else flow_dataset
    info = flow.get("flowInformation", {}) if isinstance(flow, dict) else {}
    data_info = info.get("dataSetInformation", {}) if isinstance(info, dict) else {}
    name_block = data_info.get("name", {}) if isinstance(data_info, dict) else {}
    admin = flow.get("administrativeInformation", {}) if isinstance(flow, dict) else {}
    publication = admin.get("publicationAndOwnership", {}) if isinstance(admin, dict) else {}

    base_name_en = _pick_lang(name_block.get("baseName"), prefer="en")
    base_name_zh = _pick_lang(name_block.get("baseName"), prefer="zh")
    treatment_en = _pick_lang(name_block.get("treatmentStandardsRoutes"), prefer="en")
    mix_en = _pick_lang(name_block.get("mixAndLocationTypes"), prefer="en")
    general_en = _pick_lang(data_info.get("common:generalComment"), prefer="en")
    general_zh = _pick_lang(data_info.get("common:generalComment"), prefer="zh")

    classification: list[dict[str, Any]] = []
    classification_info = data_info.get("classificationInformation") if isinstance(data_info, dict) else None
    if isinstance(classification_info, dict):
        carrier = classification_info.get("common:classification")
        if isinstance(carrier, dict):
            classes = carrier.get("common:class")
            if isinstance(classes, list):
                classification = [item for item in classes if isinstance(item, dict)]

    return {
        "uuid": str(data_info.get("common:UUID") or "").strip() or None,
        "version": str(publication.get("common:dataSetVersion") or "").strip() or None,
        "base_name_en": base_name_en,
        "base_name_zh": base_name_zh,
        "treatment_en": treatment_en,
        "mix_en": mix_en,
        "general_comment_en": general_en,
        "general_comment_zh": general_zh,
        "classification": classification,
    }


def _as_multilang_list(value: Any, *, default_lang: str = "en") -> list[dict[str, str]]:
    if value is None:
        return []
    if isinstance(value, list):
        out: list[dict[str, str]] = []
        for item in value:
            if isinstance(item, dict) and "#text" in item:
                out.append(_language_entry(str(item.get("#text") or ""), str(item.get("@xml:lang") or default_lang) or default_lang))
            else:
                text = str(item).strip()
                if text:
                    out.append(_language_entry(text, default_lang))
        return [entry for entry in out if entry.get("#text")]
    if isinstance(value, dict) and "#text" in value:
        text = str(value.get("#text") or "").strip()
        if not text:
            return []
        lang = str(value.get("@xml:lang") or default_lang) or default_lang
        return [_language_entry(text, lang)]
    text = str(value).strip()
    return [_language_entry(text, default_lang)] if text else []


def _contact_reference() -> dict[str, Any]:
    ref_object_id = "f4b4c314-8c4c-4c83-968f-5b3c7724f6a8"
    version = "01.00.000"
    return {
        "@refObjectId": ref_object_id,
        "@type": "contact data set",
        "@uri": build_local_dataset_uri("contact data set", ref_object_id, version),
        "@version": version,
        "common:shortDescription": [
            _language_entry("Tiangong LCA Data Working Group", "en"),
            _language_entry("天工LCA数据团队", "zh"),
        ],
    }


def _entry_level_compliance_reference() -> dict[str, Any]:
    return {
        "@refObjectId": ILCD_ENTRY_LEVEL_REFERENCE_ID,
        "@type": "source data set",
        "@uri": build_local_dataset_uri(
            "source data set",
            ILCD_ENTRY_LEVEL_REFERENCE_ID,
            ILCD_ENTRY_LEVEL_REFERENCE_VERSION,
        ),
        "@version": ILCD_ENTRY_LEVEL_REFERENCE_VERSION,
        "common:shortDescription": [_language_entry("ILCD Data Network - Entry-level", "en")],
    }


def _compliance_declarations() -> dict[str, Any]:
    compliance = {"common:referenceToComplianceSystem": _entry_level_compliance_reference()}
    for field, value in COMPLIANCE_DEFAULT_PREFERENCES.items():
        compliance[field] = value
    return {"compliance": compliance}


def _dataset_format_reference() -> dict[str, Any]:
    from tiangong_lca_spec.core.constants import build_dataset_format_reference as _build

    reference = dict(_build())
    reference["common:shortDescription"] = _as_multilang_list(reference.get("common:shortDescription"))
    return reference


def _candidate_reference(candidate: FlowCandidate) -> dict[str, Any]:
    version = candidate.version or "01.01.000"
    uuid_value = candidate.uuid or str(uuid4())
    uri = build_portal_uri("flow", uuid_value, version)
    return {
        "@type": "flow data set",
        "@refObjectId": uuid_value,
        "@version": version,
        "@uri": uri,
        "common:shortDescription": _as_multilang_list(
            {
                "@xml:lang": "en",
                "#text": candidate.base_name,
            }
        ),
    }


def _placeholder_flow_reference(name: str) -> dict[str, Any]:
    identifier = str(uuid4())
    version = "00.00.000"
    uri = build_portal_uri("flow", identifier, version)
    return {
        "@type": "flow data set",
        "@refObjectId": identifier,
        "@version": version,
        "@uri": uri,
        "common:shortDescription": [_language_entry(name or "Unnamed flow", "en")],
        "unmatched:placeholder": True,
    }


def _default_exchange_amount() -> str:
    return "1.0"


def _reference_direction(operation: str | None) -> str:
    op = str(operation or "produce").strip().lower()
    if op in {"treat", "dispose", "disposal", "treatment"}:
        return "Input"
    return "Output"


def _build_langgraph(
    *,
    llm: LanguageModelProtocol | None,
    settings: Settings,
    flow_search_fn: FlowSearchFn,
    selector: CandidateSelector,
) -> Any:
    graph = StateGraph(ProcessFromFlowState)

    def load_flow(state: ProcessFromFlowState) -> ProcessFromFlowState:
        path = Path(state["flow_path"])
        dataset = json.loads(path.read_text(encoding="utf-8"))
        summary = _flow_summary(dataset)
        LOGGER.info("process_from_flow.load_flow", path=str(path), uuid=summary.get("uuid"))
        return {"flow_dataset": dataset, "flow_summary": summary}

    def describe_technology(state: ProcessFromFlowState) -> ProcessFromFlowState:
        if state.get("technical_description"):
            return {}
        if llm is None:
            summary = state.get("flow_summary") or {}
            base_name = summary.get("base_name_en") or "reference flow"
            operation = str(state.get("operation") or "produce").strip().lower()
            verb = "treatment/disposal" if operation in {"treat", "dispose", "disposal", "treatment"} else "production"
            return {
                "technical_description": f"Generic {verb} of {base_name}. Assumptions: unspecified technology route; generic foreground process.",
                "assumptions": ["No quantified inventory available; amounts are placeholders."],
                "scope": "Generic scope",
            }
        payload = {
            "prompt": TECH_DESCRIPTION_PROMPT,
            "context": {
                "operation": state.get("operation") or "produce",
                "flow": state.get("flow_summary") or {},
            },
            "response_format": {"type": "json_object"},
        }
        raw = llm.invoke(payload)
        data = _ensure_dict(raw)
        return {
            "technical_description": str(data.get("technical_description") or "").strip(),
            "assumptions": [str(item) for item in (data.get("assumptions") or []) if str(item).strip()],
            "scope": str(data.get("scope") or "").strip(),
        }

    def split_processes(state: ProcessFromFlowState) -> ProcessFromFlowState:
        if state.get("processes"):
            return {}
        if llm is None:
            summary = state.get("flow_summary") or {}
            base_name = summary.get("base_name_en") or "reference flow"
            operation = str(state.get("operation") or "produce").strip().lower()
            prefix = "Treatment of" if operation in {"treat", "dispose", "disposal", "treatment"} else "Production of"
            return {
                "processes": [
                    {
                        "process_id": "P1",
                        "name": f"{prefix} {base_name}",
                        "description": state.get("technical_description") or "",
                        "is_reference_flow_process": True,
                    }
                ]
            }
        payload = {
            "prompt": PROCESS_SPLIT_PROMPT,
            "context": {
                "flow": state.get("flow_summary") or {},
                "technical_description": state.get("technical_description") or "",
                "operation": state.get("operation") or "produce",
            },
            "response_format": {"type": "json_object"},
        }
        raw = llm.invoke(payload)
        data = _ensure_dict(raw)
        processes = data.get("processes")
        if not isinstance(processes, list):
            raise ValueError("LLM did not return processes[]")
        cleaned: list[dict[str, Any]] = []
        for item in processes:
            if not isinstance(item, dict):
                continue
            process_id = str(item.get("process_id") or item.get("processId") or "").strip()
            if not process_id:
                continue
            cleaned.append(
                {
                    "process_id": process_id,
                    "name": str(item.get("name") or "").strip(),
                    "description": str(item.get("description") or "").strip(),
                    "is_reference_flow_process": bool(item.get("is_reference_flow_process")),
                }
            )
        if not cleaned:
            raise ValueError("No valid process entries returned by LLM")
        if sum(1 for proc in cleaned if proc.get("is_reference_flow_process")) != 1:
            cleaned[0]["is_reference_flow_process"] = True
            for proc in cleaned[1:]:
                proc["is_reference_flow_process"] = False
        return {"processes": cleaned}

    def generate_exchanges(state: ProcessFromFlowState) -> ProcessFromFlowState:
        if state.get("process_exchanges"):
            return {}
        if llm is None:
            summary = state.get("flow_summary") or {}
            base_name = summary.get("base_name_en") or "reference flow"
            direction = _reference_direction(state.get("operation"))
            return {
                "process_exchanges": [
                    {
                        "process_id": "P1",
                        "exchanges": [
                            {
                                "exchangeDirection": direction,
                                "exchangeName": base_name,
                                "generalComment": summary.get("general_comment_en") or "",
                                "unit": None,
                                "amount": None,
                                "is_reference_flow": True,
                            }
                        ],
                    }
                ]
            }
        payload = {
            "prompt": EXCHANGES_PROMPT,
            "context": {
                "flow": state.get("flow_summary") or {},
                "technical_description": state.get("technical_description") or "",
                "processes": state.get("processes") or [],
                "operation": state.get("operation") or "produce",
            },
            "response_format": {"type": "json_object"},
        }
        raw = llm.invoke(payload)
        data = _ensure_dict(raw)
        processes = data.get("processes")
        if not isinstance(processes, list):
            raise ValueError("LLM did not return processes[] for exchanges")
        return {"process_exchanges": [item for item in processes if isinstance(item, dict)]}

    def match_flows(state: ProcessFromFlowState) -> ProcessFromFlowState:
        if state.get("matched_process_exchanges"):
            return {}
        matched: list[dict[str, Any]] = []
        flow_summary = state.get("flow_summary") or {}
        reference_name = flow_summary.get("base_name_en") or ""

        for proc in state.get("process_exchanges") or []:
            process_id = str(proc.get("process_id") or proc.get("processId") or "").strip()
            exchanges = proc.get("exchanges") or []
            if not process_id or not isinstance(exchanges, list):
                continue
            matched_exchanges: list[dict[str, Any]] = []
            for exchange in exchanges:
                if not isinstance(exchange, dict):
                    continue
                name = str(exchange.get("exchangeName") or "").strip()
                comment = str(exchange.get("generalComment") or "").strip() or None
                query = FlowQuery(exchange_name=name or reference_name or "unknown_exchange", description=comment)
                candidates, unmatched = flow_search_fn(query)
                candidates = candidates[:10]
                # Build a minimal exchange dict for selector context.
                selector_exchange = {"exchangeName": query.exchange_name, "generalComment": comment}
                decision = selector.select(query, selector_exchange, candidates)
                selected = decision.candidate
                matched_exchanges.append(
                    {
                        **exchange,
                        "flow_search": {
                            "query": {"exchange_name": query.exchange_name, "description": comment},
                            "candidates": [
                                {
                                    "uuid": cand.uuid,
                                    "base_name": cand.base_name,
                                    "version": cand.version,
                                    "geography": cand.geography,
                                    "classification": cand.classification,
                                    "flow_properties": cand.flow_properties,
                                }
                                for cand in candidates
                            ],
                            "selected_uuid": selected.uuid if selected else None,
                            "selected_reason": decision.reasoning,
                            "selector": decision.strategy,
                            "unmatched": [getattr(item, "base_name", None) for item in (unmatched or [])],
                        },
                    }
                )
            matched.append({"process_id": process_id, "exchanges": matched_exchanges})
        return {"matched_process_exchanges": matched}

    def build_process_datasets(state: ProcessFromFlowState) -> ProcessFromFlowState:
        if state.get("process_datasets"):
            return {}
        flow_summary = state.get("flow_summary") or {}
        target_flow_name = flow_summary.get("base_name_en") or "reference flow"
        tech_description = state.get("technical_description") or ""
        scope = state.get("scope") or ""
        assumptions = state.get("assumptions") or []
        reference_direction = _reference_direction(state.get("operation"))

        process_plans = {str(item.get("process_id") or ""): item for item in (state.get("processes") or []) if isinstance(item, dict)}
        exchange_plans = {str(item.get("process_id") or ""): item for item in (state.get("matched_process_exchanges") or []) if isinstance(item, dict)}
        results: list[dict[str, Any]] = []

        for process_id, plan in process_plans.items():
            process_name = str(plan.get("name") or "").strip() or f"Process {process_id}"
            process_desc = str(plan.get("description") or "").strip() or tech_description
            is_reference_flow_process = bool(plan.get("is_reference_flow_process"))

            proc_uuid = str(uuid4())
            version = "01.01.000"

            process_info_for_classifier = {
                "dataSetInformation": {
                    "name": {
                        "baseName": process_name,
                        "treatmentStandardsRoutes": scope,
                        "mixAndLocationTypes": flow_summary.get("mix_en") or "",
                    },
                    "common:generalComment": process_desc,
                }
            }
            classification_path: list[dict[str, Any]] = []
            if llm is not None:
                try:
                    classifier = ProcessClassifier(llm)
                    classification_path = classifier.run(process_info_for_classifier)
                except Exception as exc:  # pylint: disable=broad-except
                    LOGGER.warning("process_from_flow.classification_failed", process_id=process_id, error=str(exc))
            if not classification_path:
                classification_path = [{"@level": "0", "@classId": "C", "#text": "Manufacturing"}]

            matched_entry = exchange_plans.get(process_id) or {}
            exchanges_raw = matched_entry.get("exchanges") or []
            exchange_items: list[dict[str, Any]] = []
            reference_internal_id: str | None = None
            next_internal_id = 1
            for exchange in exchanges_raw:
                if not isinstance(exchange, dict):
                    continue
                internal_id = str(next_internal_id)
                next_internal_id += 1
                name = str(exchange.get("exchangeName") or "").strip() or "unknown_exchange"
                direction = str(exchange.get("exchangeDirection") or "").strip()
                if direction not in {"Input", "Output"}:
                    direction = "Input"
                if is_reference_flow_process and bool(exchange.get("is_reference_flow")):
                    direction = reference_direction
                selected_uuid = None
                selected_version = None
                flow_search_block = exchange.get("flow_search") if isinstance(exchange.get("flow_search"), dict) else {}
                if isinstance(flow_search_block, dict):
                    selected_uuid = flow_search_block.get("selected_uuid")
                    # Try to resolve selected version from candidates list.
                    candidates = flow_search_block.get("candidates")
                    if isinstance(candidates, list) and selected_uuid:
                        for cand in candidates:
                            if isinstance(cand, dict) and cand.get("uuid") == selected_uuid:
                                selected_version = cand.get("version")
                                break
                if selected_uuid:
                    candidate = FlowCandidate(uuid=str(selected_uuid), base_name=name, version=str(selected_version) if selected_version else None)
                    reference = _candidate_reference(candidate)
                else:
                    reference = _placeholder_flow_reference(name)

                amount = exchange.get("amount")
                amount_text = _default_exchange_amount() if amount in (None, "", 0) else str(amount)

                comment_text = str(exchange.get("generalComment") or "").strip()
                exchange_item = {
                    "@dataSetInternalID": internal_id,
                    "exchangeName": name,
                    "exchangeDirection": direction,
                    "referenceToFlowDataSet": reference,
                    "meanAmount": amount_text,
                    "resultingAmount": amount_text,
                    "dataDerivationTypeStatus": "Estimated",
                }
                if comment_text:
                    exchange_item["generalComment"] = _as_multilang_list(comment_text)
                exchange_items.append(exchange_item)

                if is_reference_flow_process and bool(exchange.get("is_reference_flow")):
                    reference_internal_id = internal_id

            if is_reference_flow_process and reference_internal_id is None:
                # Ensure a reference exchange exists even if LLM failed to mark it.
                reference_internal_id = str(next_internal_id)
                exchange_items.append(
                    {
                        "@dataSetInternalID": reference_internal_id,
                        "exchangeName": target_flow_name,
                        "exchangeDirection": reference_direction,
                        "referenceToFlowDataSet": _placeholder_flow_reference(target_flow_name),
                        "meanAmount": _default_exchange_amount(),
                        "resultingAmount": _default_exchange_amount(),
                        "dataDerivationTypeStatus": "Estimated",
                        "generalComment": _as_multilang_list(flow_summary.get("general_comment_en") or ""),
                    }
                )

            functional_unit = f"1 {flow_summary.get('treatment_en') or 'unit'} of {target_flow_name}".strip()
            if is_reference_flow_process:
                if reference_direction == "Input":
                    functional_unit = f"1 unit of {target_flow_name} treated"
                else:
                    functional_unit = f"1 unit of {target_flow_name}"

            dataset_payload = {
                "processDataSet": {
                    "processInformation": {
                        "dataSetInformation": {
                            "common:UUID": proc_uuid,
                            "name": {
                                "baseName": _as_multilang_list(_language_entry(process_name, "en")),
                                "treatmentStandardsRoutes": _as_multilang_list(_language_entry(scope or "Unspecified treatment", "en")),
                                "mixAndLocationTypes": _as_multilang_list(_language_entry(flow_summary.get("mix_en") or "Unspecified mix/location", "en")),
                            },
                            "classificationInformation": {"common:classification": {"common:class": classification_path}},
                            "common:generalComment": _as_multilang_list(_language_entry(process_desc, "en")),
                        },
                        "quantitativeReference": {
                            "@type": "Reference flow(s)",
                            "referenceToReferenceFlow": reference_internal_id or "1",
                            "functionalUnitOrOther": _as_multilang_list(_language_entry(functional_unit, "en")),
                        },
                        "time": {"common:referenceYear": int(datetime.now(timezone.utc).strftime("%Y"))},
                        "geography": {"locationOfOperationSupplyOrProduction": {"@location": "GLO"}},
                        "technology": {
                            "technologyDescriptionAndIncludedProcesses": _as_multilang_list(
                                _language_entry(
                                    "; ".join([text for text in [tech_description, process_desc, *assumptions] if text]).strip(),
                                    "en",
                                )
                            )
                        },
                    },
                    "exchanges": {"exchange": exchange_items},
                    "modellingAndValidation": {
                        "LCIMethodAndAllocation": {"typeOfDataSet": "Unit process, single operation"},
                        "validation": {"review": {"@type": "Not reviewed"}},
                        "complianceDeclarations": _compliance_declarations(),
                    },
                    "administrativeInformation": {
                        "common:commissionerAndGoal": {"common:referenceToCommissioner": _contact_reference()},
                        "dataEntryBy": {
                            "common:referenceToDataSetFormat": _dataset_format_reference(),
                            "common:referenceToPersonOrEntityEnteringTheData": _contact_reference(),
                        },
                        "publicationAndOwnership": {
                            "common:dataSetVersion": version,
                            "common:permanentDataSetURI": build_portal_uri("process", proc_uuid, version),
                            "common:referenceToOwnershipOfDataSet": _contact_reference(),
                            "common:copyright": "false",
                            "common:licenseType": "Free of charge for all users and uses",
                        },
                    },
                }
            }

            entity = create_process(dataset_payload, validate=False)
            try:
                validated_model = type(entity.model).model_validate(entity.to_json(by_alias=True, exclude_none=False))
                object.__setattr__(entity, "_model", validated_model)
            except Exception as exc:  # pylint: disable=broad-except
                LOGGER.warning("process_from_flow.process_validation_coercion_failed", process_id=process_id, error=str(exc))

            valid = entity.validate(mode="pydantic")
            if not valid:
                errors = entity.last_validation_error()
                LOGGER.warning("process_from_flow.process_not_valid", process_id=process_id, error=str(errors))
            results.append(entity.model.model_dump(mode="json", by_alias=True, exclude_none=True))

        return {"process_datasets": results}

    graph.add_node("load_flow", load_flow)
    graph.add_node("describe_technology", describe_technology)
    graph.add_node("split_processes", split_processes)
    graph.add_node("generate_exchanges", generate_exchanges)
    graph.add_node("match_flows", match_flows)
    graph.add_node("build_process_datasets", build_process_datasets)

    graph.set_entry_point("load_flow")
    graph.add_edge("load_flow", "describe_technology")
    graph.add_conditional_edges(
        "describe_technology",
        lambda state: END if (str(state.get("stop_after") or "").strip().lower() == "tech") else "split_processes",
    )
    graph.add_conditional_edges(
        "split_processes",
        lambda state: END if (str(state.get("stop_after") or "").strip().lower() == "processes") else "generate_exchanges",
    )
    graph.add_conditional_edges(
        "generate_exchanges",
        lambda state: END if (str(state.get("stop_after") or "").strip().lower() == "exchanges") else "match_flows",
    )
    graph.add_conditional_edges(
        "match_flows",
        lambda state: END if (str(state.get("stop_after") or "").strip().lower() == "matches") else "build_process_datasets",
    )
    graph.add_edge("build_process_datasets", END)

    return graph.compile()


@dataclass(slots=True)
class ProcessFromFlowService:
    """Facade that builds ILCD process datasets from a reference flow via LangGraph."""

    llm: LanguageModelProtocol | None = None
    settings: Settings | None = None
    flow_search_fn: FlowSearchFn | None = None
    selector: CandidateSelector | None = None

    def run(
        self,
        *,
        flow_path: str | Path,
        operation: str = "produce",
        initial_state: dict[str, Any] | None = None,
        stop_after: str | None = None,
    ) -> ProcessFromFlowState:
        settings = self.settings or get_settings()
        flow_search_fn = self.flow_search_fn or search_flows
        selector: CandidateSelector
        if self.selector is not None:
            selector = self.selector
        elif self.llm is not None:
            selector = LLMCandidateSelector(self.llm)
        else:
            selector = SimilarityCandidateSelector()

        app = _build_langgraph(
            llm=self.llm,
            settings=settings,
            flow_search_fn=flow_search_fn,
            selector=selector,
        )
        initial: ProcessFromFlowState = {"flow_path": str(flow_path), "operation": operation}
        if stop_after:
            initial["stop_after"] = stop_after
        if initial_state:
            initial.update({k: v for k, v in initial_state.items() if k not in {"flow_path", "operation"}})
        return app.invoke(initial)
