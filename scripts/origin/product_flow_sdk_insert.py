#!/usr/bin/env python
"""
Create ILCD product flows with tidas_sdk and optionally insert via Database_CRUD_Tool.

Defaults:
- Input: input_data/origin/manual_flows/flow_class_with_desc.json (array of {class_id, leaf_name, desc, leaf_name_zh})
- Classification: SDK product flow category schema (tidas_flows_product_category.json)
- Output: artifacts/cache/manual_flows/{classid}_{uuid}_{version}.json|.xml
- Flow property: Mass (UUID 93a60a56-a3c8-11da-a746-0800200b9a66) version 03.00.003, meanValue 1.0; timestamp in UTC (YYYY-MM-DDTHH:MM:SSZ)

Usage examples:
  uv run python scripts/origin/product_flow_sdk_insert.py --limit 2              # dry-run, writes files
  uv run python scripts/origin/product_flow_sdk_insert.py --commit --class-id 01142 01151
"""

from __future__ import annotations

import argparse
import json
import re
import tomllib
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from tidas_sdk import create_flow

from tiangong_lca_spec.core.config import get_settings
from tiangong_lca_spec.core.mcp_client import MCPToolClient
from tiangong_lca_spec.tidas.level_hierarchy import HierarchyNavigator, get_product_flow_category_navigator
from tiangong_lca_spec.utils.translate import Translator

TREATMENT_ZH_MAP = {
    "Seed-grade, cleaned for sowing": "种子级，经清理可播种",
    "Harvested grain, unprocessed": "收获谷物，未加工",
    "Fresh, unprocessed produce": "鲜品，未加工",
    "Raw milk, chilled": "生奶，冷藏",
    "Eggs, shell-on": "带壳鸡蛋",
    "Greasy wool, unscoured": "原毛，未洗净",
    "Raw honey": "原蜜",
    "Unprocessed roundwood": "原木，未加工",
    "Unprocessed catch, landing quality": "原始捕获物，卸港品质",
    "Live animal, unprocessed": "活体，未加工",
    "Finished product, manufactured": "成品，已制造",
    "Unspecified treatment": "未指定处理",
}

MIX_ZH_MAP = {
    "Production mix, at farm gate": "生产混合，在农场",
    "Production mix, at forest roadside": "生产混合，在林道",
    "Production mix, at landing site": "生产混合，在渔港",
    "Production mix, at plant": "生产混合，在工厂",
    "Consumption mix, at plant": "消费混合，在工厂",
    "Production mix, to consumer": "生产混合，至消费者",
    "Consumption mix, to consumer": "消费混合，至消费者",
}


def _lang_entry(text: str, lang: str = "en") -> dict[str, str]:
    return {"@xml:lang": lang, "#text": text}


def _replace_semicolons(text: str) -> str:
    """Enforce name fields avoid semicolons by replacing with commas (ASCII + full-width)."""
    return text.replace("；", "，").replace(";", ",")


CONTACT_REF = {
    "@type": "contact data set",
    "@refObjectId": "f4b4c314-8c4c-4c83-968f-5b3c7724f6a8",
    "@uri": "../contacts/f4b4c314-8c4c-4c83-968f-5b3c7724f6a8_01.00.000.xml",
    "@version": "01.00.000",
    "common:shortDescription": [
        _lang_entry("Tiangong LCA Data Working Group", "en"),
        _lang_entry("天工LCA数据团队", "zh"),
    ],
}

COMPLIANCE_REF = {
    "common:referenceToComplianceSystem": {
        "@refObjectId": "d92a1a12-2545-49e2-a585-55c259997756",
        "@type": "source data set",
        "@uri": "../sources/d92a1a12-2545-49e2-a585-55c259997756_20.20.002.xml",
        "@version": "20.20.002",
        "common:shortDescription": [_lang_entry("ILCD Data Network - Entry-level", "en")],
    },
    "common:approvalOfOverallCompliance": "Fully compliant",
}

FORMAT_REF = {
    "@refObjectId": "a97a0155-0234-4b87-b4ce-a45da52f2a40",
    "@type": "source data set",
    "@uri": "../sources/a97a0155-0234-4b87-b4ce-a45da52f2a40_03.00.003.xml",
    "@version": "03.00.003",
    "common:shortDescription": [_lang_entry("ILCD format", "en")],
}

FLOW_PROPERTY_REF = {
    "@refObjectId": "93a60a56-a3c8-11da-a746-0800200b9a66",
    "@type": "flow property data set",
    "@uri": "../flowproperties/93a60a56-a3c8-11da-a746-0800200b9a66_03.00.003.xml",
    "@version": "03.00.003",
    "common:shortDescription": [_lang_entry("Mass", "en")],
}


def load_entries(path: Path) -> list[dict[str, Any]]:
    data = json.loads(path.read_text(encoding="utf-8"))
    return [item for item in data if isinstance(item, dict) and item.get("class_id")]


def build_class_path(navigator: HierarchyNavigator, target: str) -> list[dict[str, str]]:
    """Return classification path using the product flow category navigator."""
    path_entries = navigator.path(target)
    if not path_entries:
        raise ValueError(f"Classification code {target} not found in product flow category schema")
    return [{"@level": str(entry.level), "@classId": entry.code, "#text": entry.description} for entry in path_entries]


def build_dataset(
    entry: dict[str, Any],
    class_nav: HierarchyNavigator,
    *,
    llm_model: str | None = None,
    translator: Translator | None = None,
    llm_suggestions: list[dict[str, Any]] | None = None,
) -> tuple[str, dict[str, Any]]:
    code = str(entry["class_id"]).strip()
    base_en_raw = str(entry.get("leaf_name") or code).strip()
    base_zh_raw = str(entry.get("leaf_name_zh") or base_en_raw).strip() or base_en_raw
    base_en = _replace_semicolons(base_en_raw)
    base_zh = _replace_semicolons(base_zh_raw)
    desc_en = str(entry.get("desc") or f"Flow for {base_en}").strip()
    class_path = build_class_path(class_nav, code)
    flow_uuid = str(uuid4())
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    def has_kw(text: str, keywords: set[str]) -> bool:
        lowered = text.lower()
        tokens = re.findall(r"[a-zA-Z]+", lowered)
        for kw in keywords:
            if " " in kw:
                if kw.lower() in lowered:
                    return True
            else:
                if kw.lower() in tokens or f"{kw.lower()}s" in tokens:
                    return True
        return False

    def call_llm_treatment_mix(model: str | None) -> tuple[str | None, str | None, str | None, str | None]:
        try:
            from openai import OpenAI

            client_kwargs = {}
            openai_config_path = Path(".secrets/secrets.toml")
            if openai_config_path.exists():
                try:
                    secrets = tomllib.loads(openai_config_path.read_text(encoding="utf-8"))
                    openai_conf = secrets.get("openai", {})
                    api_key = openai_conf.get("api_key")
                    if api_key:
                        client_kwargs["api_key"] = api_key
                    if not model and openai_conf.get("model"):
                        model = openai_conf.get("model")
                except Exception:
                    pass
            client = OpenAI(**client_kwargs)
            model_name = model or "gpt-4o-mini"
            mix_options = [
                "Production mix, at farm gate",
                "Production mix, at forest roadside",
                "Production mix, at landing site",
                "Production mix, at plant",
                "Consumption mix, at plant",
                "Production mix, to consumer",
                "Consumption mix, to consumer",
            ]
            treatment_options = [
                "Seed-grade, cleaned for sowing",
                "Harvested grain, unprocessed",
                "Fresh, unprocessed produce",
                "Raw milk, chilled",
                "Eggs, shell-on",
                "Greasy wool, unscoured",
                "Raw honey",
                "Unprocessed roundwood",
                "Unprocessed catch, landing quality",
                "Live animal, unprocessed",
                "Finished product, manufactured",
                "Unspecified treatment",
            ]
            prompt = (
                "You provide two ILCD fields for a product flow:\n"
                "- treatmentStandardsRoutes: technical qualifiers (treatment received, standard fulfilled, product quality, use info, production route name), comma-separated.\n"
                "- mixAndLocationTypes: production/consumption mix and delivery point (e.g., at plant / at farm gate / at forest roadside / at landing site / to consumer), comma-separated.\n"
                "Select ONLY from the given options; do not invent new text. If the flow is a finished manufactured product, prefer 'Finished product, manufactured' + 'Production mix, at plant'.\n"
                "If the flow is clearly agricultural/livestock/forestry/fish, pick the matching farm gate / forest roadside / landing site + corresponding treatment. Otherwise keep plant.\n"
                'Respond strict JSON: {"treatment_en": <option>, "mix_en": <option>} with no extra keys.\n'
                f"class_id: {code}\n"
                f"leaf_name: {base_en}\n"
                f"description: {desc_en or 'N/A'}\n"
                f"treatment_options: {treatment_options}\n"
                f"mix_options: {mix_options}"
            )
            resp = client.chat.completions.create(
                model=model_name,
                messages=[
                    {"role": "system", "content": "Select treatmentStandardsRoutes and mixAndLocationTypes using provided options only. Reply JSON."},
                    {"role": "user", "content": prompt},
                ],
                max_completion_tokens=200,
                temperature=0,
                response_format={"type": "json_object"},
            )
            content = resp.choices[0].message.content if resp.choices else None
            if not content:
                return None, None, None, None
            parsed = json.loads(content)
            llm_treat = parsed.get("treatment_en")
            llm_mix = parsed.get("mix_en")
            if llm_treat not in treatment_options or llm_mix not in mix_options:
                return None, None, None, None
            return llm_treat, llm_mix, model_name, content
        except Exception:
            return None, None, None, None

    llm_treat, llm_mix, model_used, raw_content = call_llm_treatment_mix(llm_model)
    if not (llm_treat and llm_mix):
        raise ValueError(f"LLM did not return treatment/mix for class_id {code}")
    treatment_en = llm_treat
    treatment_zh = TREATMENT_ZH_MAP.get(llm_treat, "未指定处理")
    mix_en = llm_mix
    mix_zh = MIX_ZH_MAP.get(llm_mix, "生产混合，在工厂")
    if llm_suggestions is not None:
        llm_suggestions.append(
            {
                "class_id": code,
                "base_en": base_en,
                "treatment_en": treatment_en,
                "mix_en": mix_en,
                "model": model_used,
                "raw": raw_content,
            }
        )

    treatment_en = _replace_semicolons(treatment_en)
    treatment_zh = _replace_semicolons(treatment_zh)
    mix_en = _replace_semicolons(mix_en)
    mix_zh = _replace_semicolons(mix_zh)

    desc_clean = _replace_semicolons(desc_en)
    en_comment = desc_clean
    zh_comment = desc_clean
    if translator:
        if any("\u4e00" <= ch <= "\u9fff" for ch in desc_clean):
            zh_comment = desc_clean
            translated_en = translator.translate(desc_clean, "en")
            if translated_en:
                en_comment = _replace_semicolons(translated_en)
        else:
            en_comment = desc_clean
            translated_zh = translator.translate(desc_clean, "zh")
            if translated_zh:
                zh_comment = _replace_semicolons(translated_zh)

    flow_dataset = {
        "@xmlns": "http://lca.jrc.it/ILCD/Flow",
        "@xmlns:common": "http://lca.jrc.it/ILCD/Common",
        "@xmlns:ecn": "http://eplca.jrc.ec.europa.eu/ILCD/Extensions/2018/ECNumber",
        "@xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
        "@xsi:schemaLocation": "http://lca.jrc.it/ILCD/Flow ../../schemas/ILCD_FlowDataSet.xsd",
        "@version": "1.1",
        "@locations": "../ILCDLocations.xml",
        "flowInformation": {
            "dataSetInformation": {
                "common:UUID": flow_uuid,
                "name": {
                    "baseName": [
                        _lang_entry(base_en, "en"),
                        _lang_entry(base_zh, "zh"),
                    ],
                    "treatmentStandardsRoutes": [
                        _lang_entry(treatment_en, "en"),
                        _lang_entry(treatment_zh, "zh"),
                    ],
                    "mixAndLocationTypes": [
                        _lang_entry(mix_en, "en"),
                        _lang_entry(mix_zh, "zh"),
                    ],
                },
                "classificationInformation": {"common:classification": {"common:class": class_path}},
                "common:generalComment": [
                    _lang_entry(en_comment, "en"),
                    _lang_entry(zh_comment, "zh"),
                ],
            },
            "quantitativeReference": {"referenceToReferenceFlowProperty": "0"},
        },
        "modellingAndValidation": {
            "LCIMethod": {"typeOfDataSet": "Product flow"},
            "complianceDeclarations": {"compliance": COMPLIANCE_REF},
        },
        "administrativeInformation": {
            "dataEntryBy": {
                "common:timeStamp": ts,
                "common:referenceToDataSetFormat": FORMAT_REF,
                "common:referenceToPersonOrEntityEnteringTheData": CONTACT_REF,
            },
            "publicationAndOwnership": {
                "common:dataSetVersion": "01.01.000",
                "common:referenceToOwnershipOfDataSet": CONTACT_REF,
            },
        },
        "flowProperties": {
            "flowProperty": [
                {
                    "@dataSetInternalID": "0",
                    "meanValue": "1.0",
                    "referenceToFlowPropertyDataSet": FLOW_PROPERTY_REF,
                }
            ]
        },
    }
    return flow_uuid, flow_dataset


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=Path("input_data/origin/manual_flows/flow_class_with_desc.json"), help="JSON array with class_id/leaf_name/desc/leaf_name_zh.")
    parser.add_argument("--output-dir", type=Path, default=Path("artifacts/cache/manual_flows"))
    parser.add_argument("--start-index", type=int, default=0, help="Start index in the input array (0-based).")
    parser.add_argument("--limit", type=int, help="Max number of entries to process from start-index.")
    parser.add_argument("--class-id", nargs="*", help="Process only these class_ids (overrides start/limit if provided).")
    parser.add_argument("--commit", action="store_true", help="Insert into remote DB via Database_CRUD_Tool.")
    parser.add_argument("--llm-model", help="LLM model name (default: gpt-4o-mini or .secrets [openai].model).")
    parser.add_argument("--translate-desc", action="store_true", help="Translate desc (generalComment) via OpenAI.")
    args = parser.parse_args(argv)

    entries = load_entries(args.input)
    if args.class_id:
        entries = [e for e in entries if str(e.get("class_id")) in set(args.class_id)]
    else:
        end_idx = len(entries) if args.limit is None else args.start_index + args.limit
        entries = entries[args.start_index : end_idx]

    if not entries:
        print("No entries to process.")
        return 0

    class_nav = get_product_flow_category_navigator()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    llm_rules_log = args.output_dir / "llm_mix_rules.jsonl"
    settings = get_settings()
    translator = Translator() if args.translate_desc else None

    results = []
    llm_suggestions: list[dict[str, Any]] = []
    client = MCPToolClient(settings) if args.commit else None
    try:
        for entry in entries:
            try:
                flow_uuid, dataset = build_dataset(
                    entry,
                    class_nav,
                    llm_model=args.llm_model,
                    translator=translator,
                    llm_suggestions=llm_suggestions,
                )
                flow = create_flow({"flowDataSet": dataset}, validate=True)
                payload = flow.to_json(by_alias=True, exclude_none=True)

                def normalize_ts(value: Any) -> str:
                    if isinstance(value, datetime):
                        return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                    if isinstance(value, str):
                        try:
                            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
                            return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                        except Exception:
                            return value
                    try:
                        return datetime.fromisoformat(str(value)).astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                    except Exception:
                        return str(value)

                ts_val = payload["flowDataSet"]["administrativeInformation"]["dataEntryBy"]["common:timeStamp"]
                payload["flowDataSet"]["administrativeInformation"]["dataEntryBy"]["common:timeStamp"] = normalize_ts(ts_val)

                version = payload["flowDataSet"]["administrativeInformation"]["publicationAndOwnership"]["common:dataSetVersion"]
                class_id = str(entry["class_id"]).strip()
                basename = f"{class_id}_{flow_uuid}_{version}"
                json_path = args.output_dir / f"{basename}.json"
                xml_path = args.output_dir / f"{basename}.xml"
                json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
                xml_path.write_text(flow.to_xml(), encoding="utf-8")

                resp_id = None
                if client:
                    resp = client.invoke_json_tool(
                        settings.flow_search_service_name,
                        "Database_CRUD_Tool",
                        {"operation": "insert", "table": "flows", "id": flow_uuid, "jsonOrdered": payload},
                    )
                    if isinstance(resp, dict):
                        resp_id = resp.get("id")
                results.append({"class_id": class_id, "uuid": flow_uuid, "version": version, "inserted": bool(resp_id), "rule_source": "llm"})
                print(f"[OK] {class_id} -> {flow_uuid} ({'inserted' if resp_id else 'written only'})")
            except Exception as exc:  # noqa: BLE001
                class_id = entry.get("class_id", "unknown")
                results.append({"class_id": class_id, "uuid": None, "version": None, "inserted": False, "error": str(exc)})
                print(f"[ERR] {class_id}: {exc}")
    finally:
        if client:
            client.close()

    summary_path = args.output_dir / "product_flow_sdk_insert_summary.json"
    summary_path.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")
    if llm_suggestions:
        with llm_rules_log.open("a", encoding="utf-8") as fh:
            for item in llm_suggestions:
                fh.write(json.dumps(item, ensure_ascii=False) + "\n")
    print(f"Done. Summary written to {summary_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
