from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, Iterable, Union, Any


def build_id_to_name_map(node: Union[Dict[str, Any], Iterable[Dict[str, Any]]]) -> Dict[str, str]:
    """Traverse CPCClassification3.0_zh-CN.json structure and collect @id -> @name pairs."""
    id_to_name: Dict[str, str] = {}

    def walk(item: Union[Dict[str, Any], Iterable[Dict[str, Any]]]) -> None:
        if isinstance(item, dict):
            node_id = item.get("@id")
            node_name = item.get("@name")
            if node_id and node_name:
                id_to_name[node_id] = node_name
            children = item.get("category")
            if isinstance(children, list):
                for child in children:
                    walk(child)
        elif isinstance(item, list):
            for child in item:
                walk(child)

    walk(node)
    return id_to_name


def merge_names(
    flows: list[dict[str, Any]], id_to_name: Dict[str, str]
) -> tuple[list[dict[str, Any]], list[str]]:
    merged: list[dict[str, Any]] = []
    missing: list[str] = []

    for item in flows:
        class_id = item.get("class_id")
        zh_name = id_to_name.get(class_id)
        if zh_name is None:
            missing.append(str(class_id))
        merged.append({**item, "leaf_name_zh": zh_name})

    return merged, missing


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Merge Chinese names from CPCClassification3.0_zh-CN.json into flow_class_with_desc.json"
    )
    parser.add_argument(
        "--flows",
        type=Path,
        default=Path("flow_class_with_desc.json"),
        help="Path to flow_class_with_desc.json",
    )
    parser.add_argument(
        "--cpc",
        type=Path,
        default=Path("CPCClassification3.0_zh-CN.json"),
        help="Path to CPCClassification3.0_zh-CN.json",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("flow_class_with_desc_zh.json"),
        help="Path to write merged JSON with Chinese names",
    )
    args = parser.parse_args()

    flows_data = json.loads(args.flows.read_text(encoding="utf-8"))
    cpc_data = json.loads(args.cpc.read_text(encoding="utf-8"))

    id_to_name = build_id_to_name_map(cpc_data["CategorySystem"]["categories"])
    merged, missing = merge_names(flows_data, id_to_name)

    args.output.write_text(json.dumps(merged, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    if missing:
        print(f"Missing Chinese names for {len(missing)} class_id values:", ", ".join(sorted(missing)))
    else:
        print("All flow class_ids matched Chinese names.")


if __name__ == "__main__":
    main()
