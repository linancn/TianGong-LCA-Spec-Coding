# Product Flow Insert Quickrun / 产品流快速入库指引

本文总结了使用 `tiangong_lca_remote` 的 `Database_CRUD_Tool` 直接入库单条产品流的最简流程，避免不必要的重试和循环。每次仅调用一次 MCP，确认成功后再处理下一条。

## 步骤（中文）
1. 准备环境  
   - 确认 `.secrets/secrets.toml` 中已配置 `[tiangong_lca_remote]`（url/service_name/tool_name/api_key）。  
   - 所有 Python 命令都用 `uv run python ...`。
2. 查分类路径（避免人工找错）  
   ```bash
   uv run python - <<'PY'
   import json, importlib.resources as res
   from dataclasses import dataclass
   from collections import defaultdict

   target = "01132"  # 替换为目标 class_id
   path = res.files("tidas_tools.tidas.schemas") / "tidas_flows_product_category.json"
   doc = json.loads(path.read_text())

   @dataclass
   class Entry: level:int; code:str; desc:str
   entries=[]
   for item in doc.get("oneOf", []):
       props=item.get("properties", {})
       code = next((props.get(k, {}).get("const") for k in ("@classId","@catId","@code") if props.get(k)), None)
       level = props.get("@level", {}).get("const")
       desc = props.get("#text", {}).get("const","")
       if code and level is not None:
           entries.append(Entry(int(level), str(code), str(desc)))
   child_map=defaultdict(list); roots=[]; last={}
   for e in entries:
       if e.level==0:
           roots.append(e); child_map[""].append(e)
       else:
           t=e.level-1; parent=None
           while t>=0 and parent is None:
               parent=last.get(t); t-=1
           if parent: child_map[parent.code].append(e)
       last[e.level]=e
   def find(node):
       if node.code==target: return [node]
       for c in child_map.get(node.code, []):
           r=find(c)
           if r: return [node]+r
   for r in roots:
       path_seq=find(r)
       if path_seq:
           for n in path_seq: print(f"{n.level} {n.code} {n.desc}")
           break
   PY
   ```
3. 填写名称/同义词/注释  
   - baseName 中英各一条；可用 `leaf_name`/`leaf_name_zh` 作为默认名称。treatment/mix 选最贴合的技术表述。  
   - `common:synonyms` 英/中文各一条，避免空数组。  
   - `common:generalComment` 使用来源描述，保持英文。  
   - 默认流属性用 Mass（UUID `93a60a56-a3c8-11da-a746-0800200b9a66`），`meanValue` 设为 1.0。
4. 构造并调用 MCP（单次执行）  
   ```bash
   uv run python - <<'PY'
   import json
   from uuid import uuid4
   from datetime import datetime, timezone
   from tiangong_lca_spec.core.config import get_settings
   from tiangong_lca_spec.core.constants import build_dataset_format_reference
   from tiangong_lca_spec.core.uris import build_local_dataset_uri
   from tiangong_lca_spec.core.mcp_client import MCPToolClient

   def lang(text, lang="en"): return {"@xml:lang": lang, "#text": text}
   def contact_ref():
       uuid="f4b4c314-8c4c-4c83-968f-5b3c7724f6a8"; ver="01.00.000"
       return {"@type":"contact data set","@refObjectId":uuid,"@uri":build_local_dataset_uri("contact data set",uuid,ver),"@version":ver,"common:shortDescription":[lang("Tiangong LCA Data Working Group","en"),lang("天工LCA数据团队","zh")]}
   def compliance_ref():
       uuid="d92a1a12-2545-49e2-a585-55c259997756"; ver="20.20.002"
       return {"common:referenceToComplianceSystem":{"@refObjectId":uuid,"@type":"source data set","@uri":build_local_dataset_uri("source",uuid,ver),"@version":ver,"common:shortDescription":lang("ILCD Data Network - Entry-level","en")},"common:approvalOfOverallCompliance":"Fully compliant"}

   settings=get_settings()
   flow_uuid=str(uuid4()); version="01.01.000"; ts=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
   classification=[  # 替换为上一步的路径
       {"@level":"0","@classId":"0","#text":"Agriculture, forestry and fishery products"},
       {"@level":"1","@classId":"01","#text":"Products of agriculture, horticulture and market gardening"},
       {"@level":"2","@classId":"011","#text":"Cereals"},
       {"@level":"3","@classId":"0113","#text":"Rice"},
       {"@level":"4","@classId":"01132","#text":"Rice paddy, other (not husked)"},
   ]
   description=("This subclass includes:\\n- rice, species of Oryza, mainly Oryza sativa, not husked\\n"
                "This subclass does not include:\\n- rice grown specifically for seed purposes, cf. 01131\\n"
                "- semi-milled or wholly milled rice, whether or not polished or glazed, cf. 23161\\n"
                "- broken rice, cf. 23161\\n- husked rice, cf. 23162")
   flow_dataset={
       "@xmlns":"http://lca.jrc.it/ILCD/Flow","@xmlns:common":"http://lca.jrc.it/ILCD/Common",
       "@xmlns:ecn":"http://eplca.jrc.ec.europa.eu/ILCD/Extensions/2018/ECNumber","@xmlns:xsi":"http://www.w3.org/2001/XMLSchema-instance",
       "@locations":"../ILCDLocations.xml","@version":"1.1","@xsi:schemaLocation":"http://lca.jrc.it/ILCD/Flow ../../schemas/ILCD_FlowDataSet.xsd",
       "flowInformation":{"dataSetInformation":{
           "common:UUID":flow_uuid,
           "name":{"baseName":[lang("Rice paddy, other (not husked)","en"),lang("稻谷（未脱壳，其他）","zh")],
                   "treatmentStandardsRoutes":[lang("Unhusked paddy rice, field harvested","en")],
                   "mixAndLocationTypes":[lang("Production mix, at farm gate","en")]},
           "common:synonyms":[lang("Paddy rice; Unhusked rice; Raw rice grain","en"),lang("稻谷; 未脱壳稻米","zh")],
           "common:generalComment":[lang(description,"en")],
           "classificationInformation":{"common:classification":{"common:class":classification}},
       },"quantitativeReference":{"referenceToReferenceFlowProperty":"0"}},
       "modellingAndValidation":{"LCIMethod":{"typeOfDataSet":"Product flow"},"complianceDeclarations":compliance_ref()},
       "administrativeInformation":{"dataEntryBy":{"common:timeStamp":ts,"common:referenceToDataSetFormat":build_dataset_format_reference(),"common:referenceToPersonOrEntityEnteringTheData":contact_ref()},
           "publicationAndOwnership":{"common:dataSetVersion":version,"common:referenceToOwnershipOfDataSet":contact_ref()}},
       "flowProperties":{"flowProperty":{"@dataSetInternalID":"0","meanValue":"1.0","referenceToFlowPropertyDataSet":{"@type":"flow property data set","@refObjectId":"93a60a56-a3c8-11da-a746-0800200b9a66","@uri":"../flowproperties/93a60a56-a3c8-11da-a746-0800200b9a66.xml","@version":"03.00.003","common:shortDescription":lang("Mass","en")}}}
   }
   payload={"operation":"insert","table":"flows","id":flow_uuid,"jsonOrdered":{"flowDataSet":flow_dataset}}

   client=None
   try:
       client=MCPToolClient(settings)
       result=client.invoke_json_tool(settings.flow_search_service_name,"Database_CRUD_Tool",payload)
       print(json.dumps({"status":"success","id":flow_uuid,"version":version,"result":result},ensure_ascii=False,indent=2))
   finally:
       if client: client.close()
   PY
   ```
5. 单条校验（可选）  
   - 若需确认，可再用一次 `operation=select` 查询刚插入的 `id`。  
   - 避免反复 insert 同一 UUID；重复插入会触发更新逻辑，若需改写请显式走 `update`。

### 批量执行（`scripts/md/bulk_insert_product_flows.py`）
- 输入：JSON/JSONL 数组，字段支持 `class_id`、`leaf_name`、`leaf_name_zh`、`desc`，可选 `base_en`、`base_zh`、`en_synonyms`、`zh_synonyms`、`treatment`、`mix`、`comment`。  
- 运行（默认 dry-run）：  
  ```bash
  uv run python scripts/md/bulk_insert_product_flows.py --input flow_class_with_desc.json
  ```  
  实际入库加 `--commit`，日志写入 `artifacts/bulk_insert_product_flows_log.csv`。
  如需查询已入库：`--select-id <uuid>`。

## Steps (English)
1. Prepare env: ensure `.secrets/secrets.toml` has `tiangong_lca_remote` creds; run Python via `uv run`.  
2. Get classification path with the snippet above; never guess codes manually.  
3. Populate names/synonyms/comment: bilingual `baseName` (fallback to `leaf_name`/`leaf_name_zh`), meaningful treatment/mix, English generalComment from source text; keep Mass property (UUID `93a60a56-a3c8-11da-a746-0800200b9a66`, meanValue `1.0`).  
4. Build one payload and call `Database_CRUD_Tool` once using the Python template; do not loop or retry unless the call fails.  
5. Optional: run a `select` by `id` to confirm. Use `update` only when changing an existing UUID; otherwise generate a new UUID per flow.

### Batch mode (`scripts/md/bulk_insert_product_flows.py`)
- Input JSON/JSONL supports `class_id`, `leaf_name`, `leaf_name_zh`, `desc`, optional `base_en`, `base_zh`, `en_synonyms`, `zh_synonyms`, `treatment`, `mix`, `comment`.  
- Dry-run: `uv run python scripts/md/bulk_insert_product_flows.py --input flow_class_with_desc.json`  
  Add `--commit` to publish; log at `artifacts/bulk_insert_product_flows_log.csv`.  
  Use `--select-id <uuid>` to fetch an existing record.

## 常见避免项 / Avoid
- 不要在失败后盲目重跑同一 insert；先检查 creds、payload 结构或 UUID 冲突。  
- 不要在 Stage 3 流程里触发 alignment，这里只做直接入库。  
- 不要留空 `common:synonyms` 或 `classificationInformation`，否则校验/发布会失败。
