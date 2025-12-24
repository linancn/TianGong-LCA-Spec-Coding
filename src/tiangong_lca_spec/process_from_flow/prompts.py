"""Prompt templates for the process-from-flow LangGraph workflow."""

TECH_DESCRIPTION_PROMPT = (
    "You are an expert LCA practitioner and process engineer.\n"
    "Given a single ILCD flow definition (the 'reference flow'), list the plausible technology/process routes "
    "for producing (or treating/disposal of) the flow.\n"
    "\n"
    "Rules:\n"
    "- Base your answer strictly on the provided flow context (name, classification, general comment, treatment/mix fields).\n"
    "- Do NOT invent numeric quantities.\n"
    "- Output 1..4 routes; if multiple routes are plausible (e.g., different production technologies), include them as separate routes.\n"
    "- Keep each route concise but specific enough to derive unit processes and exchanges later.\n"
    "\n"
    "Return strict JSON:\n"
    "{\n"
    '  "routes": [\n'
    "    {\n"
    '      "route_id": "R1",\n'
    '      "route_name": "...",\n'
    '      "route_summary": "...",\n'
    '      "key_unit_processes": ["..."],\n'
    '      "key_inputs": ["..."],\n'
    '      "key_outputs": ["..."],\n'
    '      "assumptions": ["..."],\n'
    '      "scope": "..."\n'
    "    }\n"
    "  ]\n"
    "}\n"
)

PROCESS_SPLIT_PROMPT = (
    "You are selecting/using the route options and decomposing each route into unit processes (single operations).\n"
    "Input context includes the reference flow summary, the route options from Step 1, and any technical description.\n"
    "\n"
    "Rules:\n"
    "- Output 1..4 routes, each with 1..6 processes.\n"
    "- For each route, processes must be ordered from upstream to downstream (P1 -> P2 -> ...).\n"
    "- If multiple processes in a route, the reference flow of process i must be an input exchange for process i+1; the last process directly produces (or treats/disposes) the reference flow.\n"
    "- Exactly one process per route MUST be marked as `is_reference_flow_process=true` (the last process when multiple).\n"
    "- Use clear, short process names.\n"
    "- Provide `process_id` values like P1, P2, ...\n"
    "- Each process must include structured fields split by: technology/process, inputs, outputs, boundary, assumptions.\n"
    "- Also include exchange keywords (inputs/outputs) as short, searchable English names; do NOT invent quantities.\n"
    "- Each process MUST define reference_flow_name (the main output flow of the process).\n"
    "- Process name must include four modules: base_name, treatment_and_route, mix_and_location, quantitative_reference.\n"
    "- quantitative_reference must be a numeric expression like '1 kg of <reference_flow_name>' or '1 unit of <reference_flow_name>'. If unit is unknown, use 'unit'.\n"
    "- Ensure chain consistency: the reference_flow_name of process i must appear verbatim in process i+1 inputs and exchange_keywords.inputs.\n"
    "- Provide inputs/outputs as clean flow names (no f1/f2 labels); labels are added in post-processing.\n"
    "\n"
    "Return strict JSON:\n"
    "{\n"
    '  "selected_route_id": "R1",\n'
    '  "routes": [\n'
    "    {\n"
    '      "route_id": "R1",\n'
    '      "route_name": "...",\n'
    '      "processes": [\n'
    "        {\n"
    '          "process_id": "P1",\n'
    '          "reference_flow_name": "...",\n'
    '          "name_parts": {\n'
    '            "base_name": "...",\n'
    '            "treatment_and_route": "...",\n'
    '            "mix_and_location": "...",\n'
    '            "quantitative_reference": "..."\n'
    "          },\n"
    '          "name": "...",\n'
    '          "description": "...",\n'
    '          "structure": {\n'
    '            "technology": "...",\n'
    '            "inputs": ["..."],\n'
    '            "outputs": ["..."],\n'
    '            "boundary": "...",\n'
    '            "assumptions": ["..."]\n'
    "          },\n"
    '          "exchange_keywords": {\n'
    '            "inputs": ["..."],\n'
    '            "outputs": ["..."]\n'
    "          },\n"
    '          "is_reference_flow_process": true|false\n'
    "        }\n"
    "      ]\n"
    "    }\n"
    "  ]\n"
    "}\n"
)

EXCHANGES_PROMPT = (
    "You are defining the inventory exchanges (inputs/outputs) for each process.\n"
    "Input context includes the reference flow summary, a technical description, and a list of processes.\n"
    "\n"
    "Rules:\n"
    "- Provide plausible exchange names that can be searched in a flow catalogue (prefer English names).\n"
    "- If process provides structured fields (structure/inputs/outputs) or exchange_keywords, use them as primary candidates.\n"
    "- Preserve chain naming: when process i outputs intermediate flow name X, process i+1 must include X as an input with the exact same string.\n"
    "- Inputs/outputs may be labeled like 'f1: <name>'; strip the label and use only the flow name for exchangeName.\n"
    "- Do NOT use composite exchange names (e.g., 'energy and machinery', 'air emissions', 'auxiliary materials'). Split into specific flows.\n"
    "- For energy, split into carriers such as electricity, diesel, gasoline, natural gas, or heat as applicable.\n"
    "- For emissions, split into elementary flows (e.g., methane, nitrous oxide, ammonia, CO2, NOx, particulates) or waterborne pollutants (e.g., nitrate, phosphate, pesticides) when relevant.\n"
    "- For labor, split by activity if multiple (e.g., 'Labor, harvesting' and 'Labor, post-harvest handling').\n"
    "- Add flow_type for each exchange: product | elementary | waste | service.\n"
    "- Add search_hints as a list of short aliases/synonyms to improve retrieval (e.g., 'Water, fresh' -> 'Freshwater').\n"
    "- For emissions, include 'to air' / 'to water' / 'to soil' in exchangeName when applicable.\n"
    "- Provide unit for each exchange (e.g., kg, kWh, MJ, m3, unit). If unsure, use 'unit'.\n"
    "- Provide amount as a numeric string; use '1' as a placeholder when unknown.\n"
    "- For every process, output 1..12 exchanges.\n"
    "- For each process, include exactly one exchange matching reference_flow_name and set is_reference_flow=true.\n"
    "- For the final process (is_reference_flow_process=true), the reference_flow_name must correspond to the load_flow.\n"
    "- Use exchangeDirection='Output' when operation is produce; use exchangeDirection='Input' when operation is treat/dispose.\n"
    "- Use exchangeDirection exactly 'Input' or 'Output'.\n"
    "\n"
    "Return strict JSON with keys:\n"
    "{\n"
    '  "processes": [\n'
    "    {\n"
    '      "process_id": "P1",\n'
    '      "exchanges": [\n'
    "        {\n"
    '          "exchangeDirection": "Input|Output",\n'
    '          "exchangeName": "...",\n'
    '          "generalComment": "...",\n'
    '          "unit": "...",\n'
    '          "amount": null,\n'
    '          "is_reference_flow": true|false,\n'
    '          "flow_type": "product|elementary|waste|service",\n'
    '          "search_hints": ["..."]\n'
    "        }\n"
    "      ]\n"
    "    }\n"
    "  ]\n"
    "}\n"
)
