"""Prompt templates for the process-from-flow LangGraph workflow."""

TECH_DESCRIPTION_PROMPT = (
    "You are an expert LCA practitioner and process engineer.\n"
    "Given a single ILCD flow definition (the 'reference flow'), produce a realistic, generic technical description of "
    "how this flow is produced (or treated/disposed) in industry.\n"
    "\n"
    "Rules:\n"
    "- Base your answer strictly on the provided flow context (name, classification, general comment, treatment/mix fields).\n"
    "- Do NOT invent numeric quantities.\n"
    "- If the flow could be produced by multiple routes, choose the most typical route and state assumptions.\n"
    "- Keep it concise but specific enough to derive inputs/outputs later.\n"
    "\n"
    "Return strict JSON with keys:\n"
    "- technical_description: string (step-by-step paragraph, no bullets)\n"
    "- assumptions: array of strings\n"
    "- scope: short string (e.g., 'cradle-to-gate, at farm gate')\n"
)

PROCESS_SPLIT_PROMPT = (
    "You are decomposing a technical process description into one or more ILCD processes.\n"
    "Input context includes the reference flow summary and a technical description.\n"
    "\n"
    "Rules:\n"
    "- Output 1..5 processes.\n"
    "- Exactly one process MUST be marked as `is_reference_flow_process=true` and must directly produce (or treat) the reference flow.\n"
    "- Use clear, short process names.\n"
    "- Provide `process_id` values like P1, P2, ...\n"
    "\n"
    'Return strict JSON: {"processes": [{"process_id": "P1", "name": "...", "description": "...", "is_reference_flow_process": true|false}]}\n'
)

EXCHANGES_PROMPT = (
    "You are defining the inventory exchanges (inputs/outputs) for each process.\n"
    "Input context includes the reference flow summary, a technical description, and a list of processes.\n"
    "\n"
    "Rules:\n"
    "- Provide plausible exchange names that can be searched in a flow catalogue (prefer English names).\n"
    "- Do NOT invent numeric quantities; set amount to null unless explicitly known.\n"
    "- For every process, output 1..12 exchanges.\n"
    "- For the process with is_reference_flow_process=true, include exactly one exchange that corresponds to the reference flow and set is_reference_flow=true.\n"
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
    '          "is_reference_flow": true|false\n'
    "        }\n"
    "      ]\n"
    "    }\n"
    "  ]\n"
    "}\n"
)
