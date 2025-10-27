# Database_CRUD_Tool Usage

This note captures the behaviour and payload requirements of `Database_CRUD_Tool` exposed by the `tiangong_lca_remote` MCP service. The tool provides basic CRUD access to the Tiangong Supabase-backed catalogue (flows, processes, etc.), and is used by the publish stage to persist newly extracted datasets.

## Input Schema

`Database_CRUD_Tool` expects a JSON object with the following fields:

| Field | Type | Required | Notes |
| --- | --- | --- | --- |
| `operation` | `"select" | "insert" | "update" | "delete"` | ✓ | Determines CRUD action. |
| `table` | `"contacts" | "flows" | "lifecyclemodels" | "processes" | "sources"` | ✓ | Target Supabase table. |
| `jsonOrdered` | JSON value | insert/update | Complete ILCD payload stored in the `json_ordered` column. For `insert` the value must include the dataset root object (e.g. `{"flowDataSet": {...}}`). |
| `id` | UUID string | update/delete, optional filter for select | Primary key in the chosen table. When provided during `select` the result is filtered to that record. |
| `version` | string | update/delete | Dataset version string stored in the `version` column. |
| `filters` | object | optional (select) | Equality filters, e.g. `{"common:UUID": "..."}`. |
| `limit` | integer | optional (select) | Maximum number of rows for select queries. Must be > 0. |

`jsonOrdered` should always mirror the canonical ILCD structure. The Supabase validator checks for required attributes such as `@xmlns`, dataset references (`common:referenceToComplianceSystem`, `common:referenceToDataSetFormat`, etc.), and an ISO timestamp formatted as `YYYY-MM-DDTHH:MM:SSZ`.

## Response Format

- On success the tool returns a JSON string (or object) with:
  - `id`: UUID of the affected record.
  - `version`: dataset version (update/delete).
  - `data`: array of row objects containing both the raw `json` column and the canonical `json_ordered` representation.
- Validation failures are surfaced as `SpecCodingError` exceptions. The error payload lists each invalid path, expected type/value, and a message from the Supabase row-level validator.

Example `insert` response:

```json
{
  "id": "6eb84a65-989e-4586-998a-1e7630a18003",
  "data": [
    {
      "id": "6eb84a65-989e-4586-998a-1e7630a18003",
      "json": { "flowDataSet": { "...": "..." } },
      "json_ordered": { "flowDataSet": { "...": "..." } },
      "version": "01.00.000",
      "created_at": "2025-10-27T14:37:13.978371+00:00"
    }
  ]
}
```

## Insert Requirements for Flows

Successful flow inserts must include (minimum viable set):

- Root attributes: `@xmlns`, `@xmlns:common`, `@xmlns:xsi`, `@xmlns:ecn`, `@locations`, `@version`, `@xsi:schemaLocation`.
- `flowInformation.dataSetInformation`:
  - `common:UUID` (new UUID for the flow).
  - `name.baseName` (at least one entry).
  - Optional bilingual synonyms, comments, CAS/EC numbers.
  - For product/waste flows, `classificationInformation.common:classification.common:class`.
  - For elementary flows, use `common:elementaryFlowCategorization`.
  - `common:generalComment` (array of language-tagged entries).
- `quantitativeReference.referenceToReferenceFlowProperty` (typically `"0"`).
- `modellingAndValidation.LCIMethod.typeOfDataSet` with one of `"Product flow"`, `"Waste flow"`, `"Elementary flow"`.
- `modellingAndValidation.complianceDeclarations.compliance.common:referenceToComplianceSystem` referencing an existing source dataset (`d92a1a12-2545-49e2-a585-55c259997756`, version `01.00.000` works) and an `common:approvalOfOverallCompliance` status.
- `administrativeInformation` with:
  - `dataEntryBy.common:timeStamp` (UTC timestamp `YYYY-MM-DDTHH:MM:SSZ`).
  - `dataEntryBy.common:referenceToDataSetFormat` pointing to ILCD format (`a97a0155-0234-4b87-b4ce-a45da52f2a40`, version `01.00.000`).
  - `dataEntryBy.common:referenceToPersonOrEntityEnteringTheData` pointing to a contact (`1f8176e3-86ba-49d1-bab7-4eca2741cdc1`, version `01.00.005`).
  - `publicationAndOwnership.common:dataSetVersion` (e.g. `01.00.000`) and `common:referenceToOwnershipOfDataSet` (`f4b4c314-8c4c-4c83-968f-5b3c7724f6a8`, version `01.00.000`).
- `flowProperties.flowProperty` referencing at least one flow property dataset (mass property `93a60a56-a3c8-11da-a746-0800200b9a66`, version `01.00.000`).

## Delete / Update

- `delete` requires both `id` and `version`. The tool returns the removed row payload.
- `update` requires `id`, `version`, and the updated `jsonOrdered`. The server enforces optimistic concurrency on the version column.

## Operational Notes

- Inserts create records that are immediately visible to the current user (row-level security is enabled).
- When the payload is invalid the validator returns the exact JSON path and the expected type/value, making it easier to adjust the builder.
- For large batch insertions prefer batching at the client level – the MCP tool handles one request at a time.
