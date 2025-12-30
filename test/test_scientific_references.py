#!/usr/bin/env python3
"""Test script for process_from_flow with scientific references integration.

This script demonstrates how the enhanced ProcessFromFlowService now uses
scientific literature from tiangong_kb_remote to inform LLM decisions.
"""

import json
from pathlib import Path

import pytest

from tiangong_lca_spec.core.config import get_settings
from tiangong_lca_spec.core.mcp_client import MCPToolClient
from tiangong_lca_spec.process_from_flow.service import (
    ProcessFromFlowService,
    _format_references_for_prompt,
    _search_scientific_references,
)


@pytest.fixture
def flow_path(tmp_path: Path) -> Path:
    flow_path = tmp_path / "flow.json"
    flow_path.write_text(
        json.dumps(
            {
                "flowDataSet": {
                    "flowInformation": {
                        "dataSetInformation": {
                            "common:UUID": "00000000-0000-0000-0000-000000000001",
                            "name": {
                                "baseName": [{"@xml:lang": "en", "#text": "Test flow"}],
                                "treatmentStandardsRoutes": [{"@xml:lang": "en", "#text": "Finished product, manufactured"}],
                                "mixAndLocationTypes": [{"@xml:lang": "en", "#text": "Production mix, at plant"}],
                                "flowProperties": [],
                            },
                            "classificationInformation": {"common:classification": {"common:class": [{"@level": "0", "@classId": "0", "#text": "Test"}]}},
                            "common:generalComment": [{"@xml:lang": "en", "#text": "Test flow general comment."}],
                        }
                    },
                    "administrativeInformation": {"publicationAndOwnership": {"common:dataSetVersion": "01.01.000"}},
                }
            }
        ),
        encoding="utf-8",
    )
    return flow_path


def test_search_references():
    """Test the scientific reference search functionality."""
    print("=" * 80)
    print("Testing Scientific Reference Search")
    print("=" * 80)

    # Test query
    query = "battery production lithium-ion LCA life cycle assessment"
    print(f"\nSearch Query: {query}")

    # Search for references
    with MCPToolClient() as client:
        references = _search_scientific_references(
            query=query,
            mcp_client=client,
            top_k=3,
        )

    print(f"\nFound {len(references)} references")

    # Format references
    if references:
        formatted = _format_references_for_prompt(references)
        print("\nFormatted References:")
        print("-" * 80)
        print(formatted)
        print("-" * 80)
    else:
        print("\nNo references found or service unavailable")


def test_process_from_flow_with_references(flow_path: Path):
    """Test the full ProcessFromFlowService with scientific references.

    Args:
        flow_path: Path to a flow JSON file to process
    """
    print("\n" + "=" * 80)
    print("Testing ProcessFromFlowService with Scientific References")
    print("=" * 80)

    if not flow_path.exists():
        print(f"\nFlow file not found: {flow_path}")
        print("Please provide a valid flow JSON file path to test the full workflow.")
        return

    # Note: This requires a configured LLM and proper API keys
    # The service will automatically create MCP client when LLM is available
    settings = get_settings()

    # Check if we have LLM configuration
    try:
        # Import the LLM wrapper if available
        from tiangong_lca_spec.core.llm import create_llm

        llm = create_llm(settings)
        print("\nLLM configured, proceeding with test...")
    except Exception as e:
        print(f"\nLLM not configured or error: {e}")
        print("Skipping full workflow test.")
        print("\nTo test the full workflow, ensure .secrets/secrets.toml is properly configured.")
        return

    # Create service with LLM
    service = ProcessFromFlowService(llm=llm, settings=settings)

    # Run the workflow (stop after step 1 for testing)
    print(f"\nProcessing flow: {flow_path}")
    print("Running workflow with scientific references integration...")

    try:
        result = service.run(
            flow_path=flow_path,
            operation="produce",
            stop_after="tech",  # Stop after Step 1 for testing
        )

        print("\nWorkflow completed successfully!")
        print("\nTechnology Routes:")
        for route in result.get("technology_routes", []):
            print(f"  - {route.get('route_name')}: {route.get('route_summary')[:100]}...")

    except Exception as e:
        print(f"\nWorkflow failed: {e}")
        import traceback

        traceback.print_exc()


def main():
    """Main test function."""
    print("Scientific References Integration Test")
    print("=" * 80)

    # Test 1: Search references
    test_search_references()

    # Test 2: Full workflow (optional, requires flow file)
    # Uncomment and provide a valid flow path to test
    # flow_path = Path("path/to/your/flow.json")
    # test_process_from_flow_with_references(flow_path)

    print("\n" + "=" * 80)
    print("Tests completed!")
    print("=" * 80)


if __name__ == "__main__":
    main()
