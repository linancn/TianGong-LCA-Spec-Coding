"""Namespace package for Tiangong LCA workflow scripts.

The `md` subpackage contains the original paper/markdown staged CLIs, while
`jsonld` hosts the OpenLCA JSON-LD tooling. Import from these subpackages to
access the individual stage modules (e.g., `scripts.md.stage1_preprocess`).
"""

__all__ = ["md", "jsonld"]
