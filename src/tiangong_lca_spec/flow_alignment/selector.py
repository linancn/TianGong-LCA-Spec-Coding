"""Candidate selection strategies for flow alignment."""

from __future__ import annotations

import json
from dataclasses import dataclass
from difflib import SequenceMatcher
from typing import Any, Protocol, Sequence

from tiangong_lca_spec.core.json_utils import parse_json_response
from tiangong_lca_spec.core.logging import get_logger
from tiangong_lca_spec.core.models import FlowCandidate, FlowQuery

LOGGER = get_logger(__name__)


class LanguageModelProtocol(Protocol):
    """Minimal protocol for language models used in candidate selection."""

    def invoke(self, input_data: dict[str, Any]) -> Any: ...


@dataclass(slots=True)
class SelectorDecision:
    """Outcome of a candidate selection operation."""

    candidate: FlowCandidate | None
    score: float | None = None
    reasoning: str | None = None
    strategy: str | None = None


class CandidateSelector(Protocol):
    """Protocol implemented by selection strategies."""

    def select(
        self,
        query: FlowQuery,
        exchange: dict[str, Any],
        candidates: Sequence[FlowCandidate],
    ) -> SelectorDecision: ...


class SimilarityCandidateSelector:
    """Pick the candidate with the highest name-similarity score."""

    def select(
        self,
        query: FlowQuery,
        exchange: dict[str, Any],
        candidates: Sequence[FlowCandidate],
    ) -> SelectorDecision:
        if not candidates:
            return SelectorDecision(candidate=None, score=None, reasoning=None, strategy="similarity")
        scored = [(self._score(query.exchange_name, candidate.base_name), candidate) for candidate in candidates[:10]]
        scored.sort(key=lambda item: item[0], reverse=True)
        best_score, best_candidate = scored[0]
        if best_score <= 0.0:
            return SelectorDecision(candidate=None, score=None, reasoning=None, strategy="similarity")
        return SelectorDecision(
            candidate=best_candidate,
            score=best_score,
            reasoning=f"SequenceMatcher score={best_score:.3f}",
            strategy="similarity",
        )

    @staticmethod
    def _score(reference: str | None, candidate_name: str | None) -> float:
        left = (reference or "").strip().lower()
        right = (candidate_name or "").strip().lower()
        if not left and not right:
            return 0.0
        return SequenceMatcher(None, left, right).ratio()


class LLMCandidateSelector:
    """Leverage an LLM to pick the best candidate, with similarity fallback."""

    PROMPT = (
        "You are matching an inventory exchange from a life cycle assessment dataset to "
        "the best flow definition in Tiangong's flow catalogue. Review the exchange and "
        "the candidate flows carefully. Select the single best candidate or respond with "
        "`best_index: null` if no candidate is appropriate. Prefer candidates whose "
        "flow name, geography, classification, and general comments best align with the "
        "exchange details. Return strict JSON with keys:\n"
        "- `best_index`: integer index into the candidates array (0-based) or null.\n"
        "- `confidence`: number between 0 and 1 estimating confidence (optional).\n"
        "- `reason`: short natural-language justification.\n"
        "Do not include extra commentary."
    )

    def __init__(
        self,
        llm: LanguageModelProtocol,
        *,
        fallback: CandidateSelector | None = None,
    ) -> None:
        self._llm = llm
        self._fallback = fallback or SimilarityCandidateSelector()

    def select(
        self,
        query: FlowQuery,
        exchange: dict[str, Any],
        candidates: Sequence[FlowCandidate],
    ) -> SelectorDecision:
        if not candidates:
            return SelectorDecision(candidate=None, score=None, reasoning=None, strategy="llm")
        try:
            payload = {
                "prompt": self.PROMPT,
                "context": self._build_context(query, exchange, candidates),
            }
            raw_response = self._llm.invoke(payload)
            parsed = self._parse_response(raw_response)
            best_index = parsed.get("best_index")
            if best_index is None:
                return SelectorDecision(
                    candidate=None,
                    score=self._coerce_float(parsed.get("confidence")),
                    reasoning=parsed.get("reason"),
                    strategy="llm",
                )
            if not isinstance(best_index, int) or not 0 <= best_index < len(candidates):
                LOGGER.warning(
                    "flow_alignment.selector.invalid_index",
                    index=best_index,
                    candidate_count=len(candidates),
                )
                return self._fallback.select(query, exchange, candidates)
            candidate = candidates[best_index]
            return SelectorDecision(
                candidate=candidate,
                score=self._coerce_float(parsed.get("confidence")),
                reasoning=parsed.get("reason"),
                strategy="llm",
            )
        except Exception as exc:  # pylint: disable=broad-except
            LOGGER.warning("flow_alignment.selector.llm_failed", error=str(exc))
            return self._fallback.select(query, exchange, candidates)

    def _build_context(
        self,
        query: FlowQuery,
        exchange: dict[str, Any],
        candidates: Sequence[FlowCandidate],
    ) -> str:
        summary = {
            "exchange": {
                "exchange_name": query.exchange_name,
                "process_name": query.process_name,
                "description": query.description,
                "direction": exchange.get("exchangeDirection") or exchange.get("direction"),
                "unit": exchange.get("unit") or exchange.get("resultingAmountUnit"),
                "mean_amount": exchange.get("meanAmount") or exchange.get("resultingAmount"),
                "general_comment": self._stringify_comment(exchange),
            },
            "candidates": [
                {
                    "index": idx,
                    "base_name": candidate.base_name,
                    "uuid": candidate.uuid,
                    "geography": candidate.geography,
                    "classification": candidate.classification,
                    "general_comment": candidate.general_comment,
                    "reasoning": candidate.reasoning,
                }
                for idx, candidate in enumerate(candidates[:10])
            ],
        }
        return json.dumps(summary, ensure_ascii=False)

    @staticmethod
    def _stringify_comment(exchange: dict[str, Any]) -> str | None:
        comment = exchange.get("generalComment") or exchange.get("comment")
        if comment is None:
            return None
        if isinstance(comment, dict):
            text = comment.get("#text") or comment.get("text") or comment.get("@value")
            if text:
                return str(text)
        return str(comment)

    @staticmethod
    def _parse_response(raw_response: Any) -> dict[str, Any]:
        if isinstance(raw_response, dict):
            return raw_response
        if not isinstance(raw_response, str):
            raw_response = str(raw_response)
        return parse_json_response(raw_response)

    @staticmethod
    def _coerce_float(value: Any) -> float | None:
        try:
            if value is None:
                return None
            return float(value)
        except (TypeError, ValueError):
            return None
