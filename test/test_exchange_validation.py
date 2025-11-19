from tiangong_lca_spec.process_extraction.hints import enrich_exchange_hints
from tiangong_lca_spec.process_extraction.validators import validate_exchanges_strict


def _liquid_nitrogen_exchange():
    return {
        "exchangeName": "Liquid nitrogen",
        "exchangeDirection": "Input",
        "meanAmount": "3.80E+01",
        "resultingAmount": "3.80E+01",
        "unit": "kg",
        "flowHints": {
            "basename": "Liquid nitrogen",
            "treatment": "High purity, cryogenic liquid",
            "mix_location": "Production mix, at plant (CN)",
            "flow_properties": "Mass flow, kg",
            "en_synonyms": ["Liquid nitrogen", "Nitrogen, liquid", "LN2"],
            "zh_synonyms": ["液氮"],
            "abbreviation": "LN2",
            "state_purity": "Liquid, 99.999% (5N)",
            "source_or_pathway": "Air separation unit, regional supply (CN)",
            "usage_context": "Input to all-component physical recovery line",
            "formula_or_CAS": "N2; 7727-37-9",
        },
    }


def test_validate_exchanges_strict_accepts_complete_exchange():
    exchange = _liquid_nitrogen_exchange()
    errors = validate_exchanges_strict([exchange], geography="CN")
    assert errors == []


def test_validate_exchanges_strict_rejects_placeholders():
    exchange = _liquid_nitrogen_exchange()
    exchange["exchangeName"] = "LN2"
    exchange["flowHints"]["basename"] = "LN2"
    exchange["flowHints"]["mix_location"] = "GLO"
    errors = validate_exchanges_strict([exchange], geography="CN")
    assert any("exchangeName" in err for err in errors)
    assert any("mix_location" in err for err in errors)


def test_enrich_exchange_hints_serialises_structured_hints():
    exchange = _liquid_nitrogen_exchange()
    hints = enrich_exchange_hints(exchange)
    assert hints["basename"] == "Liquid nitrogen"
    comment = exchange["generalComment"]["#text"]
    assert comment.startswith("FlowSearch hints:")
    assert "basename=Liquid nitrogen" in comment
