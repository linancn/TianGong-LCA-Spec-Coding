import json

from tiangong_lca_spec.process_extraction.preprocess import preprocess_paper


def test_preprocess_corrects_ocr_numeric_artifacts():
    md_json = json.dumps(
        [
            "1 Introduction",
            ("The methanol industry started in the 195Os.\n" "Image Description: 195Os remains.\n" "\n" "More text cites 1O7 million tons."),
            "<table><tr><td>9O</td></tr></table>",
            "Values include 1 . 5 3, 1 6 3 MJ, 2o2l, 11o8.30, and O2/02 samples.",
        ]
    )

    result = preprocess_paper(md_json)

    assert "1950s" in result
    assert "107 million tons" in result
    assert "1.53" in result
    assert "163 MJ" in result
    assert "2021" in result
    assert "1108.30" in result
    assert "02/02" in result
    assert "Image Description: 195Os remains." in result
    assert "<table><tr><td>9O</td></tr></table>" in result


def test_preprocess_removes_non_content_sections():
    md_json = json.dumps(
        [
            "Main article content starts here.",
            "Acknowledgements Thanks to everyone involved.",
            "Still acknowledgement details continuing on second line.",
            "",
            "Author contributions Detail of author efforts.",
            "",
            "Open access",
            "check for updates",
            "Peer review information Editors provided insights.",
            "Reprints and permissions info available online.",
            "",
            "Main text resumes after boilerplate.",
        ]
    )

    result = preprocess_paper(md_json)

    assert "Main article content starts here." in result
    assert "Main text resumes after boilerplate." in result
    assert "Acknowledgements" not in result
    assert "Author contributions" not in result
    assert "Open access" not in result
    assert "Peer review information" not in result
