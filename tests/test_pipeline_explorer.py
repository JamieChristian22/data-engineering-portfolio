"""Basic static contract checks for the standalone, privacy-preserving demo."""
from html.parser import HTMLParser
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

class Inspector(HTMLParser):
    def __init__(self):
        super().__init__()
        self.ids = set()
        self.labels = set()
        self.controls = set()
        self.external_assets = []
    def handle_starttag(self, tag, attrs):
        a = dict(attrs)
        if "id" in a:
            assert a["id"] not in self.ids
            self.ids.add(a["id"])
        if tag == "label" and a.get("for"):
            self.labels.add(a["for"])
        if tag in {"input", "select"} and a.get("id"):
            self.controls.add(a["id"])
        if tag in {"script", "link"} and (a.get("src", "").startswith("http") or a.get("href", "").startswith("http")):
            self.external_assets.append(tag)

def test_explorer_is_accessible_and_offline():
    page = (ROOT / "demo/pipeline-explorer.html").read_text(encoding="utf-8")
    inspector = Inspector()
    inspector.feed(page)
    assert {"reportFile", "category", "status"}.issubset(inspector.controls)
    assert {"reportFile", "category", "status"}.issubset(inspector.labels)
    assert 'aria-live="polite"' in page
    assert 'type="file"' in page
    assert "textContent" in page
    assert "innerHTML" not in page
    assert not inspector.external_assets

def test_report_file_not_tracked_by_default():
    ignore = (ROOT / ".gitignore").read_text(encoding="utf-8")
    assert "demo/latest-run.json" in ignore
    assert ".env" in ignore
    assert "projects/*/artifacts/" in ignore
