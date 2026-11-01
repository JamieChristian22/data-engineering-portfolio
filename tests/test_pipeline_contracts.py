"""Fast regression tests that run without requiring generated datasets."""
import importlib.util
import json
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

def load_verifier():
    spec = importlib.util.spec_from_file_location("verify_outputs", ROOT / "scripts/verify_outputs.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

def test_launcher_can_list_from_different_directory(tmp_path):
    proc = subprocess.run(["bash", str(ROOT / "run_demo_all.sh"), "--list"], cwd=tmp_path,
                          text=True, capture_output=True)
    assert proc.returncode == 0, proc.stderr
    for project in ("DE02", "DE03", "DE04", "AI01", "AI02", "AI03"):
        assert project in proc.stdout

def test_launcher_rejects_unknown_project(tmp_path):
    proc = subprocess.run(["bash", str(ROOT / "run_demo_all.sh"), "--only", "UNKNOWN"],
                          cwd=tmp_path, text=True, capture_output=True)
    assert proc.returncode == 2
    assert "Unknown demo" in proc.stderr

def test_verifier_fails_when_artifacts_are_missing(tmp_path):
    result, passed = load_verifier().verify(tmp_path)
    assert not passed
    assert len(result["projects"]) == 6
    assert all(entry["status"] == "ERROR" for entry in result["projects"])

def test_verifier_accepts_six_synthetic_contract_fixtures(tmp_path):
    import pathlib
    module = load_verifier()
    examples = {
        "DE02": {"event_rows_total": 5, "rollup_rows": 2},
        "DE03": {"applied_rows": 3, "new_last_cdc_id": 3},
        "DE04": {"overall_pass": False, "checks": [{"name": "freshness", "pass": False}],
                 "metrics": {"rows": 8}},
        "AI01": {"results": [{"score": 0.5}], "query": "demo"},
        "AI02": {"all_features": [{"feature": "usage_30"}], "flagged_features": []},
        "AI03": {"alerts": 2, "violations": [], "volume": 10}
    }
    for code, (relative, _) in module.PROJECTS.items():
        path = tmp_path / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(examples[code]), encoding="utf-8")
    result, passed = module.verify(tmp_path)
    assert passed, result
    assert all(item["status"] == "VALIDATED" for item in result["projects"])
    quality = next(item for item in result["projects"] if item["project"] == "DE04")
    assert not quality["quality_overall_pass"]  # Alerts are visible; don't falsify passed controls.

def test_de01_password_is_not_committed_as_plaintext():
    cfg = json.loads((ROOT / "projects/DE01_Retail_Orders_ELT_dbt_Postgres/config/config.json").read_text())
    assert "password" not in cfg["postgres"]
    compose = (ROOT / "projects/DE01_Retail_Orders_ELT_dbt_Postgres/docker/docker-compose.yml").read_text()
    assert "127.0.0.1:5432:5432" in compose
    assert "127.0.0.1:8080:8080" in compose
    assert "DE01_POSTGRES_PASSWORD" in compose
