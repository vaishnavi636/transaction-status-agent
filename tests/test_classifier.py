from app.core.classifier import Agent

def test_success_classification():
    agent = Agent("configs/mappings.yaml", "configs/status_synonyms.yaml", "configs/rules.yaml")
    row = {"transaction_id": "t1", "status": "completed", "updated_at": "2026-01-01T00:00:00Z"}
    d = agent.evaluate(row)
    assert d.status == "SUCCESS"
    assert d.confidence >= 0.9

def test_failed_by_error_code_override():
    agent = Agent("configs/mappings.yaml", "configs/status_synonyms.yaml", "configs/rules.yaml")
    row = {"transaction_id": "t2", "status": "processing", "error_code": "E401", "updated_at": "2026-01-01T00:00:00Z"}
    d = agent.evaluate(row)
    assert d.status == "FAILED"
    assert "hard_fail_error_code" in d.rules_fired
