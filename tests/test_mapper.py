from app.core.mapper import SchemaMapper

def test_mapping_basic():
    mapper = SchemaMapper("configs/mappings.yaml")
    row = {"tx_id": "123", "state": "success", "last_updated": "2026-01-01T00:00:00Z"}
    mapped = mapper.map_row(row)
    assert mapped["transaction_id"] == "123"
    assert mapped["status_raw"] == "success"
    assert mapped["updated_at"] == "2026-01-01T00:00:00Z"
    assert "_map_trace" in mapped
