from datetime import datetime, timezone, timedelta
from typing import Any, Dict
from .decision import Decision
from .rules import RuleBook
from .mapper import SchemaMapper
from ..utils.timeparse import parse_dt

def classify_raw(mapped: Dict[str, Any], rulebook: RuleBook) -> Decision:
    rules_fired = []
    trace = mapped.get("_map_trace", {})

    txid = mapped.get("transaction_id")
    if txid is None or str(txid).strip() == "":
        return Decision(
            transaction_id="",
            status="UNKNOWN",
            confidence=0.0,
            reason="Missing transaction_id (unmappable).",
            evidence={"mapping_trace": trace},
            rules_fired=["missing_transaction_id"]
        )

    raw_status = mapped.get("status_raw") or ""
    bucket = rulebook.match_bucket(raw_status)

    updated_at = parse_dt(mapped.get("updated_at"))
    created_at = parse_dt(mapped.get("created_at"))
    err_code = mapped.get("error_code")
    err_msg = mapped.get("error_message")

    evidence = {
        "status_raw": raw_status,
        "bucket": bucket or None,
        "updated_at": updated_at.isoformat() if updated_at else None,
        "created_at": created_at.isoformat() if created_at else None,
        "error_code": err_code,
        "error_message": err_msg,
        "mapping_trace": trace,
    }

    if err_code is not None and str(err_code) in rulebook.hard_fail_error_codes:
        rules_fired.append("hard_fail_error_code")
        return Decision(
            transaction_id=str(txid),
            status="FAILED",
            confidence=0.98,
            reason=f"Error code {err_code} is configured as hard-fail.",
            evidence=evidence,
            rules_fired=rules_fired,
            updated_at=updated_at
        )

    if bucket == "SUCCESS":
        rules_fired.append("status_synonym_success")
        return Decision(
            transaction_id=str(txid),
            status="SUCCESS",
            confidence=0.95,
            reason="status_raw matched configured SUCCESS synonyms.",
            evidence=evidence,
            rules_fired=rules_fired,
            updated_at=updated_at
        )

    if bucket == "FAILED":
        rules_fired.append("status_synonym_failed")
        reason = "status_raw matched configured FAILED synonyms."
        if err_code or err_msg:
            rules_fired.append("error_info_present")
            reason += " Error info present."
        return Decision(
            transaction_id=str(txid),
            status="FAILED",
            confidence=0.95,
            reason=reason,
            evidence=evidence,
            rules_fired=rules_fired,
            updated_at=updated_at
        )

    if rulebook.error_implies_failed and (err_code or err_msg) and bucket in {"", "PENDING", "PROCESSING"}:
        rules_fired.append("error_implies_failed")
        return Decision(
            transaction_id=str(txid),
            status="FAILED",
            confidence=0.85,
            reason="Error info present and config error_implies_failed=true.",
            evidence=evidence,
            rules_fired=rules_fired,
            updated_at=updated_at
        )

    if bucket in {"PENDING", "PROCESSING"}:
        rules_fired.append("status_synonym_non_final")

        if updated_at is None and rulebook.require_updated_at_for_stuck:
            rules_fired.append("stuck_skipped_no_updated_at")
            return Decision(
                transaction_id=str(txid),
                status=bucket,
                confidence=0.80,
                reason="Non-final status; updated_at missing so STUCK not evaluated.",
                evidence=evidence,
                rules_fired=rules_fired,
                updated_at=updated_at
            )

        anchor = updated_at or created_at
        if anchor:
            now = datetime.now(timezone.utc)
            elapsed = now - anchor
            evidence["elapsed_seconds"] = int(elapsed.total_seconds())
            if elapsed > timedelta(minutes=rulebook.stuck_minutes):
                rules_fired.append("stuck_threshold_exceeded")
                return Decision(
                    transaction_id=str(txid),
                    status="STUCK",
                    confidence=0.88,
                    reason=f"Non-final status exceeded stuck threshold ({rulebook.stuck_minutes} minutes).",
                    evidence=evidence,
                    rules_fired=rules_fired,
                    updated_at=updated_at
                )

        return Decision(
            transaction_id=str(txid),
            status=bucket,
            confidence=0.80,
            reason="Non-final status per configured synonyms.",
            evidence=evidence,
            rules_fired=rules_fired,
            updated_at=updated_at
        )

    rules_fired.append("unrecognized_status")
    return Decision(
        transaction_id=str(txid),
        status="UNKNOWN",
        confidence=0.40 if raw_status else 0.20,
        reason="status_raw did not match any configured synonyms (or missing).",
        evidence=evidence,
        rules_fired=rules_fired,
        updated_at=updated_at
    )

class Agent:
    def __init__(self, mappings_path: str, synonyms_path: str, rules_path: str):
        self.mapper = SchemaMapper(mappings_path)
        self.rulebook = RuleBook(synonyms_path, rules_path)

    def evaluate(self, row: Dict[str, Any]) -> Decision:
        mapped = self.mapper.map_row(row)
        return classify_raw(mapped, self.rulebook)
