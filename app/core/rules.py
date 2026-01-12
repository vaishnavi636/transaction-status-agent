import yaml
from pathlib import Path

def _norm(s: str) -> str:
    return str(s).strip().lower().replace(" ", "_")

class RuleBook:
    def __init__(self, synonyms_path: str, rules_path: str):
        self.synonyms_path = synonyms_path
        self.rules_path = rules_path
        self.syn = {}
        self.rules = {}
        self.load()

    def load(self):
        self.syn = yaml.safe_load(Path(self.synonyms_path).read_text(encoding="utf-8"))
        self.rules = yaml.safe_load(Path(self.rules_path).read_text(encoding="utf-8"))
        for k, arr in list(self.syn.items()):
            self.syn[k] = {_norm(x) for x in (arr or [])}

    def normalize_status(self, raw: str) -> str:
        return _norm(raw or "")

    @property
    def stuck_minutes(self) -> int:
        return int(self.rules.get("stuck_threshold_minutes", 30))

    @property
    def error_implies_failed(self) -> bool:
        return bool(self.rules.get("error_implies_failed", True))

    @property
    def require_updated_at_for_stuck(self) -> bool:
        return bool(self.rules.get("require_updated_at_for_stuck", True))

    @property
    def hard_fail_error_codes(self) -> set[str]:
        return {str(x) for x in (self.rules.get("hard_fail_error_codes") or [])}

    def match_bucket(self, raw_status: str) -> str:
        s = self.normalize_status(raw_status)
        if not s:
            return ""
        if s in self.syn.get("success", set()):
            return "SUCCESS"
        if s in self.syn.get("failed", set()):
            return "FAILED"
        if s in self.syn.get("pending", set()):
            return "PENDING"
        if s in self.syn.get("processing", set()):
            return "PROCESSING"
        return ""
