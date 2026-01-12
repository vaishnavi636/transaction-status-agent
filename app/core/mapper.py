import yaml
from pathlib import Path
from typing import Dict, Any

def _canon(s: str) -> str:
    return str(s).strip().lower().replace(" ", "_")

class SchemaMapper:
    """
    Maps arbitrary input dict keys to canonical keys using mappings.yaml.
    Keeps an audit trail of which source key was used.
    """
    def __init__(self, mappings_path: str):
        self.mappings_path = mappings_path
        self.mapping = {}
        self.load()

    def load(self):
        cfg = yaml.safe_load(Path(self.mappings_path).read_text(encoding="utf-8"))
        raw = cfg.get("canonical_fields", {})
        normed = {}
        for canonical, aliases in raw.items():
            normed[canonical] = {_canon(a) for a in (aliases or [])}
        self.mapping = normed

    def map_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        src = {_canon(k): k for k in row.keys()}  # normalized -> original
        out: Dict[str, Any] = {}
        trace: Dict[str, str] = {}

        for canonical, aliases in self.mapping.items():
            found_key = None
            for a in aliases:
                if a in src:
                    found_key = src[a]
                    break
            if found_key is not None:
                out[canonical] = row.get(found_key)
                trace[canonical] = found_key

        out["_map_trace"] = trace
        return out
