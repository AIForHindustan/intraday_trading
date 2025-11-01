# File: tools/repo_audit.py
# Purpose: Audit Redis & logging usage across the repo; print culprits + suggested fixes.
# Run: python tools/repo_audit.py --root . --json-out audit_report.json

from __future__ import annotations
import argparse
import ast
import json
import os
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

REDIS_CLIENT_MODULE = ("redis_files.redis_client", "core.data.indicators_store")

PY_PATTERNS = {
    "key_prefix": re.compile(r'(?P<prefix>(?:indicator|indicators|ticks|ohlc|session|health):)[^"\']*', re.I),
}

YAML_HINT_KEYS = {"redis", "host", "port", "db", "username", "password", "uri", "prefix"}

@dataclass
class RedisCtor:
    file: str
    line: int
    func: str  # Redis or StrictRedis
    kwargs: Dict[str, Any]

@dataclass
class RedisUsage:
    file: str
    line: int
    api: str
    key_expr: str

@dataclass
class Finding:
    severity: str  # HIGH/MED/LOW
    code: str
    file: str
    line: int
    message: str
    hint: str

@dataclass
class AuditReport:
    redis_ctors: List[RedisCtor] = field(default_factory=list)
    redis_usages: List[RedisUsage] = field(default_factory=list)
    key_prefixes: Dict[str, int] = field(default_factory=dict)
    decode_misses: List[Finding] = field(default_factory=list)
    drift_db: Dict[str, set] = field(default_factory=dict)
    logging_overrides: List[Finding] = field(default_factory=list)
    import_central_client: List[str] = field(default_factory=list)
    import_raw_redis: List[str] = field(default_factory=list)
    yaml_hints: List[Tuple[str, int, str]] = field(default_factory=list)
    findings: List[Finding] = field(default_factory=list)

def _safe_literal(v: ast.AST) -> Any:
    try:
        return ast.literal_eval(v)
    except Exception:
        return None

class PyVisitor(ast.NodeVisitor):
    def __init__(self, path: Path, report: AuditReport) -> None:
        self.path = path
        self.report = report
        self.uses_central = False
        self.uses_raw_redis = False

    def visit_Import(self, node: ast.Import) -> None:
        for alias in node.names:
            if alias.name.startswith("redis_files.redis_client"):
                self.uses_central = True
            if alias.name == "redis":
                self.uses_raw_redis = True
        self.generic_visit(node)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        if node.module and node.module.startswith("redis_files.redis_client"):
            self.uses_central = True
        if node.module == "redis":
            self.uses_raw_redis = True
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        # Detect redis.Redis(...) or redis.StrictRedis(...)
        target = ""
        if isinstance(node.func, ast.Attribute) and isinstance(node.func.value, ast.Name):
            if node.func.value.id == "redis" and node.func.attr in ("Redis", "StrictRedis"):
                target = node.func.attr
        if target:
            kwargs: Dict[str, Any] = {}
            for kw in node.keywords or []:
                kwargs[kw.arg or ""] = _safe_literal(kw.value)
            self.report.redis_ctors.append(RedisCtor(
                file=str(self.path), line=node.lineno, func=target, kwargs=kwargs
            ))
            # DB drift tracking
            dbv = kwargs.get("db", None)
            if dbv is not None:
                self.report.drift_db.setdefault(str(self.path), set()).add(dbv)
            # decode_responses check primarily for readers; flag if obviously reading
            if kwargs.get("decode_responses") is not True:
                self.report.decode_misses.append(Finding(
                    severity="MED",
                    code="RD-DEC-001",
                    file=str(self.path),
                    line=node.lineno,
                    message="Redis client without decode_responses=True; JSON/text reads may break dashboard.",
                    hint="Use get_redis_client(..., decode_responses=True) for readers or set decode_responses=True.",
                ))
        # Detect known Redis APIs used directly
        if isinstance(node.func, ast.Attribute):
            api = node.func.attr
            if api in {"get", "set", "hset", "hgetall", "scan", "xadd", "xreadgroup", "xgroup_create"}:
                key_expr = ""
                if node.args:
                    try:
                        if hasattr(ast, "unparse"):
                            key_expr = ast.unparse(node.args[0])
                        else:
                            # Fallback for Python < 3.9
                            if isinstance(node.args[0], ast.Constant):
                                key_expr = repr(node.args[0].value)
                            elif isinstance(node.args[0], ast.Str):
                                key_expr = repr(node.args[0].s)
                            elif isinstance(node.args[0], ast.Name):
                                key_expr = node.args[0].id
                            else:
                                key_expr = "<expr>"
                    except Exception:
                        key_expr = "<expr>"
                self.report.redis_usages.append(RedisUsage(
                    file=str(self.path), line=node.lineno, api=api, key_expr=key_expr
                ))
        # Logging override detectors
        if isinstance(node.func, ast.Attribute) and isinstance(node.func.value, ast.Name):
            if node.func.value.id == "logging" and node.func.attr in {"basicConfig", "getLogger"}:
                # basicConfig with handlers/level in non-entry modules is risky
                if node.func.attr == "basicConfig":
                    self.report.logging_overrides.append(Finding(
                        severity="MED",
                        code="LOG-OVR-001",
                        file=str(self.path),
                        line=node.lineno,
                        message="logging.basicConfig found; may override centralized logging.",
                        hint="Remove and rely on centralized logger (e.g., core.logging.setup).",
                    ))
        self.generic_visit(node)

    def visit_Module(self, node: ast.Module) -> None:
        self.generic_visit(node)
        # Record imports summary
        if self.uses_central:
            self.report.import_central_client.append(str(self.path))
        if self.uses_raw_redis:
            self.report.import_raw_redis.append(str(self.path))

def scan_python(path: Path, report: AuditReport) -> None:
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except Exception:
        return
    # quick regex for prefixes
    for m in PY_PATTERNS["key_prefix"].finditer(text):
        pfx = m.group("prefix").lower()
        report.key_prefixes[pfx] = report.key_prefixes.get(pfx, 0) + 1
    try:
        tree = ast.parse(text)
    except SyntaxError as e:
        report.findings.append(Finding(
            severity="LOW",
            code="PARSE-ERR",
            file=str(path),
            line=getattr(e, "lineno", 1) or 1,
            message=f"Python parse error: {e}",
            hint="Fix syntax so auditor can analyze.",
        ))
        return
    try:
        PyVisitor(path, report).visit(tree)
    except Exception as e:
        # Handle AST traversal errors gracefully
        report.findings.append(Finding(
            severity="LOW",
            code="AST-ERR",
            file=str(path),
            line=1,
            message=f"AST traversal error: {e}",
            hint="File may contain complex AST structures that couldn't be analyzed.",
        ))

def scan_yaml(path: Path, report: AuditReport) -> None:
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except Exception:
        return
    for i, line in enumerate(text.splitlines(), start=1):
        low = line.lower()
        if any(k in low for k in YAML_HINT_KEYS):
            if "redis" in low or "db" in low or "host" in low or "prefix" in low:
                report.yaml_hints.append((str(path), i, line.strip()))

def is_entrypoint(path: Path) -> bool:
    # crude: treat top-level scripts and *_main.py as entrypoints
    p = str(path)
    return p.endswith("_main.py") or p.count(os.sep) <= 1

def build_findings(report: AuditReport) -> None:
    # Raw redis constructors → HIGH unless inside central client module
    for ctor in report.redis_ctors:
        if any(mod in ctor.file for mod in REDIS_CLIENT_MODULE):
            continue
        report.findings.append(Finding(
            severity="HIGH",
            code="RD-BYPASS-001",
            file=ctor.file,
            line=ctor.line,
            message=f"Direct {ctor.func} constructor bypasses centralized client.",
            hint="Replace with: from redis_files.redis_client import get_redis_client; use get_redis_client(db=?, decode_responses=?)",
        ))
        # Protocol check
        prot = ctor.kwargs.get("protocol", None)
        if prot not in (3, None):
            report.findings.append(Finding(
                severity="MED",
                code="RD-PROTO-002",
                file=ctor.file,
                line=ctor.line,
                message=f"Using RESP{prot} on Redis 8.x; recommend RESP3 (protocol=3).",
                hint="Pass protocol=3 or centralize via get_redis_client().",
            ))

    # DB drift by file
    db_map: Dict[int, List[str]] = {}
    for f, dbs in report.drift_db.items():
        for d in dbs:
            if isinstance(d, int):
                db_map.setdefault(d, []).append(f)
            else:
                report.findings.append(Finding(
                    severity="LOW",
                    code="RD-DB-DYN",
                    file=f,
                    line=1,
                    message="Dynamic db parameter; verify consistency with IND_DB/REDIS_DB_*.",
                    hint="Prefer env-driven db numbers via centralized client.",
                ))
    if len(db_map) > 1:
        # Multiple DBs across files is fine; flag likely mismatches with indicators DB=4/5
        pass

    # Prefix drift
    pfx = {k.rstrip(":") for k in report.key_prefixes}
    if len(pfx) > 1 and ("indicator" in pfx or "indicators" in pfx):
        report.findings.append(Finding(
            severity="MED",
            code="RD-PFX-001",
            file="(multiple)",
            line=0,
            message=f"Mixed key prefixes detected: {sorted(pfx)}; dashboard may filter by a single prefix.",
            hint="Standardize via indicators_store._key(symbol, timeframe).",
        ))

    # Decode misses that look like readers (heuristic: has get/hgetall)
    usage_by_file = {}
    for u in report.redis_usages:
        usage_by_file.setdefault(u.file, set()).add(u.api)
    for miss in list(report.decode_misses):
        apis = usage_by_file.get(miss.file, set())
        if not ({"get", "hgetall", "scan"} & apis):
            # If it's not a reader, downgrade noise
            miss.severity = "LOW"

    # Logging overrides outside entrypoints → MED
    for f in list(report.logging_overrides):
        if is_entrypoint(Path(f.file)):
            # Allow in entrypoints, but warn softly
            f.severity = "LOW"

def render(report: AuditReport, as_json: Optional[Path]) -> int:
    build_findings(report)
    # Text report
    print("=== Repo Audit: Redis & Logging ===")
    print(f"- Redis constructors found: {len(report.redis_ctors)}")
    print(f"- Redis operations found: {len(report.redis_usages)}")
    if report.key_prefixes:
        print(f"- Key prefixes seen: {', '.join(f'{k}({v})' for k, v in sorted(report.key_prefixes.items()))}")
    if report.yaml_hints:
        print(f"- YAML hints: {len(report.yaml_hints)} lines referencing redis/db/host/prefix")

    print("\n--- High/Med Findings ---")
    highmed = [f for f in report.findings if f.severity in {"HIGH", "MED"}]
    if not highmed:
        print("No HIGH/MED issues detected.")
    for f in highmed:
        print(f"[{f.severity}] {f.code} {f.file}:{f.line} :: {f.message}\n  ↳ {f.hint}")

    print("\n--- Low Findings (FYI) ---")
    for f in report.findings:
        if f.severity == "LOW":
            print(f"[LOW] {f.code} {f.file}:{f.line} :: {f.message}\n  ↳ {f.hint}")

    # Culprits table (direct Redis)
    if report.redis_ctors:
        print("\n--- Culprits (Direct redis.Redis/StrictRedis) ---")
        for c in report.redis_ctors:
            print(f"- {c.file}:{c.line} -> {c.func} {c.kwargs}")

    # YAML hint lines
    if report.yaml_hints:
        print("\n--- YAML Lines with Redis-like Config ---")
        for f, ln, line in report.yaml_hints:
            print(f"- {f}:{ln} | {line}")

    # JSON export for CI
    if as_json:
        payload = {
            "findings": [f.__dict__ for f in report.findings],
            "redis_ctors": [c.__dict__ for c in report.redis_ctors],
            "redis_usages": [u.__dict__ for u in report.redis_usages],
            "key_prefixes": report.key_prefixes,
            "yaml_hints": report.yaml_hints,
        }
        as_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        print(f"\nWrote JSON report -> {as_json}")
    # Exit code policy
    if any(f.severity == "HIGH" for f in report.findings):
        return 2
    if any(f.severity == "MED" for f in report.findings):
        return 1
    return 0

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", type=Path, default=Path("."), help="Repo root")
    ap.add_argument("--json-out", type=Path, help="Write JSON report")
    args = ap.parse_args()

    report = AuditReport()
    for path in args.root.rglob("*"):
        if path.is_dir():
            continue
        suf = path.suffix.lower()
        if suf == ".py":
            scan_python(path, report)
        elif suf in {".yaml", ".yml"}:
            scan_yaml(path, report)

    return render(report, args.json_out)

if __name__ == "__main__":
    raise SystemExit(main())
