"""
L3 Fixer — BondAnomaly (RIA outreach SaaS)

Receives HIGH severity escalations from l2_bondanomaly. Uses Claude
(tool-use loop) to read outreach/prospect files, diagnose root cause,
apply a fix if confident (>= 0.85), and append to the shared incident ledger.

Files in scope: ria_outreach.py, ria_followup.py, prospect_hunter.py,
                stripe_setup.py, outreach_log.csv, prospects.csv, crontab.
Sensitive: .env is read-blocked.
"""

import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "ouroboros.v2"))

try:
    import anthropic
    _ANTHROPIC_OK = True
except ImportError:
    _ANTHROPIC_OK = False

from supervisor.escalation_queue import resolve

try:
    from src.notifications.telegram import TelegramNotifier
    _notifier = TelegramNotifier()
except Exception:
    class _FallbackNotifier:
        def send(self, msg): print(f"[Telegram fallback] {msg}")
    _notifier = _FallbackNotifier()

MODEL                = "claude-opus-4-7"
AUTO_APPLY_THRESHOLD = 0.85
PROJECT_ROOT         = str(Path(__file__).parent.parent)
INCIDENT_LEDGER      = os.path.expanduser(
    "~/.claude/projects/-home-u-ack-it/memory/incident_ledger.md"
)

BONDANOMALY_FILES = {
    "ria_outreach.py", "ria_followup.py", "prospect_hunter.py",
    "stripe_setup.py", "outreach_log.csv", "prospects.csv",
    "run_prospect_hunter.sh", "AGENTS.md",
}


# ---------------------------------------------------------------------------
# Tool implementations
# ---------------------------------------------------------------------------

def _tool_read_file(path: str) -> str:
    if path.endswith(".env"):
        return "BLOCKED: .env is a sensitive file — read not permitted via L3"
    full = os.path.join(PROJECT_ROOT, path)
    if not os.path.exists(full):
        return f"ERROR: {path} not found"
    try:
        content = open(full).read()
        if len(content) > 8000:
            content = content[:8000] + f"\n... [truncated — {len(content)} chars total]"
        return content
    except Exception as exc:
        return f"ERROR reading {path}: {exc}"


def _tool_search_code(pattern: str, path: str = ".") -> str:
    full_path = os.path.join(PROJECT_ROOT, path)
    try:
        result = subprocess.run(
            ["grep", "-rn", "--include=*.py", "--include=*.sh",
             "--include=*.csv", pattern, full_path],
            capture_output=True, text=True, timeout=10
        )
        out = result.stdout.strip()
        if not out:
            return f"No matches for '{pattern}' in {path}"
        lines = out.splitlines()[:40]
        return "\n".join(l.replace(PROJECT_ROOT + "/", "") for l in lines)
    except Exception as exc:
        return f"ERROR: {exc}"


def _tool_apply_fix(file_path: str, old_code: str, new_code: str,
                    reason: str, confidence: float = 0.0) -> str:
    if file_path.endswith(".env"):
        return "BLOCKED: cannot modify .env via auto-fix"
    if confidence < AUTO_APPLY_THRESHOLD:
        return (
            f"SKIPPED (confidence {confidence:.2f} < {AUTO_APPLY_THRESHOLD}).\n"
            f"Proposed fix for {file_path}:\n--- OLD ---\n{old_code}\n--- NEW ---\n{new_code}\n"
            f"Reason: {reason}"
        )
    full = os.path.join(PROJECT_ROOT, file_path)
    if not os.path.exists(full):
        return f"ERROR: {file_path} not found"
    try:
        content = open(full).read()
        if old_code not in content:
            return f"ERROR: old_code not found in {file_path}"
        if content.count(old_code) > 1:
            return f"ERROR: old_code matches {content.count(old_code)} locations — ambiguous"
        open(full, "w").write(content.replace(old_code, new_code, 1))
        _notifier.send(
            f"🔧 <b>L3 BondAnomaly — patch applied</b>\n"
            f"<code>{file_path}</code>\n{reason}\nConfidence: {confidence:.0%}"
        )
        return f"OK: fix applied to {file_path}"
    except Exception as exc:
        return f"ERROR: {exc}"


def _tool_run_bash(command: str) -> str:
    allowed = ("grep", "tail", "head", "cat", "ps", "pgrep", "wc",
               "ls", "find", "crontab", "wc")
    if not any(command.strip().startswith(p) for p in allowed):
        return "BLOCKED: only read-only commands permitted"
    try:
        result = subprocess.run(
            command, shell=True, capture_output=True, text=True,
            timeout=10, cwd=PROJECT_ROOT
        )
        return (result.stdout + result.stderr).strip()[:3000] or "(no output)"
    except Exception as exc:
        return f"ERROR: {exc}"


def _tool_write_incident(inc_id: str, title: str, symptom: str,
                         root_cause: str, fix_applied: str,
                         files_changed: list, interview_tip: str) -> str:
    entry = f"""
## {inc_id} | BondAnomaly | {datetime.now().strftime('%Y-%m-%d')}
**Title**: {title}

### 1. Initial Observation
{symptom}

### 2. Root Cause (L3 Fixer diagnosis)
{root_cause}

### 3. Fix
{fix_applied}
Files changed: {', '.join(files_changed) if files_changed else 'none'}

### 4. Interview Talking Point
*"{interview_tip}"*

---
"""
    try:
        open(INCIDENT_LEDGER, "a").write(entry)
        return f"OK: {inc_id} appended to ledger"
    except Exception as exc:
        return f"ERROR: {exc}"


# ---------------------------------------------------------------------------
# Tool schemas + dispatch
# ---------------------------------------------------------------------------

TOOL_SCHEMAS = [
    {
        "name": "read_file",
        "description": "Read a file from the BondAnomaly/muni_scanner project (relative path). .env is blocked.",
        "input_schema": {"type": "object", "properties": {"path": {"type": "string"}}, "required": ["path"]},
    },
    {
        "name": "search_code",
        "description": "Grep across BondAnomaly Python/shell/CSV files.",
        "input_schema": {
            "type": "object",
            "properties": {
                "pattern": {"type": "string"},
                "path":    {"type": "string", "description": "Directory (default: .)"},
            },
            "required": ["pattern"],
        },
    },
    {
        "name": "apply_fix",
        "description": "Replace old_code with new_code in a file. Auto-applies if confidence >= 0.85. .env is blocked.",
        "input_schema": {
            "type": "object",
            "properties": {
                "file_path":  {"type": "string"},
                "old_code":   {"type": "string"},
                "new_code":   {"type": "string"},
                "reason":     {"type": "string"},
                "confidence": {"type": "number"},
            },
            "required": ["file_path", "old_code", "new_code", "reason", "confidence"],
        },
    },
    {
        "name": "run_bash",
        "description": "Read-only shell commands (grep, tail, cat, ps, pgrep, crontab, ls, find, wc, head).",
        "input_schema": {"type": "object", "properties": {"command": {"type": "string"}}, "required": ["command"]},
    },
    {
        "name": "write_incident",
        "description": "Append a structured incident to the shared incident ledger.",
        "input_schema": {
            "type": "object",
            "properties": {
                "inc_id":        {"type": "string"},
                "title":         {"type": "string"},
                "symptom":       {"type": "string"},
                "root_cause":    {"type": "string"},
                "fix_applied":   {"type": "string"},
                "files_changed": {"type": "array", "items": {"type": "string"}},
                "interview_tip": {"type": "string"},
            },
            "required": ["inc_id", "title", "symptom", "root_cause", "fix_applied", "interview_tip"],
        },
    },
]


def _dispatch(name: str, inputs: dict) -> str:
    if name == "read_file":   return _tool_read_file(inputs["path"])
    if name == "search_code": return _tool_search_code(inputs["pattern"], inputs.get("path", "."))
    if name == "apply_fix":
        return _tool_apply_fix(inputs["file_path"], inputs["old_code"], inputs["new_code"],
                               inputs["reason"], inputs.get("confidence", 0.0))
    if name == "run_bash":    return _tool_run_bash(inputs["command"])
    if name == "write_incident":
        return _tool_write_incident(
            inputs["inc_id"], inputs["title"], inputs["symptom"],
            inputs["root_cause"], inputs["fix_applied"],
            inputs.get("files_changed", []), inputs["interview_tip"],
        )
    return f"ERROR: unknown tool '{name}'"


SYSTEM_PROMPT = """You are the L3 Fixer agent for BondAnomaly, a $299/month muni bond alert
SaaS that auto-prospects RIA firms and follows up via email.

Your job:
1. Receive an L2 escalation describing an outreach/prospecting anomaly.
2. Use your tools to read ria_outreach.py, ria_followup.py, prospect_hunter.py,
   outreach_log.csv, prospects.csv, and any cron entries relevant to the issue.
3. Produce a precise code fix (old_code → new_code, exact match).
4. Call apply_fix with confidence >= 0.85 only when certain. .env is blocked.
5. Call write_incident to record the full diagnosis in the incident ledger.

Rules: Read before diagnosing. Be precise and terse. Do not modify .env.
CSV fixes require extra care — validate field counts before proposing.
"""


def run_l3(escalation: dict) -> dict:
    if not _ANTHROPIC_OK:
        return {"fixed": False, "message": "anthropic not installed", "esc_id": escalation["id"]}
    api_key = os.getenv("ANTHROPIC_API_KEY", "").strip()
    if not api_key:
        return {"fixed": False, "message": "ANTHROPIC_API_KEY not set", "esc_id": escalation["id"]}

    client   = anthropic.Anthropic(api_key=api_key)
    user_msg = (
        f"Escalation from L2 Supervisor (BondAnomaly):\n\n"
        f"ID:         {escalation['id']}\n"
        f"Type:       {escalation['type']}\n"
        f"Severity:   {escalation['severity']}\n"
        f"Detail:     {escalation['detail']}\n"
        f"Hypothesis: {escalation['hypothesis']}\n"
        f"Files:      {', '.join(escalation.get('files_to_check', []))}\n\n"
        f"Diagnose, fix if confident (>= 0.85), write incident to ledger."
    )
    messages      = [{"role": "user", "content": user_msg}]
    applied_fixes = []

    _notifier.send(
        f"🤖 <b>L3 BondAnomaly Fixer</b>\n"
        f"<code>{escalation['id']}</code> — {escalation['type']} ({escalation['severity']})"
    )

    for _ in range(12):
        response = client.messages.create(
            model=MODEL, max_tokens=4096, system=SYSTEM_PROMPT,
            tools=TOOL_SCHEMAS, messages=messages,
        )
        messages.append({"role": "assistant", "content": response.content})
        if response.stop_reason in ("end_turn", None) or response.stop_reason != "tool_use":
            break

        tool_results = []
        for block in response.content:
            if block.type != "tool_use":
                continue
            output = _dispatch(block.name, block.input)
            tool_results.append({"type": "tool_result", "tool_use_id": block.id, "content": output})
            if block.name == "apply_fix" and block.input.get("confidence", 0) >= AUTO_APPLY_THRESHOLD and "OK:" in output:
                applied_fixes.append(block.input["file_path"])
        messages.append({"role": "user", "content": tool_results})

    final_text = "".join(getattr(b, "text", "") for b in getattr(response, "content", []))
    fixed = bool(applied_fixes)
    resolve(escalation["id"], final_text[:500] or "L3 complete")

    _notifier.send(
        f"{'✅' if fixed else '📋'} <b>L3 BondAnomaly complete</b>\n"
        f"<code>{escalation['id']}</code>\n"
        + (f"Patched: {', '.join(applied_fixes)}" if fixed else "No auto-fix — see proposal above.")
    )
    return {"fixed": fixed, "files_patched": applied_fixes,
            "message": final_text[:500], "esc_id": escalation["id"]}
