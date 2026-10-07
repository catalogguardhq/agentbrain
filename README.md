# AgentBrain

AgentBrain is a shared, source-attributed memory system for agent conversations. It records native conversation events, indexes searchable history, tracks thread-scoped task state, and injects relevant shared or private memory into later turns.

AgentBrain does not verify answers, decide whether work may proceed, deny tools, or block a response. Its hook is memory-only and fails open. Verification and other policy systems can run as separate hooks.

## What it stores

- Raw lifecycle events with source IDs and best-effort secret redaction.
- Searchable project and thread history.
- Shared project facts and user preferences with source references.
- Per-thread task state, recovery checkpoints, and explicit delegations.
- Automatic summaries and relevant memory retrieval, marked as interpretations.

Memory is advisory. Current human instructions govern. Another thread's objective or next actions are never adopted automatically. Do not store credentials or hidden chain-of-thought.

## Requirements

- Python 3.10 or newer (standard library only).
- Codex CLI available as `codex` on `PATH` for automatic semantic summaries, or set `CODEX_CLI` to its executable path. Native event capture and indexing continue when the CLI is unavailable.

## Install

Clone this repository and point your Codex command hooks at `agentbrain/codex_hook.py`. For each lifecycle event you want captured, configure a command like:

```json
{
  "type": "command",
  "command": "python3 /absolute/path/to/agentbrain/agentbrain/codex_hook.py",
  "timeout": 3,
  "statusMessage": "AgentBrain shared memory (tools unrestricted)"
}
```

Use the command for `SessionStart`, `UserPromptSubmit`, `PreToolUse`, `PostToolUse`, `PreCompact`, `SubagentStart`, `SubagentStop`, `Stop`, `Interrupt`, and `SessionEnd`. AgentBrain only adds context at `SessionStart`, `UserPromptSubmit`, and `SubagentStart`; other callbacks are captured for memory. Do not configure AgentBrain as an authorization or final-answer gate.

Set `AGENT_BRAIN_HOME` to choose the memory vault location. It defaults to `~/AgentBrain`. The local `.agent-brain.json` project link and runtime state are excluded from project Git via `.git/info/exclude` by the memory core.

## Data and privacy

The memory vault is local to the configured home directory. Event and semantic memory files are written with restrictive permissions where supported. Redaction is best-effort; review the data handling requirements of your environment before enabling capture. You can disable semantic summaries by leaving the Codex CLI unavailable; event storage and indexing remain available.

## Layout

- `agentbrain/continuity.py` — event store, project/thread identity, search, task state, and memory provenance.
- `agentbrain/memory_auto.py` — automatic indexing, summaries, checkpoints, and retrieval.
- `agentbrain/observer.py` — nonblocking, memory-only Codex lifecycle hook.
- `agentbrain/CHECKPOINT-SCHEMA.json` and `agentbrain/MEMORY-OS.md` — memory format and operating rules.
