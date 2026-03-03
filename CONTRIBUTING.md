# Contributing Guide

## Development Workflow

All contributors (human and AI) must follow this workflow.

## Pre-commit Checklist

Before committing any changes to `taienergy-analytics/`:

1. **Syntax Check**
   ```bash
   python3 -m py_compile taienergy-analytics/workflows/daily_v5.py taienergy-analytics/core/memory_system.py
   ```

2. **Run QA Gate**
   ```bash
   bash scripts/qa_gate.sh
   ```

3. **Commit Requirements**
   - All tests must pass
   - Include test summary in commit message or PR description
   - List all changed files

## Commit Message Format

```
<type>(<scope>): <short summary>

- changed files: <list>
- tests run: <commands>
- result: pass/fail

<longer description if needed>
```

## Testing Policy

- **No test, no commit.** This applies to all code changes.
- If `qa_gate.sh` fails, fix the failures before proceeding.
- Never bypass the QA gate.

## Code Review

All changes must be reviewed before merging to `main`.
