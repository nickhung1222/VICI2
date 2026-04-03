# AGENTS.md

## Purpose

This file is the repo-level operating guide for coding agents working in this project.

## Git Tracking Policy

When a change qualifies as a major product or workflow update, the agent should explicitly ask whether the user wants the change committed and pushed to GitHub for tracking.

Treat a change as major when any of the following is true:

- A new CLI mode, CLI argument, or top-level workflow is added or changed.
- A new tool/module/schema is introduced, or an existing one changes behavior materially.
- Report structure, prompt behavior, output contract, or event-analysis logic changes in a user-visible way.
- Multiple core files are changed together as one feature slice.
- New tests are added to cover a new capability rather than a small bug fix only.

Usually do not treat the change as major when it is only:

- Small wording or comment cleanup.
- Pure refactor with no behavior change.
- Narrow bug fix with no interface or workflow impact.
- Test-only maintenance with no product change.

## Required Agent Workflow

After finishing a major change, the agent should:

1. Summarize the functional change in 1 to 3 short bullets.
2. Ask whether the user wants the current batch committed and pushed to GitHub.
3. If the user agrees, create a clear English commit message and push the current branch.
4. If there is no suitable branch yet, ask whether to create one or use the current branch.

Do not push automatically without user confirmation when:

- The workspace contains unrelated dirty changes.
- The branch strategy is unclear.
- Secrets, generated outputs, or large intermediate files might be included.

## Commit Message Preference

Use concise English commit messages with a feature-oriented prefix when possible, for example:

- `feat: add event report workflow`
- `feat: add heat scan comparison logic`
- `refactor: normalize event record schema`
- `fix: handle missing article content in scraper`

## Documentation Sync

For major changes, the agent should also check whether `README.md` and `CLAUDE.md` need updates.

`AGENTS.md` is the source of truth for agent collaboration rules in this repository.
