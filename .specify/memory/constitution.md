<!--
Sync Impact Report
- Version change: template (unratified) -> 1.0.0
- Modified principles:
  - Placeholder Principle 1 -> I. Inventory-Driven Operations
  - Placeholder Principle 2 -> II. Dry-Run Default and Safe Mutation
  - Placeholder Principle 3 -> III. Secrets and Dual-Remote Hygiene
  - Placeholder Principle 4 -> IV. Spec-First Delivery
  - Placeholder Principle 5 -> V. Verification Before Done
- Added sections:
  - Repository Constraints
  - Spec Kit Delivery Workflow
  - Governance
- Removed sections: none
- Follow-up TODOs: none
-->
# kafka_administration Constitution

## Core Principles

### I. Inventory-Driven Operations

Cluster hosts, bootstrap lists, CLI paths, and thresholds MUST live in
`config/clusters/*.env`. Committed files MUST be `*.example.env` only.
Scripts MUST NOT hardcode production IPs, broker lists, or passwords.
Identity topic names match MirrorMaker 2 `IdentityReplicationPolicy`.

Rationale: The same toolkit runs against prod, DR, and lab clusters.
Hardcoded endpoints produce confident lies.

### II. Dry-Run Default and Safe Mutation

Compare and sync scripts MUST print only unless `--apply` and `-y` are both
set. `sync_topic_configs.sh` MUST never create missing topics and MUST only
alter configs on topics that exist on both sides. `sync_acls.sh --prune` is
opt-in dest-only ACL delete. Scripts MUST NOT change RF or partition count
unless that is the named purpose of the script.

Rationale: Operators run these on live prod/DR. Default must be report-only.

### III. Secrets and Dual-Remote Hygiene

Passwords, PATs, and `KAFKA_COMMAND_CONFIG` contents MUST NOT be committed
or printed. Product commits dual-push to GitHub `origin` and Azure `azure`.
`plans/` is Azure-only (`azure/internal/plans`). Auth for Azure is
`AZDO_PAT_DATAPLATFORM` only. Feature work uses a short-lived branch and a
PR into `main`; this repo merges the PR and deletes the feature branch.

Rationale: Dual remotes and Azure-only plans are standing operator policy.

### IV. Spec-First Delivery

If `.specify/` or `graphify-out/graph.json` is missing, initializing them is
the first action. Features and public behavior changes MUST have a spec under
`specs/` and follow constitution → specify → plan → tasks → implement when
the change is more than a restore-documented-behavior bugfix. Active feature
is `.specify/feature.json`, not the git branch name alone.

Rationale: Chat-only plans are lost; another agent must be able to continue.

### V. Verification Before Done

Parser and CLI-argv changes MUST have unit tests. After code edits, run
`graphify update .`. A change is not done until tests that cover the
restored or new contract pass. Live cluster mutation waits for explicit
`--apply -y` by the operator.

Rationale: ACL/config parsers fail on Kafka CLI drift; tests catch the next
unsupported resource type before a dry-run traceback.

## Repository Constraints

- Toolkits live under `kafka_realtime_check/`, `kafka_topic_admin/`, and
  `kafka_mirrormaker/`. Do not replace bash entrypoints with Python files of
  the same name (`compare_storage.sh` stays bash).
- Kafka 3.9 `kafka-log-dirs.sh` has no `--json`; parse default stdout.
- Dest `compression.type` only where source is already compressed. Do not set
  MM2 `producer.compression.type` globally.
- Do not treat `__consumer_offsets` HWM as mirror lag.

## Spec Kit Delivery Workflow

1. Init graphify and Spec Kit if missing, then read this constitution and
   the latest `plans/` file.
2. `$speckit-specify` (reduced spec allowed for bug restores).
3. `$speckit-plan` / `$speckit-tasks` when the change is not a one-file
   restore of documented behavior.
4. `$speckit-implement`, then unit tests and `graphify update .`.
5. Product PR without `plans/`; plans-only commit to `azure/internal/plans`.

## Governance

This constitution supersedes conflicting chat guidance. Amendments need a
written rationale, semantic version bump (MAJOR incompatible, MINOR new
principle, PATCH wording), and an updated Sync Impact Report.

**Version**: 1.0.0 | **Ratified**: 2026-09-10 | **Last Amended**: 2026-09-10
