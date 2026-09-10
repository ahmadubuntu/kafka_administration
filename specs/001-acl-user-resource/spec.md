# Feature Specification: ACL USER resource support

**Feature Branch**: `chore/speckit-and-acl-user`

**Created**: 2026-09-10

**Status**: Approved

**Input**: Dry-run `sync_acls.sh` crashed with
`ValueError: unsupported ACL resource type USER` while building dest-only
remove argv. Graphify and Spec Kit must be initialized in every repo first.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Dry-run survives USER ACLs (Priority: P1)

An operator compares prod vs DR ACLs. Dest has extra `resourceType=USER`
entries (delegation-token / user-principal ACLs). Dry-run must print the
diff and exit 0 instead of a traceback.

**Why this priority**: Current command is unusable on the live MM host.

**Independent Test**: Feed sample `kafka-acls --list` text with a USER
resource through `acl-diff`; process exits 0 and includes USER in add/extra.

**Acceptance Scenarios**:

1. **Given** dest-only USER ACL rows, **When** `acl-diff` runs, **Then** it
   does not raise and emits `remove_argv` using `--user-principal`.
2. **Given** source-only USER ACL rows, **When** `acl-diff` runs, **Then**
   `add_argv` includes `--user-principal` and the resource name.

---

### User Story 2 - Unknown types are reported, not fatal (Priority: P2)

A future Kafka resource type must not crash dry-run.

**Why this priority**: Same class of CLI drift as `--json` / USER.

**Independent Test**: Diff containing `rtype=UNKNOWNTYPE` returns skipped
rows and still prints supported add/extra argv.

**Acceptance Scenarios**:

1. **Given** an unsupported resource type in extra ACLs, **When** `acl-diff`
   runs, **Then** exit 0 and that row is listed under skipped, not argv.

### Edge Cases

- USER resource name may be `User:name` or `*`.
- `--prune` must be able to remove dest-only USER ACLs with the same flag.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: `acl_cli_args` MUST map `USER` to `--user-principal <name>`.
- **FR-002**: `acl-diff` MUST NOT raise on USER or unknown resource types.
- **FR-003**: Unknown types MUST appear in a `skipped` list with a reason.
- **FR-004**: `sync_acls.sh` dry-run MUST print skipped rows and continue.
- **FR-005**: `--apply` MUST use `--user-principal` for USER add/remove.

### Key Entities

- **ACL row**: rtype, name, pattern, principal, host, operation, perm.
- **USER resource**: Kafka ResourceType.USER; CLI `--user-principal`.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Unit tests cover USER add/remove argv and unknown-type skip.
- **SC-002**: The reported traceback path no longer raises on USER extras.
- **SC-003**: Live dry-run on the MM host completes a report (operator).

## Assumptions

- Kafka 3.x `kafka-acls.sh` accepts `--user-principal` for USER resources.
- Spec Kit integration id in this environment is `cursor-agent`.
- Graphify graph already exists after `graphify update .`.
