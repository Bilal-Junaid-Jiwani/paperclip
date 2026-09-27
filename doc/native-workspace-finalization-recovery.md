# Native workspace finalization ownership and recovery

Native workspace export and merge acquire a PostgreSQL advisory lock scoped to
company and run before the first physical copyback. The live heartbeat and the
reconciler share that lock. Recovery skips a busy owner without recording another
workspace operation or spending a retry. A completed workspace barrier is reread
under ownership before export, and a committed coordinator cannot be overwritten
by a late failure receipt.

The lock transaction holds no row locks. Ordinary progress and finalization
receipts remain visible through the normal database pool. A dedicated connection outside the application pool is
reserved for the duration of copyback and closed when it settles; even a one-connection application pool remains available for progress writes. Its run profile carries
`nativeWorkspaceFinalizationOwner`, an exact token, host, PID, and process-start
receipt. Losing the lock connection does not prove physical copyback stopped:
a contender still refuses a receipt whose controller is alive. The original
callback joins before its token is released, and publication checks the lock
connection and token. Graceful completion clears the receipt. A controller that
has exited on the same host can be recovered; PID reuse is checked against its
recorded start time rather than trusted by PID alone.

## Controller replacement on a different host

The controller cannot verify a process on a foreign or unknown host. It surfaces
`native_workspace_finalization_owner_unverified` as board-owned recovery, with no
automatic provider wake. This is an intentional limit: elapsed time or a missing
database connection never proves the old copyback process stopped.

An instance operator must first verify through the deployment platform that the
exact prior controller **and its copyback subprocesses** have stopped. Retain the
sandbox, accepted native result, and workspace descriptor. Do not run a new
provider turn, delete workspace contents, or relax archive confinement.

After that platform verification, use a database maintenance transaction to
release only the exact receipt shown by the recovery action. Replace the four
placeholders with the action's company, run, token, and source issue. The advisory
lock prevents concurrent acquisition during this change; the token comparison
prevents clearing a newer owner. A zero-row update means ownership changed and
requires fresh inspection. This maintenance operation is for a full-control
instance operator, not an agent tool.

```sql
BEGIN;
SELECT pg_advisory_xact_lock(hashtextextended(
  'native-workspace-finalization:<company-id>:<run-id>', 0));
UPDATE heartbeat_runs
SET runner_profile_json = runner_profile_json - 'nativeWorkspaceFinalizationOwner'
WHERE company_id = '<company-id>'::uuid
  AND id = '<run-id>'::uuid
  AND native_issue_id = '<issue-id>'::uuid
  AND runtime_mode = 'native'
  AND runner_profile_json->'nativeWorkspaceFinalizationOwner'->>'token' = '<owner-token>'
RETURNING id;
COMMIT;
```

Resolve the existing board recovery action with a note containing the platform
stop evidence. The ordinary reconciliation sweep then resumes workspace
finalization from the accepted result. Confirm the run's native phase and
`resultJson.finalizationPhase` are `committed`, there is no `nextAttemptAt`, and
no workspace operation is still running. The accepted provider result is reused.
