# STAGE-02 result

Implemented intake format recovery on `codex/stage-02-wordcounter`.

## Changes and rationale

- Added the complete nine-slot JSON skeleton to the intake charter, including explicit citation and gap alternatives. This addresses missing wrappers and scalar slot values seen in run 05.
- Added pre-checkpoint format validation of every slot. Malformed JSON, missing/extra lifecycle keys, scalar values, unknown statuses, empty fields, and attempts to append a partial JSON document receive deterministic feedback while retaining the last successful source observation.
- Accept an omitted `slots` wrapper only when the root has exactly the nine lifecycle keys. Values are preserved verbatim; no quote, explanation, or requirement is synthesized.
- Permit two corrections after malformed intake submissions; the third malformed submission escalates. Existing action repetition, token, call, and time limits still apply.
- Added regression coverage for normalization, malformed documents, bounded retries without checkpointing invalid artifacts, and unchanged rejection of invented quotations and unresolved gaps. Existing recovery coverage verifies successful correction with source context retained.

The worktree already contained lease and command-receipt changes when this task began. They are preserved in this commit: longer default lease, readable command captures, and attempt-owned command receipts alongside declared aliases. Every quote still goes through the existing verifier and every checkpoint still undergoes digest verification. `verifier.py`, plan validation, the gate, the example plan/spec, and grader were not edited.

## Validation and execution

- `python3 -m unittest tests.test_runtime`: 14 passed.
- `python3 -m unittest tests.test_runtime tests.test_harness tests.test_adversarial`: 59 passed, 2 errors solely because sandboxed loopback socket bind is forbidden (`PermissionError: [Errno 1] Operation not permitted`).
- The seven `tests.test_model` tests likewise cannot initialize their local HTTP servers in this sandbox.
- `git diff --check`: passed.
- Attempted `PYTHONPATH=. python3 examples/run_local.py --run-dir runs/wordcounter-stage-02` with the default endpoint/model. The first model call could not execute here. A direct request to the same local endpoint confirmed `ModelError: local model request failed: <urlopen error [Errno 1] Operation not permitted>`.

Live execution is stopped under the task's loopback restriction instruction. Coordinator execution outside this sandbox remains required; AWAITING_COORDINATOR is not claimed.
