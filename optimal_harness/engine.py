"""Fresh-context worker runs, evidence checkpoints, stage machine and gates."""

from __future__ import annotations

import asyncio
import hashlib
import importlib.resources
import json
from pathlib import Path
import time
import uuid
import copy
import shutil
from typing import Any

from .model import Completion, ModelError, ModelQueue, parse_action, SYSTEM_PREFIX
from .ledger import Ledger, DEFAULT_BUDGET
from .policy import FileBroker, PolicyError, atomic_write, confined, normalized, sandbox_command
from .store import LeaseLost, Store, canonical
from .verifier import Verifier, digest, validate_plan


STAGE_NEXT = {
    "INTAKE": "CODER", "CODER": "INTEGRATOR", "INTEGRATOR": "QA",
    "QA": "__GATE__", "FIXER": "INTEGRATOR",
}


def role_text(role: str) -> str:
    return importlib.resources.files("optimal_harness").joinpath("charters", role.lower() + ".md").read_text()


def make_brief(role: str, plan: dict, notes: str = "", source_stage: str = "") -> dict:
    conf = plan["stages"][role]
    reads = conf.get("reads", [])
    if role == "REVIEW" and source_stage in plan["stages"]:
        prior = plan["stages"][source_stage]
        relevant = set(prior.get("reads", [])) | set(prior.get("deliverables", []))
        reads = [p for p in reads if p in relevant]
    return {
        "role": role, "goal": plan["goal"], "spec_pointer": plan["spec"],
        "permitted_reads": reads,
        "permitted_writes": conf.get("writes", []),
        "deliverables": conf.get("deliverables", []),
        "commands": sorted(conf.get("commands", {}).keys()),
        "notes_verbatim": notes, "prior_stage": source_stage,
        "budget": conf.get("budget", {}),
    }


class Engine:
    def __init__(self, store: Store, queue: ModelQueue, lease_seconds: int = 1800):
        self.store = store
        self.queue = queue
        self.lease_seconds = lease_seconds

    def submit(self, root: Path, plan: dict, grader_file: Path) -> str:
        root = root.resolve(strict=True)
        grader_file = grader_file.resolve(strict=True)
        if grader_file.is_relative_to(root):
            raise PolicyError("independent grading keys must be outside worker project root")
        plan = copy.deepcopy(plan)
        for conf in plan["stages"].values():
            conf["writes"] = list(dict.fromkeys(conf.get("writes", []) + conf.get("writable", [])))
        validate_plan(plan, root, grader_file)
        project = plan["id"]
        if not isinstance(project, str) or not project.isidentifier():
            raise PolicyError("project ID must be identifier")
        plan = copy.deepcopy(plan)
        for conf in plan["stages"].values():
            conf["writes"] = list(dict.fromkeys(conf.get("writes", []) + conf.get("writable", [])))
            for command in conf.get("commands", {}).values():
                executable = shutil.which(command["argv"][0])
                if not executable:
                    raise PolicyError("command executable unavailable")
                command["executable_sha256"] = digest(Path(executable).resolve())
        plan["private_grader_file"] = str(grader_file)
        input_paths = [plan["spec"], *plan.get("immutable_inputs", [])]
        plan["input_hashes"] = {p: digest(confined(root, p)) for p in input_paths}
        grader_files = [grader_file]
        for check in json.loads(grader_file.read_text())["checks"]:
            if check["type"] == "sandbox_acceptance":
                grader_files.append(confined(grader_file.parent, check["test_file"]))
        plan["grading_hashes"] = {str(p): digest(p) for p in grader_files}
        # Vault contains all role charters as real files, not conversation history.
        for role in plan["stages"]:
            path = confined(root, f"vault/charters/{role.lower()}.md")
            atomic_write(path, role_text(role).encode())
        plan["charter_hashes"] = {f"vault/charters/{r.lower()}.md": digest(confined(root, f"vault/charters/{r.lower()}.md")) for r in plan["stages"]}
        self.store.add_project(project, root, plan, make_brief("INTAKE", plan))
        return project

    async def _heartbeat(self, task: dict) -> None:
        try:
            while True:
                await asyncio.sleep(max(1, self.lease_seconds // 3))
                self.store.heartbeat(task, self.lease_seconds)
        except (asyncio.CancelledError, LeaseLost):
            return

    def _checkpoint(self, root: Path, task: dict, path: str,
                    checkpoints: dict[str, str], source: str, connection=None) -> None:
        full = confined(root, path)
        if not full.is_file():
            raise PolicyError("cannot checkpoint missing artifact")
        if connection is None:
            return self.store.commit(task, lambda c: self._checkpoint(root, task, path, checkpoints, source, c))
        sha = digest(full)
        data = full.read_bytes()
        archive = confined(root, f"vault/checkpoints/{sha}.blob")
        atomic_write(archive, data)
        checkpoints[path] = sha
        archive_rel = str(archive.relative_to(root))
        if connection is None:
            self.store.commit(task, lambda c: self.store.checkpoint(c, task, path, sha, archive_rel))
        else:
            self.store.checkpoint(connection, task, path, sha, archive_rel)

    def _prepare(self, root: Path, task: dict) -> tuple[FileBroker, str, str, str]:
        role = task["stage"]
        brief = task["brief"]
        base = f"vault/runs/{task['id']}/a{task['generation']}"
        charter, brief_path = f"vault/charters/{role.lower()}.md", f"{base}/brief.json"
        evidence, handoff = f"{base}/evidence.jsonl", f"{base}/handoff.json"
        # Preparation is fenced too: a stale runtime may not touch run nodes.
        def prepare(c):
            atomic_write(confined(root, brief_path), (json.dumps(brief, indent=2) + "\n").encode())
            if not confined(root, evidence).exists():
                atomic_write(confined(root, evidence), (canonical({"event": "run_started", "task": task["id"]}) + "\n").encode())
            if not confined(root, handoff).exists():
                atomic_write(confined(root, handoff), (canonical({"status": "in_progress", "open_discrepancies": [], "summary": ""}) + "\n").encode())
        self.store.commit(task, prepare)
        plan = self.store.project(task["project_id"])["plan"]
        conf = plan["stages"][role]
        declared_reads = task["brief"]["permitted_reads"]
        reads = declared_reads + [charter, brief_path, evidence, handoff]
        reads += [cmd["capture"] for cmd in conf.get("commands", {}).values()]
        reads += [f"{base}/commands/{name}.json" for name in conf.get("commands", {})]
        writes = conf.get("writes", []) + [evidence, handoff]
        return FileBroker(root, reads, writes), evidence, handoff, brief_path

    def _context(self, root: Path, task: dict, broker: FileBroker, observation: str) -> str:
        role = task["stage"]
        charter = confined(root, f"vault/charters/{role.lower()}.md").read_text()
        brief = json.dumps(task["brief"], ensure_ascii=False, sort_keys=True)
        return ("ROLE CHARTER (pinned file):\n" + charter + "\nTASK BRIEF (pinned file):\n" + brief +
                "\nALLOWED FILE POINTERS (nothing else may be read):\n" +
                canonical({"reads": sorted(broker.reads), "writes": sorted(broker.writes)}) +
                "\nLAST TOOL OBSERVATION (bounded, not a conversation history):\n" + observation)

    async def run_task(self, task: dict) -> None:
        from .runtime import run_task
        await run_task(self, task)

    def _check_stage(self, root: Path, task: dict, conf: dict,
                     checkpoints: dict[str, str], handoff: str) -> None:
        missing = []
        for path in conf.get("deliverables", []):
            file = confined(root, path)
            if path not in checkpoints or not file.is_file() or file.stat().st_size == 0:
                missing.append(path)
            elif digest(file) != checkpoints[path]:
                missing.append(path + " (modified since checkpoint)")
        if missing:
            raise RuntimeError("error:missing_deliverables:missing_or_uncheckpointed_deliverables:" + ",".join(missing))
        for path in set(conf.get("deliverables", [])) | set(checkpoints):
            if path not in {c["capture"] for c in conf.get("commands", {}).values()}:
                spec = self.store.project(task["project_id"])["plan"]["spec"]
                permitted_sources = task["brief"]["permitted_reads"] + [cmd["capture"] for cmd in conf.get("commands", {}).values()]
                permitted_sources += [f"vault/runs/{task['id']}/a{task['generation']}/commands/{name}.json" for name in conf.get("commands", {})]
                errors = Verifier(root).embedded_quotes(path, permitted_sources,
                                                         spec if task["stage"] == "INTAKE" else None)
                if errors:
                    raise RuntimeError("citation_check_failed:" + ";".join(errors)[:600])
        data = json.loads(confined(root, handoff).read_text())
        if data.get("status") != "ready" or not isinstance(data.get("open_discrepancies"), list):
            raise RuntimeError("handoff_not_ready_or_malformed")
        if data.get('open_discrepancies'):
            self.store.event('handoff_discrepancies', {'task': task['id'], 'count': len(data['open_discrepancies'])})
        if task["stage"] == "INTAKE":
            plan = self.store.project(task["project_id"])["plan"]
            errors = Verifier(root).intake(conf["deliverables"][0], plan["spec"])
            if errors:
                raise RuntimeError("intake_spec_gaps:" + "; ".join(errors)[:600])
        if task["stage"] == "REVIEW":
            report = json.loads(confined(root, conf["deliverables"][0]).read_text())
            if (not isinstance(report, dict) or report.get("reviewed_stage") != task["brief"]["prior_stage"] or
                not isinstance(report.get("counterexamples"), list) or not report["counterexamples"] or
                any(not isinstance(case, str) or not case.strip() for case in report['counterexamples']) or
                not isinstance(report.get("observations"), list) or
                not isinstance(report.get("unresolved"), list)):
                raise RuntimeError("review_missing_counterexample_pass_or_schema")
        for path, expected in checkpoints.items():
            if digest(confined(root, path)) != expected:
                raise RuntimeError("modified-after-checkpoint:" + path)
        self.store.event("stage_checks_passed", {"task": task["id"], "deliverables": checkpoints})

    def _advance(self, task: dict, plan: dict, root: Path) -> None:
        role = task["stage"]
        if role == "REVIEW":
            nxt = task["next_stage"]
            if nxt == "__GATE__":
                self._gate(task["project_id"], plan, root)
            else:
                self.store.enqueue(task["project_id"], nxt, task["cycle"],
                                   make_brief(nxt, plan, task["brief"].get("notes_verbatim", ""), "REVIEW"))
        else:
            nxt = STAGE_NEXT[role]
            review_notes = f"Counterexample-pass review of {role} handoff; your observations are evidence, never a verdict."
            self.store.enqueue(task["project_id"], "REVIEW", task["cycle"],
                               make_brief("REVIEW", plan, review_notes, role), next_stage=nxt)

    def _record_verifier_receipts(self, project, root, receipts):
        pointers = []
        gate_id = 'verifier-' + uuid.uuid4().hex
        for i, receipt in enumerate(receipts):
            path = f'vault/verifier/{gate_id}/{i}.json'
            data = (canonical(receipt) + '\n').encode()
            sha = hashlib.sha256(data).hexdigest()
            archive = f'vault/checkpoints/{sha}.blob'
            atomic_write(confined(root, path), data)
            atomic_write(confined(root, archive), data)
            with self.store.transaction() as c:
                self.store.checkpoint(c, {'id': gate_id, 'generation': 0, 'project_id': project}, path, sha, archive)
            pointers.append(path)
        return pointers

    def integrity_errors(self, project, root):
        latest = {}
        errors = []
        for cp in self.store.checkpoints(project):
            try:
                if digest(confined(root, cp['archive'])) != cp['sha256']:
                    errors.append('checkpoint archive tampered:' + cp['archive'])
                latest[cp['path']] = cp['sha256']
            except (OSError, PolicyError):
                errors.append('checkpoint archive missing:' + cp['archive'])
        for path, expected in latest.items():
            try:
                if digest(confined(root, path)) != expected:
                    errors.append('modified-after-checkpoint:' + path)
            except (OSError, PolicyError):
                errors.append('checkpointed file missing:' + path)
        return errors

    def _discrepancy_checks(self, project):
        checks = []
        cycle = self.store.project(project)['cycle']
        tasks = {t['id']: t for t in self.store.tasks(project)
                 if t['cycle'] == cycle and t['status'] == 'SUCCEEDED'}
        for cp in self.store.checkpoints(project):
            if cp['task'] in tasks and cp['path'].endswith('/handoff.json'):
                checks.append({'type': 'discrepancy-free', 'path': cp['archive'], 'key': 'open_discrepancies'})
            if cp['task'] in tasks and tasks[cp['task']]['stage'] == 'REVIEW' and cp['path'].endswith('.json') and not cp['path'].startswith('vault/'):
                checks.append({'type': 'discrepancy-free', 'path': cp['archive'], 'key': 'unresolved'})
        return checks

    def _gate(self, project: str, plan: dict, root: Path) -> None:
        errors = self.integrity_errors(project, root)
        if errors:
            self.store.project_status(project, 'ESCALATED', ';'.join(errors))
            return
        if not self._pins_intact(plan, root):
            self.store.project_status(project, "ESCALATED", "pinned_spec_or_private_grader_changed")
            return
        private = Path(plan["private_grader_file"])
        checks = json.loads(private.read_text())["checks"] + self._discrepancy_checks(project)
        for role in ("INTEGRATOR", "QA"):
            for command in plan["stages"][role]["commands"].values():
                checks.append({"type": "command_receipt", "path": command["capture"]})
        verifier = Verifier(root, private.parent)
        result = verifier.verify(checks)
        result["receipt_pointers"] = self._record_verifier_receipts(project, root, verifier.receipts)
        self.store.event("satisfiability_gate", {"project": project, "report": result})
        if any("sandbox could not establish isolation" in str(r["error"]) or
               "bubblewrap required" in str(r["error"]) for r in result["results"]):
            self.store.project_status(project, "ESCALATED", "independent verifier sandbox unavailable")
            return
        if result["passed"] and result["open_discrepancies"] == 0:
            self.store.project_status(project, "AWAITING_COORDINATOR", "machine gate passed; cloud grade required")
            return
        # A defect signature derives only from independent deterministic failures.
        failures = [r["error"] for r in result["results"] if not r["passed"]]
        notes = "\n".join(failures)
        from .failures import fingerprint as defect_fingerprint
        fingerprint = defect_fingerprint(failures, root)
        cycle, repeats = self.store.failure_cycle(project, fingerprint)
        if repeats >= 3:
            self.store.project_status(project, "ESCALATED", "three failed cycles on same defect: " + notes)
        else:
            self.store.enqueue(project, "FIXER", cycle, make_brief("FIXER", plan, notes, "QA"))

    def coordinator_verdict(self, project: str, approved: bool, notes: str) -> None:
        p = self.store.project(project)
        if p["status"] != "AWAITING_COORDINATOR":
            raise PolicyError("coordinator can grade only a machine-gated project")
        if not notes.strip():
            raise PolicyError("coordinator must supply explicit reason/notes")
        if not self._pins_intact(p["plan"], Path(p["root"])):
            self.store.project_status(project, "ESCALATED", "pinned_spec_or_private_grader_changed_before_grade")
            raise PolicyError("ground truth changed after submission; escalation required")
        errors = self.integrity_errors(project, Path(p["root"]))
        if errors:
            self.store.project_status(project, "ESCALATED", ";".join(errors))
            raise PolicyError("checkpoint integrity failed before grade")
        private = Path(p["plan"]["private_grader_file"])
        checks = json.loads(private.read_text())["checks"] + self._discrepancy_checks(project)
        for role in ("INTEGRATOR", "QA"):
            for command in p["plan"]["stages"][role]["commands"].values():
                checks.append({"type": "command_receipt", "path": command["capture"]})
        verifier = Verifier(Path(p["root"]), private.parent)
        report = verifier.verify(checks)
        report["receipt_pointers"] = self._record_verifier_receipts(project, Path(p["root"]), verifier.receipts)
        self.store.event("pre_grade_recheck", {"project": project, "report": report})
        if not report["passed"]:
            self.store.project_status(project, "ESCALATED", "machine gate no longer passes at grading time")
            raise PolicyError("machine gate became invalid before final coordinator grade")
        self.store.event("coordinator_grade", {"project": project, "approved": approved, "notes_verbatim": notes})
        if approved:
            self.store.project_status(project, "DONE", notes)
        else:
            # Coordinator prose is never a deterministic defect fingerprint.
            cycle = p['cycle'] + 1
            with self.store.transaction() as c:
                c.execute('UPDATE projects SET cycle=? WHERE id=?', (cycle, project))
            self.store.project_status(project, "RUNNING", "coordinator returned failed work")
            self.store.enqueue(project, "FIXER", cycle,
                               make_brief("FIXER", p["plan"], notes, "COORDINATOR"))

    @staticmethod
    def _pins_intact(plan: dict, root: Path) -> bool:
        try:
            return (all(digest(confined(root, p)) == expected for p, expected in plan.get("charter_hashes", {}).items()) and
                    all(digest(confined(root, p)) == expected for p, expected in plan["input_hashes"].items()) and
                    all(digest(Path(p)) == expected for p, expected in plan["grading_hashes"].items()))
        except (OSError, ValueError, PolicyError):
            return False

    async def serve(self, jobs: int = 1, until_idle: bool = True) -> None:
        if jobs != 1:
            raise ValueError("one trusted worker job is supported")
        import fcntl
        with self.store.db.with_suffix(".worker.lock").open("a+b") as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise RuntimeError("another runtime owns this board") from exc
            try:
                await self._serve(jobs, until_idle)
            finally:
                fcntl.flock(lock, fcntl.LOCK_UN)

    async def _serve(self, jobs=1, until_idle=True):
        await self.queue.start()
        # Repair a crash after durable stage finish but before enqueue/gate.
        for project in self.store.projects():
            tasks = self.store.tasks(project["id"])
            if project["status"] == "RUNNING" and tasks and tasks[-1]["status"] == "SUCCEEDED":
                with self.store.transaction() as c:
                    row = dict(c.execute("SELECT * FROM tasks WHERE id=?", (tasks[-1]["id"],)).fetchone())
                row["brief"] = json.loads(row["brief"])
                p = self.store.project(project["id"])
                self._advance(row, p["plan"], Path(p["root"]))
        async def worker(index: int) -> None:
            owner = uuid.uuid4().hex + "-" + str(index)
            while True:
                task = self.store.claim(owner, self.lease_seconds)
                if task:
                    await self.run_task(task)
                elif until_idle:
                    return
                else:
                    await asyncio.sleep(1)
        try:
            await asyncio.gather(*(worker(i) for i in range(jobs)))
        finally:
            await self.queue.stop()
