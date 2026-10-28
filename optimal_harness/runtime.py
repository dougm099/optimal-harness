"""Bounded fresh-context runtime, with trusted receipts and honest handoffs."""
from __future__ import annotations
import asyncio
import hashlib
import json
from pathlib import Path
from .ledger import Ledger
from .model import SYSTEM_PREFIX, parse_action, ModelError
from .policy import PolicyError, atomic_write, confined, sandbox_command
from .store import canonical, LeaseLost


def intake_document(data):
    """Normalize only an unambiguous wrapper omission; never invent slot content."""
    from .verifier import LIFECYCLE
    try:
        document = json.loads(data) if isinstance(data, str) else data
    except ValueError as exc:
        raise PolicyError('INTAKE schema: data must be a complete JSON object') from exc
    if isinstance(document, dict) and set(document) == set(LIFECYCLE):
        document = {'slots': document}
    slots = document.get('slots', {}) if isinstance(document, dict) else {}
    if not isinstance(slots, dict) or set(slots) != set(LIFECYCLE):
        keys = set(slots) if isinstance(slots, dict) else set()
        raise PolicyError('INTAKE schema: rewrite the COMPLETE document with slots object; missing=' +
                          ','.join(sorted(set(LIFECYCLE)-keys)) + '; unexpected=' +
                          ','.join(sorted(keys-set(LIFECYCLE))))
    for key in LIFECYCLE:
        item = slots[key]
        if not isinstance(item, dict) or item.get('status') not in ('citation', 'gap'):
            raise PolicyError('INTAKE schema: ' + key + ' must be an object with status=citation or gap')
        fields = ('quote', 'because') if item['status'] == 'citation' else ('detail',)
        for field in fields:
            if not isinstance(item.get(field), str) or not item[field].strip():
                raise PolicyError('INTAKE schema: ' + key + '.' + field + ' must be a nonempty string')
    return document


async def run_task(engine, task):
    store = engine.store
    project = store.project(task['project_id'])
    plan, root = project['plan'], Path(project['root'])
    conf = plan['stages'][task['stage']]
    ledger = Ledger(conf.get('budget'))
    checkpoints = {}
    monitor = asyncio.create_task(engine._heartbeat(task))
    broker = handoff = evidence = None
    manifest_path = f"vault/runs/{task['id']}/a{task['generation']}/manifest.json"
    last = 'New run. Write deliverables, write ready handoff, then finish.'
    last_observation = last
    repeat = 0
    previous = None
    seen_actions = {}
    intake_format_errors = 0
    manifest = {'task': task['id'], 'generation': task['generation'], 'role': task['stage'],
                'budget': ledger.budget, 'static_prefix_sha256': hashlib.sha256(SYSTEM_PREFIX.encode()).hexdigest(),
                'charter_sha256': plan['charter_hashes'][f"vault/charters/{task['stage'].lower()}.md"],
                'cache_configuration': getattr(engine.queue, 'cache_config', {'max_bytes': 268435456, 'verified': False}),
                'status': 'running'}
    manifest['cache_configuration_sha256'] = hashlib.sha256(canonical(manifest['cache_configuration']).encode()).hexdigest()

    def trusted_write(path, data):
        def effect(c):
            atomic_write(confined(root, path), data)
            # An honest runtime error may replace a model's ready handoff.
            # Keep its new trusted value checkpointed rather than falsely
            # diagnosing the runtime's own report update as outside tampering.
            if path in checkpoints:
                engine._checkpoint(root, task, path, checkpoints, 'trusted_report', c)
        store.commit(task, effect, allow_report=True)

    def write_checkpoint(path, data=None, append=False):
        def effect(c):
            count = None
            if data is not None:
                count = broker.write(path, data, append=append)
            engine._checkpoint(root, task, path, checkpoints, 'runtime', c)
            return count
        return store.commit(task, effect)

    try:
        if not engine._pins_intact(plan, root):
            raise PolicyError('pinned input or charter changed')
        broker, evidence, handoff, brief = engine._prepare(root, task)
        manifest['evidence'], manifest['handoff'] = evidence, handoff
        trusted_write(manifest_path, (canonical(manifest)+'\n').encode())
        while True:
            store.assert_lease(task)
            progress = canonical({'checkpointed': sorted(checkpoints),
                                  'missing_deliverables': [p for p in conf.get('deliverables', []) if p not in checkpoints],
                                  'handoff_path': handoff, 'evidence_path': evidence})
            missing = [p for p in conf.get('deliverables', []) if p not in checkpoints]
            context_task = dict(task)
            context_task['brief'] = dict(task['brief']) | {
                'deliverables': missing,
                'already_checkpointed_do_not_repeat': sorted(checkpoints),
                'role_work_complete': not missing,
                'current_goal': ('Produce remaining deliverables only' if missing else
                                 'The role work is ALREADY COMPLETE. Only write the ready handoff and finish.'),
            }
            prompt = engine._context(root, context_task, broker, last) + '\nSAVED RUN STATE:\n' + progress
            prompt += ('\nEXECUTION RULE: The LAST TOOL OBSERVATION has ALREADY EXECUTED. '
                       'Do not request that same read again. Use the returned content to do the next step. '
                       'Files in checkpointed are already written; do not repeat those writes. '
                       'Once all deliverables are checkpointed, write the handoff JSON with '
                       'status=ready, open_discrepancies=[], summary=<factual summary>, then finish. '
                       'Respond with ONE next JSON action only.')
            missing = [p for p in conf.get('deliverables', []) if p not in checkpoints]
            commands = {cmd['capture']: name for name, cmd in conf.get('commands', {}).items()}
            if missing:
                target = missing[0]
                next_step = ('Run named command ' + commands[target] if target in commands else
                             'Create ' + target + ' using action=write, path=' + target + ', data=<complete file text>')
            elif handoff not in checkpoints:
                next_step = 'Write ready handoff at ' + handoff
            else:
                next_step = 'All required outputs exist. Emit action=finish now; do not read or rewrite them.'
            if ledger.calls == 0 and plan['spec'] in task['brief']['permitted_reads']:
                next_step = 'Read ' + plan['spec'] + ' once to obtain the authoritative source before writing.'
            prompt += '\nSCHEDULER CURRENT STEP (not a new run): ' + next_step
            prompt += '\nCompleted model calls in this attempt: ' + str(ledger.calls)
            if last.startswith('{') and '"content":' in last:
                prompt += '\nThe read response above is already available. NOW perform the current write step, not another read.'
            cap, timeout = ledger.reserve(prompt, SYSTEM_PREFIX)
            store.event('model_call_started', {'task': task['id'], 'generation': task['generation'],
                        'call': ledger.calls+1, 'prompt_sha256': hashlib.sha256(prompt.encode()).hexdigest(),
                        'completion_cap': cap})
            manifest["calls_dispatched"] = manifest.get("calls_dispatched", 0) + 1
            try:
                result = await engine.queue.request(prompt, cap, timeout)
            except Exception:
                manifest["usage_unknown_for_failed_request"] = True
                raise
            # Persist actual usage before the breaker, even if the server over-reports.
            store.event('model_call_finished', {'task': task['id'], 'generation': task['generation'],
                        'prompt_tokens': result.prompt_tokens, 'cached_tokens': result.cached_tokens,
                        'completion_tokens': result.completion_tokens, 'fresh_tokens': result.fresh_tokens,
                        'raw_action': result.text})
            metered = store.meter(task, result.prompt_tokens, result.cached_tokens, result.completion_tokens)
            ledger.charge(result)
            if metered["budget_exceeded"]:
                raise RuntimeError("circuit_breaker:board_attempt_budget")
            if ledger.seconds >= ledger.budget['max_seconds']:
                raise RuntimeError('circuit_breaker:wall_clock_after_call')
            try:
                decoder = json.JSONDecoder()
                _, end = decoder.raw_decode(result.text.lstrip())
                first = result.text.lstrip()[:end]
                if result.text.lstrip()[end:].strip():
                    store.event('additional_actions_discarded', {'task': task['id'], 'call': ledger.calls,
                                'reason': 'only first JSON object is eligible; no speculative follow-up executes'})
                action = parse_action(first)
            except (ModelError, ValueError) as exc:
                # A bounded correction is fresh, not replay of malformed history.
                last = ('PROTOCOL_ERROR: ' + str(exc) + '. Respond with exactly one JSON action; JSON documents may use object-valued data.\n'
                        + 'LAST SUCCESSFUL TOOL OBSERVATION (still authoritative):\n' + last_observation)
                repeat += 1
                if repeat >= 3:
                    raise RuntimeError('error:no_progress:invalid_actions')
                continue
            op = action['action']
            signature = canonical(action)
            repeat = repeat + 1 if signature == previous else 0
            previous = signature
            seen_actions[signature] = seen_actions.get(signature, 0) + 1
            if repeat >= 3 or seen_actions[signature] >= 4:
                raise RuntimeError('error:no_progress:repeated_action')
            if op == 'fail':
                raise RuntimeError('honest_worker_failure:' + action['reason'])
            if op == 'finish':
                engine._check_stage(root, task, conf, checkpoints, handoff)
                manifest.update(status='succeeded', usage=ledger.totals())
                trusted_write(manifest_path, (canonical(manifest)+'\n').encode())
                store.handoff(task, {'status': 'ready', 'evidence': evidence, 'handoff': handoff,
                                    'manifest': manifest_path, 'usage': ledger.totals(), 'budget_exceeded': False})
                store.finish(task, 'SUCCEEDED')
                engine._advance(task, plan, root)
                return
            try:
                store.assert_lease(task)
                if op == 'read':
                    offset, length = action.get('offset', 0), action.get('length', 16384)
                    # Budget includes JSON escaping; no silent truncation of pages.
                    if length > ledger.budget['max_observation_bytes'] // 2:
                        raise PolicyError('page exceeds observation budget; use smaller length')
                    content = broker.read(action['path'], offset, length)
                    full = confined(root, action['path'])
                    last = canonical({'path': action['path'], 'offset': offset, 'length': length,
                                      'total_bytes': full.stat().st_size, 'content': content,
                                      'next_offset': min(offset+len(content.encode('utf-8')), full.stat().st_size)})
                    if len(last.encode('utf-8')) > ledger.budget['max_observation_bytes']:
                        raise PolicyError('encoded read page exceeds observation budget; reduce length')
                elif op in ('write', 'append'):
                    data = action["data"]
                    if task['stage'] == 'INTAKE' and action['path'] in conf.get('deliverables', []):
                        try:
                            if op != 'write':
                                raise PolicyError('INTAKE schema: use write for the complete JSON document, not append')
                            data = intake_document(data)
                        except PolicyError:
                            intake_format_errors += 1
                            if intake_format_errors >= 3:
                                raise RuntimeError('intake_format_exhausted:three invalid documents; no artifact accepted')
                            raise
                    if task['stage'] == 'REVIEW' and action['path'] in conf.get('deliverables', []):
                        report = json.loads(data) if isinstance(data, str) else data
                        if (not isinstance(report, dict) or report.get('reviewed_stage') != task['brief']['prior_stage'] or
                            not isinstance(report.get('counterexamples'), list) or not report['counterexamples'] or
                            any(not isinstance(case, str) or not case.strip() for case in report['counterexamples']) or
                            not isinstance(report.get('observations'), list) or not isinstance(report.get('unresolved'), list)):
                            raise PolicyError('REVIEW schema: reviewed_stage must be ' + task['brief']['prior_stage'] +
                                              '; counterexamples must contain at least one nonempty ATTEMPTED test case even if no bug was found; observations and unresolved must be lists. Rewrite the complete report.')
                    if isinstance(data, (dict, list)):
                        data = canonical(data) + "\n"
                    count = write_checkpoint(action['path'], data, op == 'append')
                    last = canonical({'written': action['path'], 'bytes': count, 'sha256': checkpoints[action['path']]})
                elif op == 'command':
                    if action['name'] not in conf.get('commands', {}):
                        raise PolicyError('command not allowed')
                    command = conf['commands'][action['name']]
                    # Non-isolated commands are synchronous under lease fence: reclaimed
                    # workers cannot commit staged outputs after another worker claims.
                    def command_effect(c):
                        output = sandbox_command(root, command, min(ledger.budget['max_command_seconds'],
                                                 ledger.budget['max_seconds']-ledger.seconds))
                        capture = command['capture']
                        payload = (canonical(output)+'\n').encode()
                        owned_capture = f"vault/runs/{task['id']}/a{task['generation']}/commands/{action['name']}.json"
                        atomic_write(confined(root, owned_capture), payload)
                        engine._checkpoint(root, task, owned_capture, checkpoints, 'command', c)
                        atomic_write(confined(root, capture), payload)
                        engine._checkpoint(root, task, capture, checkpoints, 'command_alias', c)
                        for path in command.get('writes', []):
                            if confined(root, path).is_file():
                                engine._checkpoint(root, task, path, checkpoints, 'command_write', c)
                        return {'capture': owned_capture, 'declared_capture': capture, 'exit_code': output['exit_code'], 'reason': output['reason'],
                                'containment': output.get('containment', 'test-double')}
                    receipt = await asyncio.to_thread(store.commit, task, command_effect)
                    last = canonical(receipt) + '\nFull output is at capture pointer. Read it if needed.'
                last_observation = last
                store.event('tool_action', {'task': task['id'], 'generation': task['generation'],
                            'op': op, 'path_or_name': action.get('path', action.get('name')), 'receipt': last})
            except (PolicyError, OSError, ValueError) as exc:
                last = 'TOOL_DENIED: ' + str(exc)[:1500] + '\nLAST SUCCESSFUL TOOL OBSERVATION:\n' + last_observation
                if len(last.encode()) > ledger.budget['max_observation_bytes']:
                    last = 'TOOL_DENIED: ' + str(exc)[:1500] + '\nRe-read the last source using a smaller page before retrying.'
                store.event('tool_denied', {'task': task['id'], 'op': op, 'reason': str(exc)})
    except (Exception, asyncio.CancelledError) as exc:
        reason = str(exc) or type(exc).__name__
        manifest.update(status='failed', reason=reason, usage=ledger.totals())
        store.event('run_aborted', {'task': task['id'], 'generation': task['generation'], **manifest})
        try:
            trusted_write(manifest_path, (canonical(manifest)+'\n').encode())
            if handoff:
                trusted_write(handoff, (canonical({'status': 'error', 'reason': reason,
                              'open_discrepancies': [reason], 'evidence': evidence})+'\n').encode())
                if evidence:
                    old = confined(root, evidence).read_bytes()
                    trusted_write(evidence, old+(canonical({'event': 'honest_failure', 'reason': reason})+'\n').encode())
            store.handoff(task, {'status': 'error', 'reason': reason, 'handoff': handoff, 'evidence': evidence,
                                'manifest': manifest_path, 'usage': ledger.totals(),
                                'budget_stopped': reason.startswith('circuit_breaker'),
                                'budget_exceeded': (ledger.fresh > ledger.budget['max_fresh_tokens'] or
                                                    ledger.calls > ledger.budget['max_calls'] or
                                                    ledger.seconds > ledger.budget['max_seconds'])})
            store.finish(task, 'FAILED', reason)
            store.project_status(task['project_id'], 'ESCALATED', reason)
        except LeaseLost:
            pass
    finally:
        monitor.cancel()
        await asyncio.gather(monitor, return_exceptions=True)
