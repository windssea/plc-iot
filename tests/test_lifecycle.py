"""Exercise lifecycle against real SQLite and task-owning test device runners."""
import asyncio
from contextlib import closing
import copy
import importlib
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import threading
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))


class TaskRunner:
    """A real cancellable task standing in for a future network driver."""
    def __init__(self, config, factory):
        self.config, self.factory = config, factory
        self.task = None
        self.starts = 0
        self.fail_restart = False

    @property
    def running(self):
        return self.task is not None and not self.task.done()

    async def start(self):
        self.starts += 1
        if self.running:
            raise RuntimeError("duplicate device task")
        self.task = asyncio.create_task(asyncio.Event().wait())
        address = self.config.points[0].modbus.address
        self.factory.entered.set()
        if address in self.factory.gates:
            await self.factory.gates[address].wait()
        if address in self.factory.fail_addresses or (self.fail_restart and self.starts > 1):
            raise RuntimeError("driver failure with sensitive details")

    async def stop(self):
        if self.task is not None:
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)
            self.task = None


class TaskFactory:
    def __init__(self):
        self.runners = []
        self.gates = {}
        self.fail_addresses = set()
        self.entered = asyncio.Event()

    def __call__(self, config):
        runner = TaskRunner(config, self)
        self.runners.append(runner)
        return runner


class LifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.assertTrue((ROOT / "src/plcnext_iot/core/lifecycle.py").is_file(),
                        "Execution coordinator and lifecycle have not been implemented")
        self.life = importlib.import_module("plcnext_iot.core.lifecycle")
        self.settings = importlib.import_module("plcnext_iot.core.settings")
        self.runtime_module = importlib.import_module("plcnext_iot.devices.runtime")
        self.store_module = importlib.import_module("plcnext_iot.config.store")
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.msg = json.loads((ROOT / "contracts/examples/valid/config-set.json").read_text())
        self.factory = TaskFactory()
        self.agents = []

    async def asyncTearDown(self):
        for agent in reversed(self.agents):
            try:
                await agent.close()
            except self.life.LifecycleError:
                pass
        for runner in self.factory.runners:
            await runner.stop()
        self.tmp.cleanup()

    def make_agent(self, *, factory=True, queue_capacity=2, timeout=1, store_factory=None):
        settings = self.settings.AgentSettings("PLC-01", self.root, timeout, queue_capacity)
        runtime = self.runtime_module.DeviceRuntime(self.factory if factory else None)
        agent = self.life.AgentLifecycle(settings, runtime, store_factory=store_factory)
        self.agents.append(agent)
        return agent

    def payload(self, version=10, address=0, *, empty=False, enabled=True):
        message = copy.deepcopy(self.msg)
        message.update(configVersion=version, messageId=f"request-{version}")
        message["config"]["enabled"] = enabled
        if empty:
            message["config"]["devices"] = []
        else:
            message["config"]["devices"][0]["points"][0]["modbus"]["address"] = address
        return json.dumps(message).encode()

    async def test_empty_start_close_and_instance_cannot_restart(self):
        agent = self.make_agent()
        self.assertEqual((await agent.submit(self.payload())).code, "NOT_RUNNING")
        await agent.start()
        self.assertEqual(agent.state, "WAITING_CONFIG")
        self.assertEqual(agent.active_config_version, 0)
        await agent.close()
        await agent.close()
        self.assertEqual(agent.state, "STOPPED")
        with self.assertRaises(self.life.LifecycleError):
            await agent.start()
        with self.store_module.ConfigStore(self.root / "agent.db", "PLC-01"):
            pass

    async def test_apply_commit_restart_restores_device_tasks(self):
        agent = self.make_agent()
        await agent.start()
        result = await agent.submit(self.payload())
        self.assertEqual(result.status, "APPLIED")
        self.assertEqual(agent.state, "RUNNING")
        self.assertTrue(agent.runtime.runners["meter-A"].running)
        await agent.close()
        self.assertFalse(any(r.running for r in self.factory.runners))
        restarted = self.make_agent()
        await restarted.start()
        self.assertEqual(restarted.active_config_version, 10)
        self.assertTrue(restarted.runtime.runners["meter-A"].running)

    async def test_unmodified_device_and_report_only_changes_do_not_restart(self):
        b = copy.deepcopy(self.msg["config"]["devices"][0])
        b["deviceId"] = "meter-B"
        self.msg["config"]["devices"].append(b)
        agent = self.make_agent()
        await agent.start()
        await agent.submit(self.payload())
        old_a, old_b = agent.runtime.runners["meter-A"], agent.runtime.runners["meter-B"]
        await agent.submit(self.payload(11, 1))
        self.assertIs(agent.runtime.runners["meter-B"], old_b)
        self.assertTrue(old_b.running)
        self.assertFalse(old_a.running)
        a = agent.runtime.runners["meter-A"]
        self.msg["config"]["devices"][0]["points"][0]["deadband"] = 2
        await agent.submit(self.payload(12, 1))
        self.assertIs(agent.runtime.runners["meter-A"], a)
        self.assertEqual(agent.runtime.snapshot.devices[0].points[0].deadband, 2)

    async def test_failed_partial_start_restores_old_tasks_and_version(self):
        agent = self.make_agent()
        await agent.start()
        await agent.submit(self.payload())
        old = agent.runtime.runners["meter-A"]
        self.factory.fail_addresses.add(1)
        result = await agent.submit(self.payload(11, 1))
        self.assertEqual(result.status, "FAILED")
        self.assertEqual(agent.active_config_version, 10)
        self.assertIs(agent.runtime.runners["meter-A"], old)
        self.assertTrue(old.running)
        self.assertFalse(self.factory.runners[-1].running)
        self.assertNotIn("sensitive", str(result))
        self.factory.fail_addresses.clear()
        self.assertEqual((await agent.submit(self.payload(11, 1))).status, "APPLIED")

    async def test_commit_failure_rolls_runtime_back(self):
        agent = self.make_agent()
        await agent.start()
        await agent.submit(self.payload())
        with closing(sqlite3.connect(self.root / "agent.db")) as conn:
            conn.execute("""CREATE TRIGGER reject_activation BEFORE UPDATE OF active_version ON metadata
                            BEGIN SELECT RAISE(ABORT,'write refused'); END""")
        result = await agent.submit(self.payload(11, 1))
        self.assertEqual(result.code, "STORAGE_ERROR")
        self.assertEqual(agent.active_config_version, 10)
        self.assertEqual(agent.runtime.runners["meter-A"].config.points[0].modbus.address, 0)
        self.assertTrue(agent.runtime.runners["meter-A"].running)

    async def test_ack_loss_after_db_commit_is_resolved_by_reading_committed_state(self):
        Base, Error = self.store_module.ConfigStore, self.store_module.StoreError
        class CommitThenRaise(Base):
            def commit(self, token):
                super().commit(token)
                raise Error("outcome notification lost")
        agent = self.make_agent(store_factory=CommitThenRaise)
        await agent.start()
        self.assertEqual((await agent.submit(self.payload())).status, "APPLIED")
        self.assertEqual(agent.active_config_version, 10)
        self.assertTrue(agent.runtime.runners["meter-A"].running)

    async def test_restore_failure_never_reports_running_and_releases_database(self):
        with self.store_module.ConfigStore(self.root / "agent.db", "PLC-01") as store:
            store.commit(store.prepare(self.payload()).token)
        self.factory.fail_addresses.add(0)
        agent = self.make_agent()
        with self.assertRaises(self.life.LifecycleError):
            await agent.start()
        self.assertEqual(agent.state, "FAILED")
        self.assertFalse(any(r.running for r in self.factory.runners))
        self.assertNotEqual((await agent.submit(self.payload())).status, "APPLIED")
        with self.store_module.ConfigStore(self.root / "agent.db", "PLC-01") as store:
            self.assertEqual(store.load_active().config_version, 10)

    async def test_rollback_failure_enters_failed_and_stops_accepting(self):
        agent = self.make_agent()
        await agent.start()
        await agent.submit(self.payload())
        agent.runtime.runners["meter-A"].fail_restart = True
        self.factory.fail_addresses.add(1)
        result = await agent.submit(self.payload(11, 1))
        self.assertEqual(result.code, "ROLLBACK_FAILED")
        self.assertEqual(agent.state, "FAILED")
        self.assertFalse(any(r.running for r in self.factory.runners))
        self.assertEqual((await agent.submit(self.payload(12))).code, "NOT_RUNNING")

    async def test_timeout_cleans_candidate_and_keeps_old_version(self):
        agent = self.make_agent(timeout=0.05)
        await agent.start()
        await agent.submit(self.payload())
        self.factory.gates[1] = asyncio.Event()
        result = await agent.submit(self.payload(11, 1))
        self.assertEqual(result.code, "APPLY_TIMEOUT")
        self.assertEqual(agent.active_config_version, 10)
        self.assertTrue(agent.runtime.runners["meter-A"].running)
        self.assertFalse(self.factory.runners[-1].running)

    async def test_cancelled_submit_waiter_does_not_cancel_transaction(self):
        agent = self.make_agent()
        await agent.start()
        gate = self.factory.gates[0] = asyncio.Event()
        task = asyncio.create_task(agent.submit(self.payload()))
        await asyncio.wait_for(self.factory.entered.wait(), 1)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        gate.set()
        result = await agent.submit(self.payload())
        self.assertEqual(result.status, "APPLIED")
        self.assertEqual(len([r for r in self.factory.runners if r.running]), 1)

    async def test_bounded_queue_rejects_excess_and_executes_in_order(self):
        agent = self.make_agent(queue_capacity=1)
        await agent.start()
        gate = self.factory.gates[0] = asyncio.Event()
        first = asyncio.create_task(agent.submit(self.payload()))
        await asyncio.wait_for(self.factory.entered.wait(), 1)
        second = asyncio.create_task(agent.submit(self.payload(11, 1)))
        await asyncio.sleep(0)
        self.assertEqual((await agent.submit(self.payload(12, 2))).code, "RESOURCE_LIMIT")
        gate.set()
        self.assertEqual((await first).active_config_version, 10)
        self.assertEqual((await second).active_config_version, 11)

    async def test_close_rejects_queued_and_new_requests_but_finishes_inflight(self):
        agent = self.make_agent()
        await agent.start()
        gate = self.factory.gates[0] = asyncio.Event()
        inflight = asyncio.create_task(agent.submit(self.payload()))
        await asyncio.wait_for(self.factory.entered.wait(), 1)
        queued = asyncio.create_task(agent.submit(self.payload(11, 1)))
        await asyncio.sleep(0)
        stopping = asyncio.create_task(agent.close())
        await asyncio.sleep(0)
        self.assertNotEqual((await agent.submit(self.payload(12))).status, "APPLIED")
        gate.set()
        self.assertEqual((await queued).code, "STOPPING")
        self.assertEqual((await inflight).status, "APPLIED")
        await stopping
        self.assertEqual(agent.state, "STOPPED")
        self.assertFalse(any(r.running for r in self.factory.runners))

    async def test_disabled_config_stops_tasks_and_empty_config_works_without_driver(self):
        agent = self.make_agent()
        await agent.start()
        await agent.submit(self.payload())
        self.assertEqual((await agent.submit(self.payload(11, enabled=False))).status, "APPLIED")
        self.assertEqual(agent.state, "DISABLED")
        self.assertFalse(agent.runtime.runners)
        await agent.close()
        agent = self.make_agent(factory=False)
        await agent.start()
        self.assertEqual((await agent.submit(self.payload(12))).code, "UNSUPPORTED_DRIVER")
        self.assertEqual((await agent.submit(self.payload(13, empty=True))).status, "APPLIED")

    async def test_store_is_created_used_and_closed_off_loop_on_same_thread(self):
        ids = []
        Base = self.store_module.ConfigStore
        class TracedStore(Base):
            def __init__(self, *args, **kwargs):
                ids.append(threading.get_ident())
                super().__init__(*args, **kwargs)
            def prepare(self, payload):
                ids.append(threading.get_ident())
                return super().prepare(payload)
            def close(self):
                ids.append(threading.get_ident())
                return super().close()
        agent = self.make_agent(store_factory=TracedStore)
        await agent.start()
        await agent.submit(self.payload())
        await agent.close()
        self.assertEqual(len(set(ids)), 1)
        self.assertNotEqual(ids[0], threading.get_ident())

    async def test_bootstrap_strictness_and_relative_data_directory(self):
        path = self.root / "bootstrap.json"
        path.write_text(json.dumps({"gatewayId": "PLC-01", "dataDirectory": "data",
                                   "operationTimeoutSeconds": 1, "queueCapacity": 2}))
        config = self.settings.AgentSettings.from_file(path)
        self.assertEqual(config.data_directory, self.root / "data")
        for content in ['{"gatewayId":"one","gatewayId":"two"}',
                        '{"gatewayId":"PLC-01","dataDirectory":"data","unknown":true}']:
            path.write_text(content)
            with self.assertRaises(ValueError):
                self.settings.AgentSettings.from_file(path)

    async def test_unreadable_commit_outcome_fails_closed_without_guessing_version(self):
        Base, Error = self.store_module.ConfigStore, self.store_module.StoreError
        class UnreadableAfterCommit(Base):
            unreadable = False
            def commit(self, token):
                super().commit(token)
                self.unreadable = True
                raise Error("commit notification lost")
            def load_active(self):
                if self.unreadable:
                    raise Error("cannot determine committed state")
                return super().load_active()
        agent = self.make_agent(store_factory=UnreadableAfterCommit)
        await agent.start()
        result = await agent.submit(self.payload())
        self.assertEqual(result.code, "COMMIT_OUTCOME_UNKNOWN")
        self.assertIsNone(result.active_config_version)
        self.assertEqual(agent.state, "FAILED")
        self.assertFalse(any(r.running for r in self.factory.runners))
        await agent.close()
        with Base(self.root / "agent.db", "PLC-01") as store:
            self.assertEqual(store.load_active().config_version, 10)

    async def test_cancelled_close_waiter_still_releases_tasks_and_database(self):
        agent = self.make_agent()
        await agent.start()
        gate = self.factory.gates[0] = asyncio.Event()
        submit = asyncio.create_task(agent.submit(self.payload()))
        await asyncio.wait_for(self.factory.entered.wait(), 1)
        closing = asyncio.create_task(agent.close())
        await asyncio.sleep(0)
        closing.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await closing
        gate.set()
        await submit
        await agent.close()
        self.assertFalse(any(r.running for r in self.factory.runners))
        self.assertEqual(agent.state, "STOPPED")
        with self.store_module.ConfigStore(self.root / "agent.db", "PLC-01"):
            pass

    async def test_cancelled_start_waiter_finishes_cleanup(self):
        with self.store_module.ConfigStore(self.root / "agent.db", "PLC-01") as store:
            store.commit(store.prepare(self.payload()).token)
        agent = self.make_agent()
        gate = self.factory.gates[0] = asyncio.Event()
        starting = asyncio.create_task(agent.start())
        await asyncio.wait_for(self.factory.entered.wait(), 1)
        starting.cancel()
        gate.set()
        with self.assertRaises(asyncio.CancelledError):
            await starting
        self.assertEqual(agent.state, "STOPPED")
        self.assertFalse(any(r.running for r in self.factory.runners))

    async def test_removed_child_stops_without_restarting_sibling(self):
        b = copy.deepcopy(self.msg["config"]["devices"][0])
        b["deviceId"] = "meter-B"
        self.msg["config"]["devices"].append(b)
        agent = self.make_agent()
        await agent.start()
        await agent.submit(self.payload())
        old_a, old_b = agent.runtime.runners["meter-A"], agent.runtime.runners["meter-B"]
        self.msg["config"]["devices"] = [b]
        await agent.submit(self.payload(11))
        self.assertNotIn("meter-A", agent.runtime.runners)
        self.assertFalse(old_a.running)
        self.assertIs(agent.runtime.runners["meter-B"], old_b)
        self.assertTrue(old_b.running)

    async def test_candidate_cannot_publish_before_commit_and_old_runner_is_fenced(self):
        agent = self.make_agent()
        await agent.start()
        await agent.submit(self.payload())
        old = agent.runtime.runners["meter-A"]
        self.factory.entered.clear()
        gate = self.factory.gates[1] = asyncio.Event()
        applying = asyncio.create_task(agent.submit(self.payload(11, 1)))
        try:
            await asyncio.wait_for(self.factory.entered.wait(), 1)
            candidate = self.factory.runners[-1]
            self.assertIsNone(agent.runtime.sample_context("meter-A", candidate))
            self.assertEqual(agent.runtime.sample_context("meter-A", old).config_version, 10)
            self.assertEqual(agent.active_config_version, 10)
        finally:
            gate.set()
            await applying
        self.assertIsNone(agent.runtime.sample_context("meter-A", old))
        context = agent.runtime.sample_context("meter-A", candidate)
        self.assertEqual(context.config_version, 11)
        self.assertEqual(context.device.points[0].modbus.address, 1)

    async def test_cli_rejects_real_device_configuration_without_installed_driver(self):
        bootstrap = self.root / "bootstrap.json"
        bootstrap.write_text(json.dumps({"gatewayId":"PLC-01","dataDirectory":"data",
                                        "operationTimeoutSeconds":1,"queueCapacity":2}))
        payload = self.root / "device.json"
        payload.write_bytes(self.payload())
        process = await asyncio.create_subprocess_exec(
            sys.executable, "-m", "tools.agent", "--bootstrap", str(bootstrap),
            "--config", str(payload), "--run-seconds", "0", cwd=ROOT,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = await asyncio.wait_for(process.communicate(), 5)
        self.assertEqual(process.returncode, 1, err.decode())
        events = [json.loads(line) for line in out.decode().splitlines()]
        self.assertEqual(events[1]["code"], "UNSUPPORTED_DRIVER")
        self.assertEqual(events[-1]["activeConfigVersion"], 0)

    async def test_cli_applies_empty_config_and_restores_on_next_process(self):
        bootstrap = self.root / "bootstrap.json"
        bootstrap.write_text(json.dumps({"gatewayId":"PLC-01","dataDirectory":"data",
                                        "operationTimeoutSeconds":1,"queueCapacity":2}))
        payload = self.root / "empty.json"
        payload.write_bytes(self.payload(empty=True))
        for args in (["--config", str(payload)], []):
            process = await asyncio.create_subprocess_exec(
                sys.executable, "-m", "tools.agent", "--bootstrap", str(bootstrap),
                "--run-seconds", "0", *args, cwd=ROOT,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            out, err = await asyncio.wait_for(process.communicate(), 5)
            self.assertEqual(process.returncode, 0, err.decode())
            events = [json.loads(line) for line in out.decode().splitlines()]
            self.assertEqual(events[-1]["state"], "STOPPED")
            self.assertEqual(events[-1]["activeConfigVersion"], 10)
