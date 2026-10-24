"""Exercise production collector loops using only injected readers/SDKs."""
import asyncio
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from test_phase1_events import setup_engine
from plcnext_iot.devices.modbus import ModbusRunner
from plcnext_iot.devices.opcua import OpcUaRunner
from plcnext_iot.devices.runtime import DeviceRuntime
from plcnext_iot.drivers.modbus import ModbusReader, ReadError
from plcnext_iot.drivers.opcua import OpcUaReader


def runtime_with(device, snapshot, samples, runner_type):
    runtime=DeviceRuntime()
    runtime.snapshot=replace(snapshot,devices=(device,))
    runtime.sample_generation=1
    runner=runner_type(device,runtime,samples)
    runtime._runners[device.device_id]=runner
    return runtime,runner


async def collect(samples, predicate, count):
    result=[]
    async with asyncio.timeout(2):
        while len(result)<count:
            s=await samples.get()
            if predicate(s):result.append(s)
    return result


class CollectorTests(unittest.IsolatedAsyncioTestCase):
    async def test_opcua_configuration_error_is_diagnostic_without_fake_physical_result(self):
        _,snapshot,samples,_=setup_engine()
        device=replace(snapshot.devices[0],protocol='opcua',connection=replace(
            snapshot.devices[0].connection,security_policy='Basic256Sha256',security_mode='Sign'))
        _,runner=runtime_with(device,snapshot,samples,OpcUaRunner)
        await runner.start()
        try:
            async with asyncio.timeout(1):
                while runner.last_error is None:await asyncio.sleep(0)
            view=runner.diagnostics(device,asyncio.get_running_loop().time())[0]
            self.assertEqual(view['lastError'],'BAD_CONFIGURATION')
            self.assertTrue(all(samples.get_nowait().kind=='QUALITY_STATE' for _ in range(samples._queue.qsize())))
        finally:await runner.stop()

    async def test_modbus_block_retry_creates_exactly_one_final_event_per_point(self):
        _,snapshot,samples,_=setup_engine(points=2)
        p=snapshot.devices[0].points[0]
        points=tuple(replace(p,point_id='p'+str(i),poll_interval_ms=500,data_type='uint16',scale=1,offset=0,
                             modbus=replace(p.modbus,address=i,byte_order='AB')) for i in range(2))
        device=replace(snapshot.devices[0],points=points,connection=replace(snapshot.devices[0].connection,retry_count=1))
        _,runner=runtime_with(device,snapshot,samples,ModbusRunner)
        calls=[]
        async def once(reader,block):
            calls.append(block)
            if len(calls)==1:raise ReadError('BAD_TIMEOUT')
            return [10,20]
        with patch.object(ModbusReader,'_read_once',once):
            await runner.start()
            try: physical=await collect(samples,lambda s:s.kind=='PHYSICAL_RESULT',2)
            finally:await runner.stop()
        self.assertEqual(len(calls),2)
        self.assertEqual([s.value for s in physical],[10,20])
        self.assertEqual(len({s.sample_seq for s in physical}),2)
        self.assertEqual(physical[0].nominal_poll_due_mono_ms,physical[1].nominal_poll_due_mono_ms)
        self.assertTrue(all(s.completed_mono_ms>=s.nominal_poll_due_mono_ms for s in physical))
        self.assertEqual(samples.captured_events,4) # Two UNKNOWN + two final physical outcomes.

    async def test_modbus_budget_skips_count_points_without_bad_physical_samples(self):
        _,snapshot,samples,_=setup_engine(points=3)
        device=snapshot.devices[0];runtime,runner=runtime_with(device,snapshot,samples,ModbusRunner)
        entered=asyncio.Event()
        class Budget:
            @asynccontextmanager
            async def acquire(self,*_):
                entered.set();yield False
        runtime.read_budget=Budget()
        await runner.start()
        try:
            await asyncio.wait_for(entered.wait(),1)
            self.assertEqual(samples.missed_poll_cycles,3)
            captured=[samples.get_nowait() for _ in range(samples._queue.qsize())]
            self.assertTrue(all(s.kind=='QUALITY_STATE' for s in captured))
            self.assertEqual(len(captured),3)
        finally:await runner.stop()

    async def test_modbus_inflight_config_change_never_relabels_old_result(self):
        _,snapshot,samples,_=setup_engine()
        device=snapshot.devices[0];runtime,runner=runtime_with(device,snapshot,samples,ModbusRunner)
        entered=asyncio.Event();release=asyncio.Event()
        class Reader:
            def __init__(self,*_):pass
            def close(self):pass
            async def read_block(self,block,budget):
                entered.set();await release.wait()
                return {p.point_id:(42,'GOOD') for p in block.points},0
        with patch('plcnext_iot.devices.modbus.ModbusReader',Reader):
            await runner.start()
            try:
                await entered.wait()
                runtime.snapshot=replace(runtime.snapshot,config_version=11);runtime.sample_generation=2
                release.set()
                async with asyncio.timeout(1):
                    while samples.version_discarded_events==0:await asyncio.sleep(0)
                captured=[samples.get_nowait() for _ in range(samples._queue.qsize())]
                self.assertFalse(any(s.kind=='PHYSICAL_RESULT' for s in captured))
                self.assertEqual(samples.version_discarded_events,1)
                self.assertEqual(samples.undecided_lost_events,1)
            finally:await runner.stop()

    async def test_opcua_scheduled_reads_only_no_subscription_duplicates_and_real_nominal(self):
        from plcnext_iot.config.models import OpcUaAddress
        _,snapshot,samples,_=setup_engine()
        p=replace(snapshot.devices[0].points[0],poll_interval_ms=20,opcua=OpcUaAddress('ns=2;s=p'),modbus=None)
        conn=replace(snapshot.devices[0].connection,security_policy='None',security_mode='None',retry_count=0)
        device=replace(snapshot.devices[0],protocol='opcua',points=(p,),connection=conn)
        _,runner=runtime_with(device,snapshot,samples,OpcUaRunner)
        class Node:
            async def read_data_value(self):
                return SimpleNamespace(StatusCode=None,Value=SimpleNamespace(Value=10))
        class Client:
            def __init__(self,*args,**kwargs):pass
            async def connect(self):pass
            async def disconnect(self):pass
            def get_node(self,_):return Node()
            async def create_subscription(self,*args):raise AssertionError('Duplicate subscription capture')
        with patch('plcnext_iot.drivers.opcua.Client',Client):
            await runner.start()
            try:physical=await collect(samples,lambda s:s.kind=='PHYSICAL_RESULT',3)
            finally:await runner.stop()
        self.assertEqual([s.quality for s in physical],['GOOD']*3)
        q=[s.nominal_poll_due_mono_ms for s in physical]
        deltas=[b-a for a,b in zip(q,q[1:])]
        # Windows may deschedule the loop across a real slot. Verify the grid
        # and the corresponding missed-slot evidence, not wall-clock punctuality.
        self.assertTrue(all(delta>=20 and delta%20==0 for delta in deltas))
        self.assertGreaterEqual(samples.missed_poll_cycles,sum(delta//20-1 for delta in deltas))
        self.assertEqual(samples.boot_context.accounting.snapshot()['counters']['missedPollCyclesTotal'],samples.missed_poll_cycles)
        self.assertEqual(physical[0].nominal_origin_mono_ms,q[0])
        self.assertEqual(samples.captured_events,4)

    async def test_opcua_connection_retry_has_one_final_failure_per_due_point(self):
        from plcnext_iot.config.models import OpcUaAddress
        _,snapshot,samples,_=setup_engine(points=2)
        points=tuple(replace(p,opcua=OpcUaAddress('ns=2;s='+p.point_id),modbus=None) for p in snapshot.devices[0].points)
        conn=replace(snapshot.devices[0].connection,security_policy='None',security_mode='None',retry_count=1)
        device=replace(snapshot.devices[0],protocol='opcua',points=points,connection=conn)
        _,runner=runtime_with(device,snapshot,samples,OpcUaRunner)
        class Client:
            calls=0
            def __init__(self,*args,**kwargs):pass
            async def connect(self):
                Client.calls+=1;raise OSError('injected disconnect')
            async def disconnect(self):pass
        with patch('plcnext_iot.drivers.opcua.Client',Client):
            await runner.start()
            try:physical=await collect(samples,lambda s:s.kind=='PHYSICAL_RESULT',2)
            finally:await runner.stop()
        self.assertEqual(Client.calls,2)
        self.assertEqual([s.quality for s in physical],['BAD_CONNECTION']*2)
        self.assertEqual(samples.captured_events,4)

    async def test_opcua_inflight_config_switch_replans_before_reading_next_point(self):
        from plcnext_iot.config.models import OpcUaAddress
        _,snapshot,samples,_=setup_engine(points=2)
        points=tuple(replace(p,opcua=OpcUaAddress('ns=2;s='+p.point_id),modbus=None) for p in snapshot.devices[0].points)
        conn=replace(snapshot.devices[0].connection,security_policy='None',security_mode='None',retry_count=0)
        device=replace(snapshot.devices[0],protocol='opcua',points=points,connection=conn)
        runtime,runner=runtime_with(device,snapshot,samples,OpcUaRunner)
        entered=asyncio.Event();release=asyncio.Event()
        class Node:
            first=True
            async def read_data_value(self):
                if Node.first:
                    Node.first=False;entered.set();await release.wait()
                return SimpleNamespace(StatusCode=None,Value=SimpleNamespace(Value=10))
        class Client:
            def __init__(self,*args,**kwargs):pass
            async def connect(self):pass
            async def disconnect(self):pass
            def get_node(self,_):return Node()
        with patch('plcnext_iot.drivers.opcua.Client',Client):
            await runner.start()
            try:
                await entered.wait()
                runtime.sample_published_at=asyncio.get_running_loop().time()
                runtime.snapshot=replace(runtime.snapshot,config_version=11);runtime.sample_generation=2
                release.set()
                current=(await collect(samples,lambda s:s.kind=='PHYSICAL_RESULT' and s.config_version==11,1))[0]
                self.assertGreaterEqual(current.nominal_poll_due_mono_ms,round(runtime.sample_published_at*1000))
                self.assertGreaterEqual(current.nominal_origin_mono_ms,round(runtime.sample_published_at*1000))
                self.assertEqual(samples.version_discarded_events,1)
            finally:await runner.stop()

    async def test_state_monitor_creates_stale_during_a_blocked_modbus_read(self):
        _,snapshot,samples,_=setup_engine()
        p=replace(snapshot.devices[0].points[0],stale_after_ms=50)
        device=replace(snapshot.devices[0],points=(p,))
        _,runner=runtime_with(device,snapshot,samples,ModbusRunner)
        class Reader:
            def __init__(self,*_):pass
            def close(self):pass
            async def read_block(self,*_):await asyncio.Event().wait()
        with patch('plcnext_iot.devices.modbus.ModbusReader',Reader):
            await runner.start()
            try:
                stale=(await collect(samples,lambda s:s.quality=='STALE',1))[0]
                self.assertEqual(stale.kind,'QUALITY_STATE')
                self.assertIsNone(stale.nominal_poll_due_mono_ms)
                await asyncio.sleep(.1)
                self.assertTrue(samples._queue.empty())
            finally:await runner.stop()


if __name__=='__main__':unittest.main()
