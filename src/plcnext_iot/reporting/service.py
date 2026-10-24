"""Owned sample consumer and durable transport adapter; one DB worker thread."""
import asyncio
import json
import time
from collections import deque

from plcnext_iot.core.storage_worker import StoreWorker
from plcnext_iot.core.boot import ProcessBootContext
from plcnext_iot.reporting.engine import ReportEngine
from plcnext_iot.storage.outbox import OutboxStore
from plcnext_iot.storage.outbox_session import OutboxSession
from plcnext_iot.core.storage_worker import WorkerFull
from plcnext_iot.config.store import StoreError,StoreIdentityError
from plcnext_iot.messaging.transport import PublishRejected


class TelemetryPipeline:
    def __init__(self,runtime,samples,directory,gateway,*,on_sample=None,store_factory=None,boot_id=None,boot_context=None,
                 close_timeout=5.0,on_fact=None):
        self.runtime,self.samples=runtime,samples
        # The supervisor owns a boot's counters; each run still owns fresh policy
        # caches. An independent pipeline defaults to a fresh process identity.
        context=boot_context if boot_context is not None else (getattr(samples,'boot_context',None)
                if boot_id is None else None) or ProcessBootContext(boot_id)
        if boot_id is not None and boot_id!=context.boot_id:
            raise ValueError('Telemetry boot identity does not match its process context')
        self.engine=ReportEngine(gateway,context.boot_id,boot_context=context)
        self.context=context
        if context.outbox_session is None:
            context.outbox_session=OutboxSession(on_fact=on_fact,accounting=context.accounting)
        elif on_fact is not None:
            context.outbox_session.on_fact=on_fact
        self.session=context.outbox_session
        owner=(str(directory.resolve()),gateway)
        if (context.telemetry_owner is not None and context.telemetry_owner!=owner
                and (self.session.entries or self.session.staged)):
            raise StoreIdentityError('Pending telemetry belongs to the previous gateway/directory')
        context.telemetry_owner=owner
        factory=store_factory or (lambda path,gateway:OutboxStore(path,gateway,session=self.session))
        self.storage=StoreWorker(directory/'telemetry.db',gateway,factory,max_bytes=2*1024*1024,detachable=True)
        if close_timeout<=0:
            raise ValueError('Close timeout must be positive')
        self.close_timeout=close_timeout
        self._deadline=None
        self._persist_task=None
        self._cache=dict(pending=0,dead=0,payload_bytes=0,dropped=0,dead_pruned=0)
        self._stats_at=0
        self.enqueue_attempt_failures=0
        self.shutdown_volatile_events=0
        self.shutdown_unsettled_events=0
        self.on_sample=on_sample
        self.last_error=None
        self.final_stats=None
        self._failed=asyncio.Event()
        self._stop=asyncio.Event()
        self._task=None
        self._close_task=None

    async def start(self):
        if self._task is not None or self._stop.is_set():
            raise RuntimeError('Telemetry pipeline starts once')
        if self.context.telemetry_closing is not None and not self.context.telemetry_closing.done():
            raise RuntimeError('Previous telemetry transaction still owns storage')
        if self.context.telemetry_closing is not None and self.context.telemetry_closing.exception() is not None:
            raise StoreError('Previous telemetry close failed')
        try:
            await self.storage.open()
        except BaseException:
            await self.storage.close()
            raise
        self._configure()
        self._task=asyncio.create_task(self._run(),name='iot-reporting')
        self._persist_task=asyncio.create_task(self._persist(),name='iot-outbox-commit')

    def _accept(self,sample):
        discarded=self.engine.discarded
        try:
            self.engine.accept(sample,asyncio.get_running_loop().time())
        except Exception:
            if self.engine._identity(sample) not in self.engine._owned:
                self.samples.undecided_lost_events+=1
            self.samples.accounting_complete=False
            self.context.accounting.undecided(sample,'STORAGE_ERROR')
            raise
        if self.engine.discarded!=discarded:
            self.samples.version_discarded_events+=1
            self.samples.undecided_lost_events+=1
            self.samples.accounting_complete=False
        if self.on_sample is not None:
            try:
                self.on_sample(sample)
            except Exception:
                self.last_error='TELEMETRY_OBSERVER_FAILED'
                self.session.detail_truncated=True
                self.session.accounting_complete=False
                self._failed.set()

    def _configure(self):
        self.engine.configure(self.runtime.snapshot,asyncio.get_running_loop().time(),
                              generation=getattr(self.runtime,'sample_generation',0))

    async def _flush(self,force=False):
        now=asyncio.get_running_loop().time()
        batches=self.engine.prepare(now,time.time_ns()//1_000_000,force=force)
        groups={c:deque(b for b in batches if self.engine._classes.get(b.sequence,'new')==c) for c in ('new','history')}
        while any(groups.values()):
            if getattr(self,'_deadline',None) is not None and asyncio.get_running_loop().time()>=self._deadline:
                break
            category='new' if getattr(self.engine,'_prefer_commit_new',True) else 'history'
            if not groups[category]:
                category='history' if category=='new' else 'new'
            batch=groups[category].popleft()
            args=(batch.payload,time.time())
            if hasattr(self,'session'):
                args+= (dict(events=tuple(s.sample_seq for s in batch.samples),capture_boot=self.engine.boot_id),)
            try:
                accepted=await self.storage.call('enqueue',*args)
            except WorkerFull:
                if hasattr(self,'context'):self.context.accounting.add('enqueueRejectedAttemptsTotal')
                raise
            if accepted:
                self.engine.committed(batch,asyncio.get_running_loop().time())
                self.engine._prefer_commit_new=category!='new'

    async def _persist(self):
        try:
            while not self._stop.is_set() or self.engine._owned or (self._task is not None and not self._task.done()):
                if self._deadline is not None and asyncio.get_running_loop().time()>=self._deadline:
                    break
                try:
                    await self._flush(force=self._stop.is_set())
                    now=asyncio.get_running_loop().time()
                    if now>=self._stats_at:
                        self._cache=await self.storage.call('stats')
                        self._stats_at=now+1
                except (StoreError,WorkerFull):
                    self.enqueue_attempt_failures+=1
                await asyncio.sleep(.025)
        except Exception:
            self.last_error='TELEMETRY_STORE_FAILED';self._failed.set()
            self.context.accounting.incomplete()

    async def _run(self):
        try:
            while not self._stop.is_set():
                # Runtime clears its snapshot on shutdown; keep the last mapping
                # until the stopped producers' final queued samples are flushed.
                if self.runtime.snapshot is not None:
                    self._configure()
                try:
                    sample=await asyncio.wait_for(self.samples.get(),0.05)
                except TimeoutError:
                    sample=None
                if sample is not None:
                    # A config transaction may have committed during the await.
                    if self.runtime.snapshot is not None:
                        self._configure()
                    self._accept(sample)
            drained=0
            while True:
                try:
                    self._accept(self.samples.get_nowait())
                    drained+=1
                    if drained%256==0:
                        await asyncio.sleep(0)
                except asyncio.QueueEmpty:
                    break
        except Exception:
            self.last_error='TELEMETRY_REPORT_FAILED'
            self._failed.set()
            self.context.accounting.incomplete()

    async def send_loop(self,connection,topic):
        watermark=self.context._sample_sequence
        self.engine.open_gate(watermark)
        await self.storage.call('open_gate',watermark)
        publishes=set()
        try:
            while not self._stop.is_set() and connection.connected:
                for task in list(publishes):
                    if task.done():
                        publishes.remove(task);task.result()
                if len(publishes)<8:
                    try:
                        payload=await self.storage.call('reserve',time.time())
                    except (StoreError,WorkerFull):
                        payload=None
                    if payload is not None:
                        key=json.loads(payload)['messageId']
                        # Rate is also charged at the actual SDK handoff. A slow
                        # transaction or event-loop pause cannot create a burst.
                        while True:
                            now=asyncio.get_running_loop().time()
                            while self.session.sdk_rate and now-self.session.sdk_rate[0][0]>=1:
                                self.session.sdk_rate.popleft()
                            if (now>=self.session.sdk_next_send and len(self.session.sdk_rate)<4
                                    and sum(size for _,size in self.session.sdk_rate)+len(payload)<=512*1024):
                                break
                            await asyncio.sleep(.025)
                        if self._stop.is_set() or not connection.connected:
                            await self.storage.call('submission_failed',key)
                            break
                        try:
                            self.session.sdk_next_send=now+.25
                            self.session.sdk_rate.append((now,len(payload)))
                            # No await between recheck and SDK acceptance.
                            if hasattr(connection,'submit_publish'):
                                info=connection.submit_publish(topic,payload)
                                self.context.accounting.record('sdk_accepted',messageId=key)
                                task=asyncio.create_task(connection.wait_puback(info))
                            else:
                                task=asyncio.create_task(connection.publish(topic,payload))
                            publishes.add(task)
                        except PublishRejected:
                            await self.storage.call('submission_failed',key)
                            raise
                await asyncio.sleep(.025)
        finally:
            for task in publishes:
                task.cancel()
            await asyncio.gather(*publishes,return_exceptions=True)

    async def acknowledge(self,payload):
        try:
            return await self.storage.call('acknowledge',payload,time.time())
        except (StoreError,WorkerFull):
            self.enqueue_attempt_failures+=1
            return False

    async def stats(self):
        # This local snapshot never queues a DB operation behind a slow write.
        # It is not the I05 atomic diagnostics protocol; cache age is explicit.
        return dict(self._cache,sample_dropped=self.samples.dropped,
                    version_discarded=self.engine.discarded,cache_reset_points=self.engine.cache_reset_points,
                    capture_events=self.samples.captured_events,capture_overflow_events=self.samples.capture_overflow_events,
                    undecided_lost_events=self.samples.undecided_lost_events,
                    missed_poll_cycles=self.samples.missed_poll_cycles,
                    capture_accounting_complete=self.samples.accounting_complete,
                    eligible_events=self.engine.eligible_events,policy_filtered_events=self.engine.policy_filtered_events,
                    time_invalid_events=self.engine.time_invalid_events,known_unsent_events=self.engine.known_unsent_events,
                    missed_report_slots=self.engine.missed_report_slots,selected_pending_events=len(self.engine._owned),
                    ingress_bytes=self.samples.used_bytes,selection_bytes=self.engine.used_bytes,
                    worker_jobs=self.storage.jobs,worker_bytes=self.storage.used_bytes,
                    enqueue_attempt_failures=self.enqueue_attempt_failures,
                    shutdown_volatile_events=self.shutdown_volatile_events,
                    shutdown_unsettled_events=self.shutdown_unsettled_events,
                    outbox_stats_cached=True,diagnostics=self.diagnostics_snapshot())

    def diagnostics_snapshot(self):
        if not self.session.accounting_complete:self.context.accounting.incomplete()
        if self.session.detail_truncated:self.context.accounting.incomplete(truncated=True)
        if self.session.counter_overflow:self.context.accounting.incomplete(overflow=True)
        return dict(self.context.accounting.snapshot(),gatewayId=self.engine.gateway,bootId=self.context.boot_id,
                    timeMode='SYSTEM_RTC_COMPAT_UNVERIFIED')

    async def wait_failed(self):
        await self._failed.wait()

    async def close(self):
        if self._close_task is None:
            self._stop.set()
            self._close_task=asyncio.create_task(self._close())
        await asyncio.shield(self._close_task)

    async def _close(self):
        self._deadline=asyncio.get_running_loop().time()+self.close_timeout
        cleanup=asyncio.create_task(self._finish_close(),name='iot-outbox-close')
        self.context.telemetry_closing=cleanup
        try:
            await asyncio.wait_for(asyncio.shield(cleanup),self.close_timeout)
        except TimeoutError:
            # A thread may still commit. Do not declare these events lost or
            # durable, drop their owner, unlock the DB, or start a second writer.
            self.last_error='TELEMETRY_CLOSE_UNSETTLED'
            self.shutdown_unsettled_events=len(self.engine._owned)
            self.session.accounting_complete=False
        except Exception:
            self.last_error='TELEMETRY_CLOSE_FAILED'
            self.session.accounting_complete=False
        self.final_stats=await self.stats()

    async def _finish_close(self):
        if self._task is not None:
            await self._task
        # A failed ingress task must not silently strand its undecided input.
        drained=0
        while not self.samples._queue.empty():
            sample=self.samples.get_nowait()
            try:
                self._accept(sample)
            except Exception:
                pass # _accept records the undecided failure before propagating.
            drained+=1
            if drained%256==0:
                await asyncio.sleep(0)
        if self._persist_task is not None:
            await self._persist_task
        if self.session.on_fact is not None:
            owned=list(self.engine._owned.values())
            for start in range(0,len(owned),256):
                try:
                    result=self.session.on_fact(dict(kind='ram_terminal',source='VOLATILE_RESTART',outcome='known_unsent',
                        capture_boot=self.engine.boot_id,sample_seqs=tuple(s.sample_seq for s in owned[start:start+256])))
                    if result is False:
                        self.session.detail_truncated=True
                except Exception:
                    self.session.detail_truncated=True
        self.shutdown_volatile_events=self.engine.abandon()
        if self.shutdown_volatile_events:
            self.last_error='TELEMETRY_CLOSE_VOLATILE'
            self.session.add_fact('known_unsent',self.shutdown_volatile_events)
            self.session.accounting_complete=False
        self.session.staged.clear()
        try:
            self._cache=await self.storage.call('stats')
        finally:
            await self.storage.close()
