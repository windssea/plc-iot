"""Deterministic platform clock tests: no database, network, or device."""
import json
import sys
from pathlib import Path
import unittest

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from plcnext_iot.core.platform_time import PlatformTime,TimeUnavailable,MAX_SAFE

BASE=1_792_000_000_000


class Clock:
    def __init__(self):self.ms=100000;self.wall=BASE
    def mono(self):return self.ms
    def system(self):return self.wall
    def advance(self,ms):self.ms+=ms;self.wall+=ms


def provider(clock=None,boot='boot-one'):
    clock=clock or Clock()
    p=PlatformTime('PLC-01',boot,basis='site-hub-1',drift_ppm=50,drift_evidence='TEST_FIXTURE_ONLY',
                   mono=clock.mono,system=clock.system,jitter=lambda a,b:(a+b)/2)
    p.connect('session-one')
    return clock,p


def respond(p,c,*,offset=0,rtt=20,processing=2,retained=False,changes=None,dispatch=0):
    raw=p.request()
    assert raw is not None
    req=json.loads(raw)
    t2=BASE+(req['clientMonoMs']-100000)+(rtt-processing)/2+offset
    c.advance(rtt)
    msg=dict(req,serverReceivedAt=int(t2),serverSentAt=int(t2+processing))
    if changes:msg.update(changes)
    m4=c.ms;c.advance(dispatch)
    return p.response(json.dumps(msg).encode(),received_mono_ms=m4,session=p.session,retained=retained)


def sync(p,c,offset=0):
    for _ in range(3):assert respond(p,c,offset=offset)
    assert p.snapshot().time_valid


def ready(p):
    p.control_submitted('heartbeat',p.token())
    p.control_submitted('config/get',p.token())


class PlatformClockTests(unittest.TestCase):
    def test_decreasing_monotonic_clock_cannot_bypass_history_with_resync(self):
        c,p=provider();sync(p,c);ready(p);token=p.token()
        c.ms-=1
        self.assertFalse(p.snapshot().time_valid)
        self.assertEqual(p.state,'MAINTENANCE')
        self.assertIsNone(p.request())
        self.assertFalse(p.valid(token,'ENQUEUE'))
        self.assertTrue(p.valid(None,'ACK_APPLY'))

    def test_cold_start_and_two_control_submissions_are_separate_gates(self):
        c,p=provider()
        self.assertIsNone(p.read(c.ms).utc_ms)
        self.assertTrue(p.valid(None,'CAPTURE'))
        self.assertFalse(p.valid(None,'ENQUEUE'))
        self.assertTrue(p.valid(None,'ACK_APPLY'))
        sync(p,c)
        self.assertTrue(p.valid(None,'ENQUEUE'))
        self.assertTrue(p.valid(None,'CONTROL_SEND'))
        self.assertFalse(p.valid(None,'SEND'))
        p.control_submitted('config/get',p.token())
        self.assertFalse(p.valid(None,'SEND'))
        ready(p)
        self.assertTrue(p.valid(None,'SEND'))
        with self.assertRaises(TimeUnavailable):p.require(None,'ENQUEUE')

    def test_reconnect_retains_anchor_but_requires_current_session_recheck(self):
        c,p=provider();sync(p,c);ready(p)
        token=p.token();revision=p.revision
        p.connect('session-two')
        self.assertTrue(p.snapshot().time_valid)
        self.assertTrue(p.valid(None,'ENQUEUE'))
        self.assertFalse(p.valid(token,'ENQUEUE'))
        self.assertFalse(p.valid(None,'CONTROL_SEND'))
        sync(p,c);self.assertEqual(p.revision,revision)
        self.assertFalse(p.valid(None,'SEND'));ready(p);self.assertTrue(p.valid(None,'SEND'))

    def test_utc_and_retry_clock_are_independent_of_system_wall_jumps(self):
        c,p=provider();sync(p,c)
        old=p.snapshot().utc_ms
        c.wall-=86400*1000;c.ms+=1000
        self.assertEqual(p.snapshot().utc_ms,old+1000)
        c.wall+=10*86400*1000
        self.assertEqual(p.snapshot().utc_ms,old+1000)

    def test_slew_never_reverses_and_preserves_revision_for_small_change(self):
        c,p=provider();sync(p,c);rev=p.revision
        c.advance(300000)
        sync(p,c,offset=-200)
        old=p.snapshot().utc_ms;c.advance(1000);new=p.snapshot().utc_ms
        self.assertGreaterEqual(new-old,995);self.assertLessEqual(new-old,1000)
        self.assertEqual(p.revision,rev)

    def test_expiry_invalidates_tokens_and_never_blocks_existing_ack_application(self):
        c,p=provider();sync(p,c);ready(p);token=p.token()
        c.advance(3_600_000)
        self.assertFalse(p.snapshot().time_valid)
        self.assertEqual(p.state,'EXPIRED')
        self.assertFalse(p.valid(token,'SEND'))
        self.assertTrue(p.valid(None,'ACK_APPLY'))
        self.assertIsNone(p.read(c.ms).utc_ms)

    def test_stale_malformed_retained_and_wrong_echo_do_not_change_anchor(self):
        for kwargs in ({'retained':True},{'changes':{'sessionId':'other'}},{'changes':{'clientMonoMs':True}},
                       {'changes':{'unknown':0}},{'processing':501},{'rtt':2000},{'dispatch':2000}):
            c,p=provider()
            self.assertFalse(respond(p,c,**kwargs),kwargs)
            self.assertFalse(p.snapshot().time_valid)

    def test_negative_path_tolerance_and_derived_safe_range(self):
        c,p=provider()
        self.assertTrue(respond(p,c,rtt=20,processing=40))
        c,p=provider();self.assertFalse(respond(p,c,rtt=20,processing=100))
        c,p=provider();self.assertFalse(respond(p,c,changes={'serverReceivedAt':MAX_SAFE-1,'serverSentAt':MAX_SAFE}))

    def test_round_has_ten_attempt_limit_and_monotonic_backoff(self):
        c,p=provider()
        for _ in range(10):
            self.assertIsNotNone(p.request());self.assertIsNone(p.request());c.advance(2000)
        self.assertIsNone(p.request());self.assertFalse(p.snapshot().time_valid)
        self.assertEqual(p.reason,'TIMEOUT')
        c.advance(5500);self.assertIsNotNone(p.request())

    def test_single_good_sample_only_bootstraps_at_bounded_round_end_and_rechecks_early(self):
        c,p=provider();respond(p,c)
        self.assertFalse(p.snapshot().time_valid)
        c.advance(30000)
        self.assertIsNone(p.request())
        self.assertTrue(p.snapshot().time_valid)
        c.advance(9999);self.assertIsNone(p.request())
        c.advance(1);self.assertIsNotNone(p.request())

    def test_inconsistent_samples_and_known_history_rollback_do_not_open_data(self):
        c,p=provider()
        for offset in (0,1000,2000):respond(p,c,offset=offset)
        self.assertFalse(p.snapshot().time_valid)
        self.assertEqual(p.reason,'INCONSISTENT_SAMPLES')
        c,p=provider();p.load_history_boundary('site-hub-1',BASE+10000)
        for _ in range(3):respond(p,c)
        self.assertEqual(p.state,'MAINTENANCE')
        self.assertIsNone(p.request());self.assertFalse(p.valid(None,'SEND'))

    def test_old_process_monotonic_and_changed_basis_do_not_authenticate_history(self):
        c,p=provider();p.load_history_boundary('other-hub',BASE)
        self.assertEqual(p.state,'MAINTENANCE')
        c,p=provider();sync(p,c);token=p.token()
        _,fresh=provider(c,boot='boot-two')
        self.assertFalse(fresh.valid(token,'ENQUEUE'))

    def test_declares_compatibility_without_claiming_platform_synchronization(self):
        c=Clock();p=PlatformTime('PLC-01','boot-one',mode='SYSTEM_RTC',mono=c.mono,system=c.system)
        p.connect('session-one')
        self.assertFalse(p.snapshot().time_valid)
        self.assertIsNone(p.snapshot().utc_ms)
        self.assertEqual(p.read(c.ms).utc_ms,c.wall)
        self.assertEqual(p.read(c.ms).source,'SYSTEM_RTC_COMPAT')


if __name__=='__main__':unittest.main()
