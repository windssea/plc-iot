"""Unit tests for Phase 3 capability-negotiated v2 protocol, revision clocks, ACK queue, and jittered backoff."""
import json
from pathlib import Path
import unittest

from plcnext_iot.messaging.v2_protocol import (
    StateEventAckQueue,
    V2_CAPABILITY_TOKENS,
    V2RevisionClock,
    backoff_with_jitter,
    build_data_sample_entry,
    build_device_status_message,
    build_heartbeat_message,
    negotiate_capabilities,
)


class V2ScaleProtocolTests(unittest.TestCase):
    def test_v2_schema_file_parses_and_defines_required_message_types(self):
        schema_path = Path(__file__).resolve().parents[1] / 'contracts' / 'v2' / 'protocol.schema.json'
        schema = json.loads(schema_path.read_text(encoding='utf-8'))
        defs = schema['$defs']
        for name in ('heartbeatV2', 'deviceStatusV2', 'stateEventAckV2', 'dataV2'):
            self.assertIn(name, defs)
        self.assertIn('stateRevision', defs['deviceStatusV2']['properties']['device']['required'])
        self.assertIn('heartbeatRevision', defs['heartbeatV2']['required'])
        self.assertIn('collectionEpoch', defs['dataV2']['properties']['values']['items']['required'])
        self.assertIn('sourceSampleSequence', defs['dataV2']['properties']['values']['items']['required'])

    def test_capability_negotiation_falls_back_to_v1_when_peer_lacks_v2_tokens(self):
        clock = V2RevisionClock(boot_id='boot-1')
        v1_caps = negotiate_capabilities(V2_CAPABILITY_TOKENS, {'devicePatchV1'})
        self.assertEqual(v1_caps, frozenset())

        hb_v1 = build_heartbeat_message(
            gateway_id='GW-1', session_id='s1', agent_state='RUNNING',
            active_config_version=3, timestamp_ms=1700000000000, capabilities=v1_caps, clock=clock,
        )
        self.assertEqual(hb_v1['schemaVersion'], 1)
        self.assertNotIn('heartbeatRevision', hb_v1)

        dev_v1 = build_device_status_message(
            gateway_id='GW-1', session_id='s1', config_version=3, device_id='m-1',
            status='ONLINE', timestamp_ms=1700000000000, capabilities=v1_caps, clock=clock,
        )
        self.assertEqual(dev_v1['schemaVersion'], 1)
        self.assertNotIn('stateRevision', dev_v1['device'])

    def test_v2_revisions_and_collection_epoch_are_strictly_monotonic_even_at_same_ms(self):
        clock = V2RevisionClock(boot_id='boot-2')
        caps = negotiate_capabilities(V2_CAPABILITY_TOKENS, V2_CAPABILITY_TOKENS)
        self.assertEqual(caps, V2_CAPABILITY_TOKENS)

        # Same millisecond ONLINE -> OFFLINE gets strictly increasing stateRevision.
        d1 = build_device_status_message(
            gateway_id='GW-1', session_id='s1', config_version=5, device_id='m-1',
            status='ONLINE', timestamp_ms=1700000001000, capabilities=caps, clock=clock,
        )
        d2 = build_device_status_message(
            gateway_id='GW-1', session_id='s1', config_version=5, device_id='m-1',
            status='OFFLINE', timestamp_ms=1700000001000, capabilities=caps, clock=clock,
        )
        self.assertEqual(d1['schemaVersion'], 2)
        self.assertEqual(d1['device']['stateRevision'], 1)
        self.assertEqual(d2['device']['stateRevision'], 2)
        self.assertEqual(d2['stateEventId'], 'se-m-1-2')

        # Re-enable bumps collectionEpoch so pre-enable cached samples cannot masquerade as new evidence.
        s_before = build_data_sample_entry(
            device_id='m-1', point_id='p1', value=12.3, quality='GOOD',
            timestamp_ms=1700000001000, capabilities=caps, clock=clock,
        )
        new_epoch = clock.advance_collection_epoch('m-1')
        s_after = build_data_sample_entry(
            device_id='m-1', point_id='p1', value=12.4, quality='GOOD',
            timestamp_ms=1700000001000, capabilities=caps, clock=clock,
        )
        self.assertEqual(new_epoch, 2)
        self.assertEqual(s_before['collectionEpoch'], 1)
        self.assertEqual(s_after['collectionEpoch'], 2)
        self.assertEqual(s_before['sourceSampleSequence'], 1)
        self.assertEqual(s_after['sourceSampleSequence'], 2)

        clock.bump_clock_revision('STEPPED')
        hb = build_heartbeat_message(
            gateway_id='GW-1', session_id='s1', agent_state='RUNNING',
            active_config_version=5, timestamp_ms=1700000002000, capabilities=caps, clock=clock,
        )
        self.assertEqual(hb['heartbeatRevision'], 1)
        self.assertEqual(hb['clockRevision'], 1)
        self.assertEqual(hb['timeQuality'], 'STEPPED')

    def test_state_event_ack_queue_is_bounded_and_idempotent(self):
        q = StateEventAckQueue(max_entries=2)
        e1 = q.enqueue('m-1', 1, {'status': 'OFFLINE'}, now_ms=1000)
        e1_dup = q.enqueue('m-1', 1, {'status': 'OFFLINE'}, now_ms=1001)
        self.assertEqual(e1, e1_dup)
        q.enqueue('m-2', 1, {'status': 'DISABLED'}, now_ms=1002)
        q.enqueue('m-3', 1, {'status': 'OFFLINE'}, now_ms=1003)
        self.assertEqual(len(q), 2)
        self.assertEqual(q.dropped_oldest, 1)
        self.assertFalse(q.ack(e1.state_event_id))
        self.assertTrue(q.ack('se-m-2-1'))
        self.assertEqual(len(q), 1)

    def test_backoff_with_jitter_desynchronizes_multiple_failing_meters(self):
        delays = [backoff_with_jitter(3, device_key=f'meter-{i}') for i in range(30)]
        self.assertGreater(len(set(round(d, 4) for d in delays)), 20)
        self.assertTrue(all(0.1 <= d <= 30.0 for d in delays))
        self.assertEqual(backoff_with_jitter(10, base_s=0.5, cap_s=30.0, jitter_ratio=0), 30.0)


if __name__ == '__main__':
    unittest.main()
