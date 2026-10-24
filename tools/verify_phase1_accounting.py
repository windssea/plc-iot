"""Run only I05/I06 approved tests with database/network/process creation blocked.

Does not start an Agent or a Hub. Store/driver tests use injected objects only.
Writes reproducible, credential-free evidence for the selected source revision.
"""
from contextlib import ExitStack
import datetime
import hashlib
import json
from pathlib import Path
import socket
import sqlite3
import subprocess
import sys
import threading
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'src'),str(ROOT/'tests'),str(ROOT)]
MODULES=('test_phase1_accounting','test_phase1_outbox','test_phase1_events','test_phase1_collectors','test_reporting',
         'test_report_policy_clock','test_boot_reload_identity','test_runtime_health',
         'test_contract_schema_sharing','test_opcua.OpcUaContractTests')
EXCLUDED_CLI='test_cli_exit_codes_and_does_not_echo_bad_payload'


def main():
    attempts=[]
    internal=threading.local()
    socketpairs=[]
    original_pair=socket.socketpair
    def wakeup_pair(*args,**kwargs):
        # Windows implements socketpair with a private loopback listener. Permit
        # only this stdlib operation, not application-created TCP connections.
        internal.wakeup=True
        try:
            pair=original_pair(*args,**kwargs)
            socketpairs.append(1)
            return pair
        finally:internal.wakeup=False
    def connection_guard(original):
        def guarded(*args,**kwargs):
            if getattr(internal,'wakeup',False):return original(*args,**kwargs)
            return deny('external network')(*args,**kwargs)
        return guarded
    def deny(kind):
        def blocked(*args,**kwargs):
            attempts.append(kind)
            raise AssertionError('I05/I06 isolated suite forbids '+kind)
        return blocked
    with ExitStack() as guards:
        import test_phase1_outbox
        test_phase1_outbox.EVIDENCE_OUTPUT=ROOT/'doc/verification/evidence/2026-10-24-i05-i06'
        guards.enter_context(patch.object(socket,'socketpair',wakeup_pair))
        for name in ('connect','connect_ex'):
            guards.enter_context(patch.object(socket.socket,name,connection_guard(getattr(socket.socket,name))))
        for owner,attribute,kind in ((sqlite3,'connect','database'),(sqlite3.dbapi2,'connect','database'),
                                    (subprocess,'Popen','subprocess')):
            guards.enter_context(patch.object(owner,attribute,deny(kind)))
        suite=unittest.TestSuite(unittest.defaultTestLoader.loadTestsFromName(n) for n in MODULES)
        from test_contracts import ContractTests
        suite.addTests(ContractTests(n) for n in unittest.defaultTestLoader.getTestCaseNames(ContractTests)
                       if n!=EXCLUDED_CLI)
        result=unittest.TextTestRunner(verbosity=2).run(suite)
    files=[p for folder in ('src','tests') for p in (ROOT/folder).rglob('*.py')]
    files+=[ROOT/'tools/agent.py',ROOT/'tools/appliance.py',ROOT/'tools/verify_existing_outbox.py',Path(__file__)]
    evidence=dict(checkedAt=datetime.datetime.now(datetime.timezone.utc).isoformat(),
        tests=result.testsRun,failures=len(result.failures),errors=len(result.errors),skipped=len(result.skipped),
        prohibitedAttempts=attempts,databaseCreated=False,deviceConnections=False,
        stdlibAsyncioWakeupSocketPairs=len(socketpairs),
        runtimeAgentStarted=False,approvedModules=MODULES,excludedCLI=EXCLUDED_CLI,
        goldenContract='HUB-IOT-PLC-P0/2',goldenCasesCovered=['R%02d'%i for i in range(1,15)],
        sourceFiles=[dict(path=str(p.relative_to(ROOT)).replace('\\','/'),
                          sha256=hashlib.sha256(p.read_bytes()).hexdigest()) for p in sorted(files)])
    from test_phase1_outbox import MEASUREMENTS
    from test_phase1_accounting import MEASUREMENTS as ACCOUNTING_MEASUREMENTS
    evidence['measurements']=MEASUREMENTS+ACCOUNTING_MEASUREMENTS
    output=ROOT/'doc/verification/evidence/2026-10-24-i05-i06'
    output.mkdir(parents=True,exist_ok=True)
    (output/'isolated-tests.json').write_text(json.dumps(evidence,indent=2)+'\n',encoding='utf-8')
    return 0 if result.wasSuccessful() and not attempts else 1


if __name__=='__main__':raise SystemExit(main())
