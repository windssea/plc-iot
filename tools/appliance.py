"""First-boot local setup followed by the existing IoT Agent lifecycle.

The setup page stays reachable while the agent runs, so a saved revision can be
applied without leaving the process: the agent is restarted in place, the
container and the setup port are untouched.
"""
import argparse
import asyncio
import json
from pathlib import Path
import signal
import ssl
import uuid

from tools import _source_path  # noqa: F401
from tools.agent import run
from plcnext_iot.config.locking import StoreLock
from plcnext_iot.provisioning.store import SetupStore
from plcnext_iot.provisioning.server import SetupServer


RELOAD_POLL_SECONDS = 0.2
RELOAD_SETTLE_SECONDS = 0.5


async def _watch_reload(stop, server, seq, run_stop):
    """Ask the current run to end once the record changes, or on shutdown."""
    while not run_stop.is_set():
        try:
            await asyncio.wait_for(stop.wait(), RELOAD_POLL_SECONDS)
        except TimeoutError:
            pass
        if stop.is_set() or server.change_seq != seq:
            run_stop.set()
            return


async def _supervise(store, server, stop):
    """Run the agent, restarting it only when a saved revision asks for it.

    The record is the sole trigger: a revision that no one saved never restarts
    anything, so a deterministic startup failure still ends the process (leaving
    the restart decision to the container's supervisor) instead of spinning.
    """
    boot_id = 'boot-' + uuid.uuid4().hex
    while True:
        value = server.value
        if value is None:
            print(json.dumps(dict(event='agent_unsupervised', code='NOT_CONFIGURED')), flush=True)
            return 1
        seq = server.change_seq
        try:
            store.write_ca(value)
            settings, mqtt = store.settings(value)
        except (ValueError, OSError):
            # An unusable record must not take down the process serving the page.
            print(json.dumps(dict(event='agent_reload_blocked', code='INVALID_SETTINGS', seq=seq)), flush=True)
            while not stop.is_set() and server.change_seq == seq:
                await asyncio.sleep(RELOAD_POLL_SECONDS)
            if stop.is_set():
                return 0
            continue
        server.applied_seq = seq
        print(json.dumps(dict(event='agent_applied', seq=seq)), flush=True)
        run_stop = asyncio.Event()
        watcher = asyncio.create_task(_watch_reload(stop, server, seq, run_stop), name='iot-reload-watch')
        try:
            code = await run(settings, [], None, 'all', mqtt, True, run_stop, boot_id)
        finally:
            watcher.cancel()
            await asyncio.gather(watcher, return_exceptions=True)
        if stop.is_set() or server.change_seq == seq:
            return code
        # Let a burst of saves settle, then take the latest revision: the pages
        # always hold the newest record, so the skipped ones need no replay.
        await asyncio.sleep(RELOAD_SETTLE_SECONDS)
        if stop.is_set():
            return code
        print(json.dumps(dict(event='agent_reloading', reason='config_changed',
                              seq=server.change_seq)), flush=True)


async def serve(directory, host, port, tls=None):
    store = SetupStore(directory)
    lock = StoreLock(store.directory/'commissioning.lock')
    server = None
    previous = {}
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    waits = []
    try:
        value = store.load()
        server = SetupServer(store, value)
        await server.start(host, port, tls)
        for sig in (signal.SIGINT, signal.SIGTERM):
            previous[sig] = signal.signal(sig, lambda *_: loop.call_soon_threadsafe(stop.set))
        print(json.dumps(dict(event='setup_ready', configured=value is not None,
                              port=server.server.sockets[0].getsockname()[1])), flush=True)
        waits = [asyncio.create_task(stop.wait()), asyncio.create_task(server.ready.wait())]
        await asyncio.wait(waits, return_when=asyncio.FIRST_COMPLETED)
        for task in waits:
            task.cancel()
        await asyncio.gather(*waits, return_exceptions=True)
        waits = []
        if stop.is_set():
            return 0
        return await _supervise(store, server, stop)
    finally:
        stop.set()
        for task in waits:
            task.cancel()
        await asyncio.gather(*waits, return_exceptions=True)
        if server is not None:
            await server.close()
        for sig, handler in previous.items():
            signal.signal(sig, handler)
        lock.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--data-directory',type=Path,default=Path('/var/lib/plcnext-iot'))
    parser.add_argument('--listen',default='127.0.0.1')
    parser.add_argument('--port',type=int,default=8080)
    parser.add_argument('--cert',type=Path)
    parser.add_argument('--key',type=Path)
    args = parser.parse_args()
    if not 0 <= args.port <= 65535 or bool(args.cert) != bool(args.key):
        parser.error('Invalid port or TLS certificate/key pair')
    tls = None
    if args.cert:
        tls=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        tls.load_cert_chain(args.cert,args.key)
    try:
        return asyncio.run(serve(args.data_directory,args.listen,args.port,tls))
    except (OSError, ValueError, RuntimeError):
        print(json.dumps(dict(event='appliance_failed',code='STARTUP_OR_STORAGE_ERROR')),flush=True)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
