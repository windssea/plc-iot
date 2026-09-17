"""Private subprocess helper for mock_device_smoke; no persistent configuration."""
import asyncio
import json
from pathlib import Path
import sys


async def serve(project):
    sys.path.insert(0, str(project))
    from iot_sim.adapters.modbus import ModbusAdapter
    from iot_sim.models import AppConfig
    adapter = ModbusAdapter()
    config = None
    values = []
    try:
        while line := await asyncio.to_thread(sys.stdin.readline):
            command = json.loads(line)
            action = command['action']
            if action == 'exit':
                break
            if action == 'start':
                if 'config' in command:
                    config = AppConfig.model_validate(command['config'])
                    values = command['values']
                await adapter.start(config)
                for sample in values:
                    await adapter.update(sample)
            elif action == 'stop':
                await adapter.stop()
            elif action == 'update':
                values = command['values']
                for sample in values:
                    await adapter.update(sample)
            else:
                raise ValueError('Unknown action')
            print(json.dumps({'ok': True, 'action': action}), flush=True)
    finally:
        await adapter.stop()


if __name__ == '__main__':
    asyncio.run(serve(Path(sys.argv[1]).resolve()))
