"""Read-only, serial Modbus TCP access using the pinned PyModbus API."""
import asyncio
import math
import struct

from pymodbus.client import AsyncModbusTcpClient
from pymodbus.exceptions import ConnectionException, ModbusException
from plcnext_iot.points.read_plan import ReadBlock, width


class ReadError(Exception):
    def __init__(self, quality, exception_code=None):
        self.quality = quality
        self.exception_code = exception_code
        super().__init__(quality)


def decode(point, values):
    try:
        if point.data_type == 'bool':
            if not values or type(values[0]) is not bool:
                raise ValueError('Expected bit')
            return values[0]
        formats = {'int16': 'h', 'uint16': 'H', 'int32': 'i', 'uint32': 'I', 'float32': 'f'}
        fmt = formats[point.data_type]
        width = struct.calcsize('>' + fmt)
        if len(values) * 2 != width:
            raise ValueError('Unexpected register count')
        wire = b''.join(struct.pack('>H', v) for v in values)
        order = point.modbus.byte_order
        canonical = bytes(wire[order.index(letter)] for letter in 'ABCD'[:width])
        raw = struct.unpack('>' + fmt, canonical)[0]
        result = raw * point.scale + point.offset
        if isinstance(result, float) and not math.isfinite(result):
            raise ValueError('Nonfinite value')
        return result
    except (ValueError, TypeError, KeyError, OverflowError, struct.error) as exc:
        raise ReadError('BAD_DECODE') from exc


async def _bounded(coro, seconds):
    # PyModbus 3.13.1 can convert CancelledError to ModbusIOException. Keep
    # cancellation/deadline ownership outside that task, then always reap it.
    task = asyncio.create_task(coro)
    try:
        done, _ = await asyncio.wait([task], timeout=seconds)
        if not done:
            raise TimeoutError
        return task.result()
    finally:
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)


class ModbusReader:
    def __init__(self, connection):
        self.connection = connection
        self._client = None
        self._lock = asyncio.Lock()

    def close(self):
        if self._client is not None:
            self._client.close()
            self._client = None

    async def read(self, point):
        values = await self._read_raw(ReadBlock(point.modbus.area, point.modbus.address,
            width(point), point.poll_interval_ms, (point,)))
        return decode(point, values[:width(point)])

    async def read_block(self, block, fallback_budget=4):
        """Return point outcomes and extra requests consumed by illegal-address fallback."""
        try:
            values = await self._read_raw(block)
        except ReadError as exc:
            if exc.exception_code != 2 or len(block.points) == 1:
                raise
            results, used = {}, 0
            for point in block.points:
                if used >= fallback_budget:
                    results[point.point_id] = (None, 'BAD_PROTOCOL')
                    continue
                used += 1
                try:
                    raw = await self._read_raw(ReadBlock(point.modbus.area, point.modbus.address,
                        width(point), point.poll_interval_ms, (point,)), retries=0)
                    results[point.point_id] = (decode(point, raw[:width(point)]), 'GOOD')
                except ReadError as failure:
                    results[point.point_id] = (None, failure.quality)
                    if failure.quality in ('BAD_TIMEOUT', 'BAD_CONNECTION'):
                        for remaining in block.points[used:]:
                            results[remaining.point_id] = (None, failure.quality)
                        break
            return results, used
        results = {}
        for point in block.points:
            offset = point.modbus.address - block.address
            try:
                results[point.point_id] = (decode(point, values[offset:offset + width(point)]), 'GOOD')
            except ReadError as exc:
                results[point.point_id] = (None, exc.quality)
        return results, 0

    async def _read_raw(self, block, retries=None):
        retries = self.connection.retry_count if retries is None else retries
        async with self._lock:
            for attempt in range(retries + 1):
                try:
                    return await self._read_once(block)
                except ReadError as exc:
                    if exc.quality not in ('BAD_TIMEOUT', 'BAD_CONNECTION') or attempt == retries:
                        raise
                    await asyncio.sleep(min(0.1 * 2 ** attempt, 1))

    async def _read_once(self, block):
        c = self.connection
        connecting = self._client is None or not self._client.connected
        try:
            if connecting:
                self.close()
                self._client = AsyncModbusTcpClient(c.host, port=c.port, retries=0, reconnect_delay=0,
                    timeout=max(c.connect_timeout_ms, c.request_timeout_ms) / 1000 + 1)
                if not await _bounded(self._client.connect(), c.connect_timeout_ms / 1000):
                    raise ReadError('BAD_CONNECTION')
                connecting = False
            methods = {'coil': 'read_coils', 'discrete_input': 'read_discrete_inputs',
                       'holding_register': 'read_holding_registers', 'input_register': 'read_input_registers'}
            response = await _bounded(getattr(self._client, methods[block.area])(
                block.address, count=block.count, device_id=c.unit_id), c.request_timeout_ms / 1000)
            if response.isError():
                raise ReadError('BAD_PROTOCOL', getattr(response, 'exception_code', None))
            expected_fc = {'coil': 1, 'discrete_input': 2, 'holding_register': 3, 'input_register': 4}[block.area]
            if response.function_code != expected_fc:
                raise ReadError('BAD_PROTOCOL')
            values = response.bits if expected_fc < 3 else response.registers
            if len(values) < block.count or (expected_fc >= 3 and len(values) != block.count):
                raise ReadError('BAD_PROTOCOL')
            return values
        except asyncio.CancelledError:
            self.close()
            raise
        except TimeoutError as exc:
            disconnected = self._client is None or not self._client.connected
            self.close()
            raise ReadError('BAD_CONNECTION' if connecting or disconnected else 'BAD_TIMEOUT') from exc
        except (OSError, ConnectionException) as exc:
            self.close()
            raise ReadError('BAD_CONNECTION') from exc
        except ModbusException as exc:
            disconnected = self._client is None or not self._client.connected
            self.close()
            raise ReadError('BAD_CONNECTION' if disconnected else 'BAD_PROTOCOL') from exc
        except ReadError as exc:
            if exc.quality == 'BAD_CONNECTION':
                self.close()
            raise
