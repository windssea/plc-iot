"""Protocol-dispatching runner factory. Construction is side-effect free."""
import os

from plcnext_iot.devices.runtime import UnsupportedDriver

DRIVERS = {
    'modbus-tcp': frozenset({'modbus_tcp'}),
    'opcua': frozenset({'opcua'}),
    'all': frozenset({'modbus_tcp', 'opcua'}),
}


def bind(runtime, samples, protocols):
    def create(device):
        if device.protocol == 'modbus_tcp' and 'modbus_tcp' in protocols:
            from plcnext_iot.devices.modbus import ModbusRunner
            return ModbusRunner(device, runtime, samples)
        if device.protocol == 'opcua' and 'opcua' in protocols:
            if device.connection.username and not os.environ.get(device.connection.password_env or ''):
                raise RuntimeError('OPCUA_CREDENTIAL_ENV_MISSING')
            from plcnext_iot.devices.opcua import OpcUaRunner
            return OpcUaRunner(device, runtime, samples)
        raise UnsupportedDriver(device.protocol)
    return create
