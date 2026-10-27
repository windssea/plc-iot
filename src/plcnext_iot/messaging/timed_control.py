"""A synchronous SDK handoff is the time-token linearization point, not PUBACK."""
from plcnext_iot.core.platform_time import TimeUnavailable


async def publish_control(connection, topic, factory, provider=None, *, kind=None, retain=False):
    token=provider.token() if provider is not None else None
    current=provider.require(token,'CONTROL_SEND') if provider is not None else None
    payload=factory(current.utc_ms if current.time_valid else current.system_utc_ms) if current is not None else factory(None)
    if provider is not None:
        provider.require(token,'CONTROL_SEND')
    if hasattr(connection,'submit_publish'):
        info=connection.submit_publish(topic,payload,retain=retain)
        if provider is not None and kind is not None:
            provider.control_submitted(kind,token)
        await connection.wait_puback(info)
    else:
        if provider is not None and provider.platform:
            raise TimeUnavailable('Platform control requires synchronous SDK acceptance')
        await connection.publish(topic,payload,retain=retain)
        if provider is not None and kind is not None:
            provider.control_submitted(kind,token)
