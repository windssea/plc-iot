"""Bounded standalone acceptance sink: commit a receipt before returning STORED."""
import hashlib
import json

from plcnext_iot.config.models import ConfigSnapshot
from plcnext_iot.config.store import StoreError
from plcnext_iot.contracts import validate_message
from plcnext_iot.messaging.messages import encode,identity
from plcnext_iot.storage.database import Database


class ReceiverStore(Database):
    def __init__(self,path,gateway,*,max_batches=10000,max_bytes=64*1024*1024):
        if any(type(v) is not int or v<1 for v in (max_batches,max_bytes)):
            raise ValueError('Receiver limits must be positive integers')
        self.max_batches,self.max_bytes=max_batches,max_bytes
        super().__init__(path,gateway,0x50524543,{
            'configs':'CREATE TABLE configs(version INTEGER PRIMARY KEY,payload BLOB NOT NULL,content_hash TEXT NOT NULL)',
            'receipts':'CREATE TABLE receipts(message_id TEXT PRIMARY KEY,payload BLOB NOT NULL,hash TEXT NOT NULL,stored_at INTEGER NOT NULL)'})

    def install_config(self,payload):
        snapshot=ConfigSnapshot.parse(payload,self.gateway)
        with self.transaction():
            row=self.c.execute('SELECT content_hash FROM configs WHERE version=?',(snapshot.config_version,)).fetchone()
            if row:
                if row[0]!=snapshot.content_hash:
                    raise ValueError('Historical configuration conflict')
                return True
            count,size=self.c.execute('SELECT count(*),coalesce(sum(length(payload)),0) FROM configs').fetchone()
            if count>=32 or size+len(payload)>64*1024*1024:
                return False
            self.c.execute('INSERT INTO configs VALUES(?,?,?)',(snapshot.config_version,payload,snapshot.content_hash))
            return True

    def receive(self,payload,now_ms):
        request=identity(payload,self.gateway)
        if request is None:
            return None
        message_id,version=request
        def ack(code=None):
            return encode({'schemaVersion':1,'gatewayId':self.gateway,'timestamp':now_ms,'messageId':message_id,
                'status':'STORED' if code is None else 'REJECTED',
                'errors':[] if code is None else [{'code':code,'path':'','message':'Batch could not be stored.'}]})
        issues=validate_message('data',payload,expected_gateway_id=self.gateway)
        if issues:
            return ack('SCHEMA_INVALID')
        wire=json.loads(payload)
        try:
            with self.transaction():
                row=self.c.execute('SELECT hash FROM receipts WHERE message_id=?',(message_id,)).fetchone()
                digest=hashlib.sha256(payload).hexdigest()
                if row:
                    return ack(None if row[0]==digest else 'VERSION_CONFLICT')
                row=self.c.execute('SELECT payload,content_hash FROM configs WHERE version=?',(version,)).fetchone()
                if row is None:
                    return ack('UNKNOWN_CONFIG_VERSION')
                snapshot=ConfigSnapshot.parse(row['payload'],self.gateway)
                if snapshot.content_hash!=row['content_hash']:
                    return ack('STORAGE_ERROR')
                devices={d.device_id:{p.point_id:p for p in d.points if p.enabled}
                         for d in snapshot.devices if snapshot.enabled and d.enabled}
                if wire['timestamp']<now_ms-7*86400*1000:
                    return ack('EXPIRED_DATA')
                if wire['timestamp']>now_ms+300000:
                    return ack('INVALID_TIMESTAMP')
                for value in wire['values']:
                    if value['deviceId'] not in devices:
                        return ack('UNKNOWN_DEVICE')
                    point=devices[value['deviceId']].get(value['pointId'])
                    if point is None:
                        return ack('UNKNOWN_POINT')
                    if value['timestamp']>now_ms+300000:
                        return ack('INVALID_TIMESTAMP')
                    if value['quality']=='GOOD' and ((point.data_type=='bool')!=(type(value['value']) is bool)):
                        return ack('SCHEMA_INVALID')
                count,size=self.c.execute('SELECT count(*),coalesce(sum(length(payload)),0) FROM receipts').fetchone()
                if count>=self.max_batches or size+len(payload)>self.max_bytes:
                    return ack('RESOURCE_LIMIT')
                self.c.execute('INSERT INTO receipts VALUES(?,?,?,?)',(message_id,payload,digest,now_ms))
            return ack()
        except (StoreError,ValueError):
            return ack('STORAGE_ERROR')

    def stats(self):
        count,size=self.c.execute('SELECT count(*),coalesce(sum(length(payload)),0) FROM receipts').fetchone()
        return {'stored':count,'payload_bytes':size,'configs':self.c.execute('SELECT count(*) FROM configs').fetchone()[0]}
