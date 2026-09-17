"""Immutable batches: reserve is not delete; only STORED acknowledges delivery."""
import hashlib
import json

from plcnext_iot.contracts import validate_message
from plcnext_iot.config.store import StoreFormatError
from plcnext_iot.storage.database import Database


class OutboxStore(Database):
    def __init__(self,path,gateway,*,max_batches=10000,max_bytes=64*1024*1024,max_dead=128):
        if any(type(v) is not int or v<1 for v in (max_batches,max_bytes,max_dead)):
            raise ValueError('Outbox limits must be positive integers')
        self.max_batches,self.max_bytes,self.max_dead=max_batches,max_bytes,max_dead
        super().__init__(path,gateway,0x504F5554,{'batches':'''CREATE TABLE batches(
            message_id TEXT PRIMARY KEY,payload BLOB NOT NULL,hash TEXT NOT NULL,
            created REAL NOT NULL,attempts INTEGER NOT NULL DEFAULT 0,next_due REAL NOT NULL DEFAULT 0,
            state TEXT NOT NULL CHECK(state IN ('PENDING','DEAD')),reason TEXT)'''})
        try:
            self.reset_inflight()
        except BaseException:
            self.close()
            raise

    def reset_inflight(self):
        with self.transaction():
            self.c.execute("UPDATE batches SET next_due=0 WHERE state='PENDING'")

    def _expire(self,now):
        self.c.execute("UPDATE batches SET state='DEAD',reason='EXPIRED_DATA' WHERE state='PENDING' AND created<=?",(now-7*86400,))
        count=self.c.execute("SELECT count(*) FROM batches WHERE state='DEAD'").fetchone()[0]
        remove=max(0,count-self.max_dead)
        if remove:
            self.c.execute("DELETE FROM batches WHERE message_id IN (SELECT message_id FROM batches WHERE state='DEAD' ORDER BY created,message_id LIMIT ?)",(remove,))
            self.c.execute('UPDATE meta SET dead_pruned=dead_pruned+? WHERE id=1',(remove,))

    def enqueue(self,payload,now):
        if validate_message('data',payload,expected_gateway_id=self.gateway):
            raise ValueError('Invalid telemetry batch')
        message_id=json.loads(payload)['messageId']
        with self.transaction():
            existing=self.c.execute('SELECT payload FROM batches WHERE message_id=?',(message_id,)).fetchone()
            if existing:
                if existing['payload']!=payload:
                    raise ValueError('Batch identity conflict')
                return True
            self._expire(now)
            count,size=self.c.execute('SELECT count(*),coalesce(sum(length(payload)),0) FROM batches').fetchone()
            if count>=self.max_batches or size+len(payload)>self.max_bytes:
                self.c.execute('UPDATE meta SET dropped=dropped+1 WHERE id=1')
                return False
            self.c.execute("INSERT INTO batches(message_id,payload,hash,created,state) VALUES(?,?,?,?,'PENDING')",
                           (message_id,payload,hashlib.sha256(payload).hexdigest(),now))
            return True

    def reserve(self,now,prefer_new):
        with self.transaction():
            self._expire(now)
            if self.c.execute("SELECT count(*) FROM batches WHERE state='PENDING' AND attempts>0 AND next_due>?",(now,)).fetchone()[0]>=8:
                return None
            order='attempts=0 DESC' if prefer_new else 'attempts>0 DESC'
            row=self.c.execute(f"SELECT * FROM batches WHERE state='PENDING' AND next_due<=? ORDER BY {order},created,message_id LIMIT 1",(now,)).fetchone()
            if row is None:
                return None
            payload=row['payload']
            if hashlib.sha256(payload).hexdigest()!=row['hash'] or validate_message('data',payload,expected_gateway_id=self.gateway):
                raise StoreFormatError('Stored telemetry batch is corrupt')
            self.c.execute('UPDATE batches SET attempts=attempts+1,next_due=? WHERE message_id=?',
                           (now+(30 if row['attempts']==0 else 60),row['message_id']))
            return payload

    def acknowledge(self,payload,now):
        if validate_message('dataAck',payload,expected_gateway_id=self.gateway):
            return False
        ack=json.loads(payload)
        with self.transaction():
            row=self.c.execute("SELECT * FROM batches WHERE message_id=? AND state='PENDING' AND attempts>0",(ack['messageId'],)).fetchone()
            if row is None:
                return False
            if ack['status']=='STORED':
                self.c.execute('DELETE FROM batches WHERE message_id=?',(ack['messageId'],))
            else:
                codes=[e['code'] for e in ack['errors']]
                if all(c in {'UNKNOWN_CONFIG_VERSION','RESOURCE_LIMIT','STORAGE_ERROR'} for c in codes):
                    self.c.execute('UPDATE batches SET next_due=? WHERE message_id=?',(now+60,ack['messageId']))
                else:
                    self.c.execute("UPDATE batches SET state='DEAD',reason=? WHERE message_id=?",(','.join(codes),ack['messageId']))
                    self._expire(now)
            return True

    def stats(self):
        row=self.c.execute("SELECT coalesce(sum(state='PENDING'),0),coalesce(sum(state='DEAD'),0),coalesce(sum(length(payload)),0) FROM batches").fetchone()
        meta=self.c.execute('SELECT dropped,dead_pruned FROM meta WHERE id=1').fetchone()
        return dict(pending=row[0],dead=row[1],payload_bytes=row[2],dropped=meta[0],dead_pruned=meta[1])
