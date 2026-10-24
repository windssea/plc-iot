"""Bounded independent JSONL evidence; never waits for disk on the capture path."""
import json
from pathlib import Path
import queue
import threading


class AuditRecorder:
    def __init__(self,path,*,queue_size=512,max_records=100000,max_bytes=64*1024*1024):
        if any(type(v) is not int or v<1 for v in (queue_size,max_records,max_bytes)):
            raise ValueError('Audit bounds must be positive integers')
        self.path=Path(path)
        self.queue=queue.Queue(queue_size)
        self.max_records,self.max_bytes=max_records,max_bytes
        self.accepted=self.written=self.used_bytes=self.rejected=0
        self.failed=False;self.lock=threading.Lock();self.stop=threading.Event()
        # Exclusive creation on the caller before accepting any facts; never
        # overwrite earlier evidence. File I/O after this point is worker-owned.
        self.output=self.path.open('xb')
        self.thread=threading.Thread(target=self._write,name='iot-audit',daemon=True)
        self.thread.start()

    def record(self,event):
        try:
            if (type(event) is not dict or len(event)>32 or any(type(k) is not str or len(k)>64 or
                (v is not None and type(v) not in (str,int,float,bool)) or
                (type(v) is str and len(v)>256) for k,v in event.items())):
                raise ValueError('Unbounded audit fact')
            raw=(json.dumps(event,allow_nan=False,separators=(',',':'))+'\n').encode()
        except (ValueError,TypeError):
            with self.lock:self.rejected+=1
            return False
        with self.lock:
            if (self.stop.is_set() or self.failed or len(raw)>8192 or self.accepted>=self.max_records
                    or self.used_bytes+len(raw)>self.max_bytes):
                self.rejected+=1;return False
            try:self.queue.put_nowait(raw)
            except queue.Full:self.rejected+=1;return False
            self.accepted+=1;self.used_bytes+=len(raw)
        return True

    def _write(self):
        try:
            with self.output as stream:
                while not self.stop.is_set() or not self.queue.empty():
                    try:raw=self.queue.get(timeout=.05)
                    except queue.Empty:continue
                    stream.write(raw)
                    with self.lock:self.written+=1
        except OSError:
            with self.lock:self.failed=True

    def close(self,timeout=2):
        self.stop.set();self.thread.join(timeout)
        with self.lock:
            self.failed|=self.thread.is_alive() or self.accepted!=self.written
            return dict(accepted=self.accepted,written=self.written,rejected=self.rejected,
                accountingComplete=not self.failed and not self.rejected and not self.thread.is_alive() and self.accepted==self.written)
