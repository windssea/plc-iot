import asyncio
import json
import os
from pathlib import Path
import re
import sys
import tempfile
import unittest

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))
from plcnext_iot.provisioning.store import FIELDS, SetupStore, parse
from plcnext_iot.provisioning.server import SetupServer


# Self-signed test CA (public certificate only; the key was discarded). Real
# material is required because the settings validator parses it before saving.
TEST_CA="""-----BEGIN CERTIFICATE-----
MIIDHzCCAgegAwIBAgIUB1XSfxh8ouNpHfzuIc0O7ZUyBUMwDQYJKoZIhvcNAQEL
BQAwHjEcMBoGA1UEAwwTcGxjbmV4dC1pb3QtdGVzdC1jYTAgFw0yNjA5MTcwNzU1
NTFaGA8yMTI2MDgyNDA3NTU1MVowHjEcMBoGA1UEAwwTcGxjbmV4dC1pb3QtdGVz
dC1jYTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBALYRgyDEQhNHrA5O
MEB5uz0jzXPO6zrEoeW/mWpzKTHBGJxRzcKtMn4OYV1cgYGG6oIv7xWVkXjRLx/S
HCr8aT/oS/w2vne2Jd+UL+1iQWKf+Dk3I/LT4vJ0cTzA37Ejv3+ljt4z5O4lc3gA
8c8Yo7OHoiBnnfSk4p2E9AgxKzYypQ/f4Nr+37uHbj21VFI3GLiRd3Dgxz7txail
LIxRAS0TmV0f6xEmPQWTEmcwsxfBuqIbi/e1er0np9oRos5PgSkFb71lP7i7W5gq
M8uOt/9PGVd3i6AZuUWR/oPLBmDmQxXJQrMESfvKsaUXuXV8r+A+RDz9WDcp1U/8
sujIZO0CAwEAAaNTMFEwHQYDVR0OBBYEFGiyPtM5Nb23B1JR2xUctApprNTeMB8G
A1UdIwQYMBaAFGiyPtM5Nb23B1JR2xUctApprNTeMA8GA1UdEwEB/wQFMAMBAf8w
DQYJKoZIhvcNAQELBQADggEBABij+tJmBLlXpv8XpRLyBhY5fBZrJgTlcua7wj+7
cyQFYQDytVaBmcKopTgc5AF2wvBMzECwxnqJ36GECBf3z7t83ZKOYsAIBedoLKCT
QOrBkzro518Bb4frUBkN8s8uDTVWgMl3lXjbdSsdVzhE38ehBGHeucm67o+YojdZ
DWIeP//MIJIE2SrxfbDBqELJhBP+x3zP/LbhDHuIh+skbbFY1KnEq377X4OfiQQM
q96MSycjVr3nTzTxuwCUlwr1Z0XnSO0CZS3uSXlr8HEGBFSDcYzdUQhIWt56SzQT
4MrJhsol/QTPzt4uurwfHTIsIm7fGH9GzQhiDt6FTR0mJkA=
-----END CERTIFICATE-----
"""


def payload(**changes):
    value=dict(gatewayId='PLC-01',host='localhost',port=1883,tls=False,username='user',password='secret-test',caPem='')
    value.update(changes)
    return json.dumps(value).encode()


class ProvisioningTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temporary=tempfile.TemporaryDirectory()
        self.store=SetupStore(Path(self.temporary.name))
        self.server=SetupServer(self.store)
        await self.server.start('127.0.0.1',0)
        self.port=self.server.server.sockets[0].getsockname()[1]

    async def asyncTearDown(self):
        await self.server.close()
        self.temporary.cleanup()

    async def request(self,method,path,body=b'',token=None,origin=None,port=None):
        target=port or self.port
        reader,writer=await asyncio.open_connection('127.0.0.1',target)
        headers=f'{method} {path} HTTP/1.1\r\nHost: localhost:{target}\r\nContent-Type: application/json\r\nContent-Length: {len(body)}\r\n'
        if token: headers+='X-Setup-Token: '+token+'\r\n'
        if origin: headers+='Origin: '+origin+'\r\n'
        writer.write(headers.encode()+b'\r\n'+body);await writer.drain()
        result=await reader.read()
        writer.close();await writer.wait_closed()
        head,content=result.split(b'\r\n\r\n',1)
        return int(head.split()[1]),content

    async def test_first_save_restore_and_no_secret_readback(self):
        code,_=await self.request('POST','/api/setup',payload(),self.server.token)
        self.assertEqual(code,201)
        self.assertTrue(self.server.ready.is_set())
        self.assertEqual(self.store.load()['password'],'secret-test')
        settings,mqtt=self.store.settings(self.store.load())
        self.assertEqual(settings.gateway_id,'PLC-01')
        self.assertEqual(mqtt.password,'secret-test')
        code,result=await self.request('GET','/api/status')
        self.assertNotIn(b'secret-test',result)
        self.assertTrue(json.loads(result)['configured'])
        code,result=await self.request('GET','/api/config')
        self.assertEqual(code,200)
        self.assertNotIn(b'secret-test',result)
        self.assertNotIn(b'BEGIN CERTIFICATE',result)
        shown=json.loads(result)
        self.assertEqual(set(shown),{'configured','gatewayId','host','port','tls','username',
                                     'passwordSet','caSet','changeSeq','appliedSeq'})
        self.assertEqual((shown['gatewayId'],shown['host'],shown['username']),('PLC-01','localhost','user'))
        self.assertTrue(shown['passwordSet'])
        self.assertFalse(shown['caSet'])
        if os.name!='nt':self.assertEqual(self.store.path.stat().st_mode&0o777,0o600)

    async def test_identity_is_immutable_while_broker_settings_change(self):
        await self.request('POST','/api/setup',payload(),self.server.token)
        code,result=await self.request('POST','/api/setup',payload(gatewayId='PLC-02'),self.server.token)
        self.assertEqual(code,409)
        self.assertEqual(json.loads(result)['code'],'IDENTITY_IMMUTABLE')
        self.assertEqual(self.store.load()['gatewayId'],'PLC-01')
        self.assertEqual(self.server.change_seq,0)
        code,result=await self.request('POST','/api/setup',
            payload(host='broker.example',port=8883,tls=True),self.server.token)
        self.assertEqual(code,200)
        self.assertEqual(json.loads(result)['code'],'UPDATED')
        self.assertEqual(self.server.change_seq,1)
        saved=self.store.load()
        self.assertEqual((saved['gatewayId'],saved['host'],saved['port'],saved['tls']),
                         ('PLC-01','broker.example',8883,True))
        code,result=await self.request('GET','/api/config')
        shown=json.loads(result)
        self.assertEqual((shown['host'],shown['port'],shown['tls']),('broker.example',8883,True))

    async def test_only_a_real_change_advances_the_revision(self):
        await self.request('POST','/api/setup',payload(),self.server.token)
        self.assertEqual((self.server.change_seq,self.server.applied_seq),(0,0))
        code,result=await self.request('POST','/api/setup',payload(),self.server.token)
        self.assertEqual(code,200)
        self.assertEqual(self.server.change_seq,0)
        await self.request('POST','/api/setup',payload(gatewayId='PLC-02'),self.server.token)
        self.assertEqual(self.server.change_seq,0)
        await self.request('POST','/api/setup',payload(host='broker.example'),self.server.token)
        self.assertEqual(self.server.change_seq,1)

    async def test_blank_password_keeps_the_saved_one(self):
        await self.request('POST','/api/setup',payload(),self.server.token)
        code,_=await self.request('POST','/api/setup',payload(password='',host='broker.example'),self.server.token)
        self.assertEqual(code,200)
        saved=self.store.load()
        self.assertEqual((saved['username'],saved['password']),('user','secret-test'))
        _,mqtt=self.store.settings(saved)
        self.assertEqual(mqtt.password,'secret-test')
        code,_=await self.request('POST','/api/setup',payload(username='',password=''),self.server.token)
        self.assertEqual(code,200)
        saved=self.store.load()
        self.assertEqual((saved['username'],saved['password']),('',''))
        _,mqtt=self.store.settings(saved)
        self.assertIsNone(mqtt.password)
        self.assertIsNone(mqtt.username)

    async def test_password_pair_rules(self):
        code,result=await self.request('POST','/api/setup',payload(username='',password='secret'),self.server.token)
        self.assertEqual(code,400)
        self.assertEqual(json.loads(result)['code'],'INVALID_SETTINGS')
        self.assertFalse(self.store.path.exists())
        await self.request('POST','/api/setup',payload(),self.server.token)
        code,result=await self.request('POST','/api/setup',payload(username='',password='secret'),self.server.token)
        self.assertEqual(code,400)
        self.assertEqual(json.loads(result)['code'],'PASSWORD_WITHOUT_USERNAME')
        code,result=await self.request('POST','/api/setup',payload(username='user',password=''),self.server.token)
        self.assertEqual(code,200)
        self.assertEqual(self.store.load()['password'],'secret-test')   # kept, not blanked
        await self.request('POST','/api/setup',payload(username='',password=''),self.server.token)
        code,result=await self.request('POST','/api/setup',payload(username='user',password=''),self.server.token)
        self.assertEqual(code,400)
        self.assertEqual(json.loads(result)['code'],'PASSWORD_REQUIRED')

    async def test_private_ca_can_be_set_kept_and_removed(self):
        await self.request('POST','/api/setup',payload(tls=True,port=8883),self.server.token)
        code,_=await self.request('POST','/api/setup',payload(tls=True,port=8883,caPem=TEST_CA),self.server.token)
        self.assertEqual(code,200)
        self.assertEqual(self.store.load()['caPem'],TEST_CA)
        self.assertEqual(self.store.ca_path.read_bytes(),TEST_CA.encode())
        code,_=await self.request('POST','/api/setup',payload(tls=True,port=8883),self.server.token)
        self.assertEqual(code,200)
        self.assertEqual(self.store.load()['caPem'],TEST_CA)
        code,_=await self.request('POST','/api/setup',
            payload(tls=True,port=8883,password='',caPem='',removeCa=True),self.server.token)
        self.assertEqual(code,200)
        self.assertEqual(self.store.load()['caPem'],'')
        self.assertEqual(self.store.load()['password'],'secret-test')
        self.assertFalse(self.store.ca_path.exists())
        code,_=await self.request('POST','/api/setup',payload(tls=True,port=8883,caPem=TEST_CA),self.server.token)
        self.assertEqual(code,200)
        self.assertTrue(self.store.ca_path.exists())
        code,_=await self.request('POST','/api/setup',payload(tls=False,caPem=''),self.server.token)
        self.assertEqual(code,200)
        self.assertEqual(self.store.load()['caPem'],'')
        self.assertFalse(self.store.ca_path.exists())

    async def test_ca_without_tls_is_refused(self):
        await self.request('POST','/api/setup',payload(),self.server.token)
        code,result=await self.request('POST','/api/setup',payload(tls=False,caPem=TEST_CA),self.server.token)
        self.assertEqual(code,400)
        self.assertEqual(json.loads(result)['code'],'CA_REQUIRES_TLS')
        self.assertEqual(self.server.change_seq,0)

    async def test_test_settings_never_touch_the_live_ca(self):
        value=json.loads(payload(tls=True,port=8883,caPem=TEST_CA))
        self.assertFalse(self.store.ca_path.exists())
        self.store.settings(value)
        self.assertFalse(self.store.ca_path.exists())   # settings() stays pure
        self.store.write_ca(value)
        live=self.store.ca_path.read_bytes()
        with self.store.test_settings(value) as (_,mqtt):
            self.assertNotEqual(mqtt.ca_file,str(self.store.ca_path))
            self.assertEqual(Path(mqtt.ca_file).read_bytes(),TEST_CA.encode())
        self.assertEqual(self.store.ca_path.read_bytes(),live)
        self.assertEqual([p.name for p in self.store.directory.iterdir() if p.name.startswith('.setup-ca-')],[])
        self.assertEqual(self.store.write_ca(value),str(self.store.ca_path))

    async def test_test_endpoint_reports_failure_without_disturbing_state(self):
        await self.request('POST','/api/setup',payload(tls=True,port=8883,caPem=TEST_CA),self.server.token)
        live=self.store.ca_path.read_bytes()
        code,result=await self.request('POST','/api/test',payload(host='127.0.0.1',port=1),self.server.token)
        self.assertEqual(code,503)
        self.assertEqual(json.loads(result)['code'],'CONNECTION_FAILED')
        self.assertEqual(self.store.ca_path.read_bytes(),live)
        self.assertEqual(self.store.load()['host'],'localhost')
        self.assertEqual(self.server.change_seq,0)
        self.assertEqual([p.name for p in self.store.directory.iterdir() if p.name.startswith('.setup-ca-')],[])

    async def test_legacy_record_without_request_flags_still_loads(self):
        legacy=dict(gatewayId='PLC-01',host='localhost',port=1883,tls=False,username='user',
                    password='legacy',caPem='')
        self.store.path.write_text(json.dumps(legacy))
        self.assertEqual(self.store.load(),legacy)
        server=SetupServer(self.store,self.store.load())
        await server.start('127.0.0.1',0)
        try:
            port=server.server.sockets[0].getsockname()[1]
            code,result=await self.request('GET','/api/config',port=port)
            self.assertEqual(code,200)
            self.assertTrue(json.loads(result)['configured'])
            code,_=await self.request('POST','/api/setup',payload(host='broker.example'),server.token,port=port)
            self.assertEqual(code,200)
            self.assertEqual(self.store.load()['host'],'broker.example')
        finally:
            await server.close()

    async def test_update_requires_an_existing_record(self):
        with self.assertRaises(ValueError):self.store.update(payload(host='broker.example'))
        self.assertFalse(self.store.path.exists())

    async def test_csrf_and_invalid_settings_do_not_write(self):
        code,_=await self.request('POST','/api/setup',payload())
        self.assertEqual(code,403)
        code,_=await self.request('POST','/api/setup',payload(),self.server.token,'https://foreign.example')
        self.assertEqual(code,403)
        code,_=await self.request('POST','/api/setup',payload(port=0),self.server.token)
        self.assertEqual(code,400)
        self.assertFalse(self.store.path.exists())

    async def test_concurrent_first_save_has_single_winner(self):
        results=await asyncio.gather(*(self.request('POST','/api/setup',payload(gatewayId='PLC-'+str(i)),self.server.token) for i in range(3)))
        self.assertEqual(sorted(code for code,_ in results),[201,409,409])
        self.assertIn(self.store.load()['gatewayId'],('PLC-0','PLC-1','PLC-2'))

    async def test_corrupt_or_legacy_data_never_reinitializes(self):
        self.store.path.write_text('{broken')
        with self.assertRaises(ValueError):self.store.load()
        self.store.path.unlink()
        (self.store.directory/'agent.db').write_bytes(b'legacy')
        with self.assertRaises(ValueError):self.store.load()

    async def test_page_and_assets_are_offline_and_bounded_shutdown(self):
        for path in ('/','/app.js','/style.css'):
            code,body=await self.request('GET',path)
            self.assertEqual(code,200)
            self.assertTrue(body)
        reader,writer=await asyncio.open_connection('127.0.0.1',self.port)
        writer.write(b'GET / HTTP/1.1\r\n');await writer.drain()
        await asyncio.wait_for(self.server.close(),2)
        writer.close();await writer.wait_closed()

    def test_page_script_and_server_agree_on_fields_and_routes(self):
        """Typo guards for the three-file contract a browser would otherwise catch."""
        web=ROOT/'src/plcnext_iot/provisioning/web'
        html=(web/'index.html').read_text(encoding='utf-8')
        script=(web/'app.js').read_text(encoding='utf-8')
        server=(ROOT/'src/plcnext_iot/provisioning/server.py').read_text(encoding='utf-8')
        # The form must serialize exactly the fields the record validator accepts.
        serialized=set(re.findall(r'<(?:input|textarea)\b[^>]*\bname="([^"]+)"',html))
        self.assertEqual(serialized,FIELDS)
        # Every element the script reaches for must exist, and only by id.
        self.assertEqual(set(re.findall(r"\$\('#([A-Za-z0-9_-]+)'\)",script))
                         -set(re.findall(r'id="([^"]+)"',html)),set())
        routes={'/api/status','/api/config','/api/test','/api/setup'}
        self.assertTrue(set(re.findall(r"'(/api/[a-z]+)'",script))<=routes)
        self.assertIn("'/api/'+path",script)
        for route in routes:self.assertIn(f"'{route}'",server)
        for endpoint in ('test','setup'):self.assertIn(f"send('{endpoint}')",script)
        # The request-only flag must never reach the persisted record schema, and
        # a first save carrying it is refused rather than stored.
        self.assertNotIn('removeCa',FIELDS)
        with self.assertRaises(ValueError):parse(payload(removeCa=True))

    async def test_password_pair_and_ca_validation(self):
        for data in (payload(password=''),payload(caPem='invalid'),payload(gatewayId='../bad'),payload(port=True)):
            with self.assertRaises(ValueError):parse(data)
