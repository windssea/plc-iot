'use strict';
// Actual shipped script, controlled DOM, HTTP and clock: no browser or timer sleeps.
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const web=path.join(__dirname,'../src/plcnext_iot/provisioning/web');
const script=fs.readFileSync(path.join(web,'app.js'),'utf8'),html=fs.readFileSync(path.join(web,'index.html'),'utf8');
class Element{
  constructor(tag='div'){this.tagName=tag;this.children=[];this.hidden=false;this.disabled=false;this.listeners={};this.attributes={};this._text='';this.className='';this.value='';this.checked=false;this.writes=0;this.classList={add(){},remove(){}};}
  set textContent(value){this._text=String(value);this.children=[];this.writes++;}
  get textContent(){return this._text+this.children.map(n=>n.textContent).join('');}
  set innerHTML(value){throw Error('Unsafe HTML '+value);}
  append(...children){this.children.push(...children);}
  replaceChildren(...children){this._text='';this.children=children;}
  setAttribute(name,value){this.attributes[name]=String(value);}
  addEventListener(name,callback){this.listeners[name]=callback;}
  querySelectorAll(){return [];}
  reportValidity(){return true;}
}
const settle=async()=>{await new Promise(resolve=>setImmediate(resolve));await new Promise(resolve=>setImmediate(resolve));};
const metadata={configured:true,gatewayId:'edge-1',configVersion:2,enabled:true};
const point=(id,overrides={})=>({pointId:id,name:id,unit:'',enabled:true,staleAfterMs:10000,...overrides});
const device={deviceId:'d/A',name:'Device A',enabled:true,protocol:'modbus_tcp',connection:{},points:[point('zero',{unit:'bar'}),point('bool'),point('text')]};
const url='/api/devices/d%2FA/samples?configVersion=2';
const response=points=>({gatewayId:'edge-1',configVersion:2,deviceId:'d/A',points});
const good=[{pointId:'zero',value:0,timestamp:1720000000000,quality:'GOOD'},{pointId:'bool',value:false,timestamp:1720000000000,quality:'GOOD'},{pointId:'text',value:'<img onerror=x>',timestamp:1720000000000,quality:'GOOD'}];
async function harness({enableAuto=true}={}){
  const elements=Object.fromEntries([...html.matchAll(/id="([^"]+)"/g)].map(m=>['#'+m[1],new Element()]));elements.main=new Element('main');
  const autoInput=html.match(/<input\b[^>]*id="autoSamples"[^>]*>/)[0];
  elements['#autoSamples'].checked=/\schecked(?:\s|=|>)/.test(autoInput);
  // Existing polling cases explicitly opt into auto mode, as a user would.
  if(enableAuto)elements['#autoSamples'].checked=true;
  const document={hidden:false,listeners:{},querySelector:s=>elements[s],createElement:tag=>new Element(tag),addEventListener(event,fn){this.listeners[event]=fn;}};
  const window={listeners:{},addEventListener(event,fn){this.listeners[event]=fn;}};
  let clock=0,next=1;const timers=new Map(),calls=[];
  const routes=new Map([['/api/status',()=>({configured:true,token:'csrf'})],['/api/config',()=>({gatewayId:'edge-1',host:'localhost',port:1883})],['/api/devices',()=>({...metadata,devices:[{...device,pointCount:3}]})],['/api/devices/d%2FA?configVersion=2',()=>({...metadata,device})],[url,()=>response(good)]]);
  const context=vm.createContext({document,window,AbortController,clearTimeout:id=>timers.delete(id),setTimeout:(fn,ms)=>{const id=next++;timers.set(id,{fn,at:clock+ms,ms});return id;},fetch:async(url,options)=>{
    calls.push({url,options});assert.ok(!options.method||options.method==='GET');const handler=routes.get(url);assert.ok(handler,'Unexpected '+url);
    const data=await handler(options);return {ok:!data.error,status:data.error||200,json:async()=>data};
  }});
  vm.runInContext(script,context);await settle();
  const h={elements,document,window,timers,calls,routes,run:s=>vm.runInContext(s,context),sampleCalls:()=>calls.filter(c=>c.url.includes('/samples?')),row:i=>elements['#samplesBody'].children[i].children,advance:async ms=>{clock+=ms;for(const [id,t] of [...timers])if(t.at<=clock){timers.delete(id);t.fn();}await settle();},open:async()=>{await vm.runInContext('viewDevices()',context);await settle();await vm.runInContext("showDeviceDetail('d/A')",context);await settle();}};
  return h;
}
test('typed zero/false, unit, true sample time and hostile text render safely',async()=>{
  const h=await harness();await h.open();assert.equal(h.row(0)[1].textContent,'0 bar');assert.equal(h.row(1)[1].textContent,'false');assert.equal(h.row(2)[1].textContent,'<img onerror=x>');
  assert.equal(h.row(0)[3].textContent,'良好');assert.notEqual(h.row(0)[2].textContent,'—');assert.match(h.elements['#samplesStatus'].textContent,/实际采集时间/);
  assert.equal(h.sampleCalls().length,1);assert.equal(h.timers.size,1);assert.equal([...h.timers.values()][0].ms,5000);
  assert.equal(h.row(0)[1].attributes.title,'原始值：0');assert.equal(h.row(1)[1].attributes.title,'原始值：false');
  h.routes.set(url,()=>response(good.map(s=>s.pointId==='zero'?{...s,value:220.60000000000002}:s)));
  await h.run('refreshSamples()');assert.equal(h.row(0)[1].textContent,'220.6 bar');assert.equal(h.row(0)[1].attributes.title,'原始值：220.60000000000002');
  h.routes.set('/api/devices/d%2FA?configVersion=2',()=>({...metadata,device:{...device,points:device.points.map(p=>({...p,dataType:'float32'}))}}));
  h.routes.set(url,()=>response(good.map(s=>s.pointId==='zero'?{...s,value:4.429999828338623}:s)));
  await h.run("showDeviceDetail('d/A')");await settle();assert.equal(h.row(0)[1].textContent,'4.43 bar');assert.equal(h.row(0)[1].attributes.title,'原始值：4.429999828338623');
});
test('completed polls only update changed cells and never reload configuration',async()=>{
  const h=await harness();await h.open();const row=h.row(0),body=h.elements['#deviceDetailBody'].children,write=row[1].writes;
  await h.advance(5000);assert.strictEqual(h.row(0),row);assert.strictEqual(h.elements['#deviceDetailBody'].children,body);assert.equal(row[1].writes,write);
  h.routes.set(url,()=>response(good.map(s=>s.pointId==='zero'?{...s,value:2}:s)));await h.advance(5000);assert.equal(row[1].textContent,'2 bar');assert.equal(row[1].writes,write+1);
  assert.equal(h.calls.filter(c=>c.url==='/api/devices').length,1);assert.equal(h.calls.filter(c=>c.url==='/api/devices/d%2FA?configVersion=2').length,1);assert.equal(h.timers.size,1);
});
test('one in-flight request, no duplicate manual polls, timeout clears old GOOD and can recover',async()=>{
  const h=await harness();await h.open();let release;h.routes.set(url,()=>new Promise(resolve=>{release=resolve;}));
  await h.advance(5000);const pending=h.sampleCalls().at(-1);h.run('refreshSamples()');h.run('refreshSamples()');assert.equal(h.sampleCalls().length,2);
  await h.advance(5000);assert.equal(pending.options.signal.aborted,true);assert.equal(h.row(0)[1].textContent,'—');assert.equal(h.row(0)[3].textContent,'读取失败');assert.equal(h.timers.size,1);
  h.routes.set(url,()=>response(good));await h.advance(5000);assert.equal(h.row(0)[1].textContent,'0 bar');release(response(good.map(s=>({...s,value:999}))));await settle();assert.equal(h.row(0)[1].textContent,'0 bar');assert.equal(h.timers.size,1);
});
test('hidden visibility and pagehide abort pending work, resume once, late responses cannot schedule extra polls',async()=>{
  const h=await harness();await h.open();let release;h.routes.set(url,()=>new Promise(resolve=>{release=resolve;}));await h.advance(5000);const old=h.sampleCalls().at(-1);
  h.document.hidden=true;h.document.listeners.visibilitychange();assert.equal(old.options.signal.aborted,true);assert.equal(h.timers.size,0);await h.advance(60000);assert.equal(h.sampleCalls().length,2);
  h.routes.set(url,()=>response(good));h.document.hidden=false;h.document.listeners.visibilitychange();await settle();assert.equal(h.sampleCalls().length,3);assert.equal(h.timers.size,1);
  release(response(good));await settle();assert.equal(h.timers.size,1);
  h.window.listeners.pagehide();assert.equal(h.timers.size,0);await h.advance(60000);assert.equal(h.sampleCalls().length,3);h.window.listeners.pageshow();await settle();assert.equal(h.sampleCalls().length,4);assert.equal(h.timers.size,1);
});
test('leaving details for list or platform stops polling and ignores late data',async()=>{
  const h=await harness();await h.open();let release;h.routes.set(url,()=>new Promise(resolve=>{release=resolve;}));await h.advance(5000);const old=h.sampleCalls().at(-1);
  await h.run('showDeviceList()');assert.equal(old.options.signal.aborted,true);assert.equal(h.timers.size,0);assert.equal(h.elements['#liveSamples'].hidden,true);
  release(response(good));await settle();assert.equal(h.timers.size,0);await h.advance(100000);assert.equal(h.sampleCalls().length,2);
  h.routes.set(url,()=>response(good));await h.run("showDeviceDetail('d/A')");await settle();await h.elements['#platformView'].listeners.click();assert.equal(h.timers.size,0);assert.equal(h.elements['#liveSamples'].hidden,true);
});
test('pause control cancels automatic work; manual refresh stays available without scheduling',async()=>{
  const h=await harness();await h.open();h.elements['#autoSamples'].checked=false;h.elements['#autoSamples'].listeners.change();assert.equal(h.timers.size,0);
  await h.advance(60000);assert.equal(h.sampleCalls().length,1);await h.elements['#refreshSamples'].listeners.click();assert.equal(h.sampleCalls().length,2);assert.equal(h.timers.size,0);
  h.elements['#autoSamples'].checked=true;h.elements['#autoSamples'].listeners.change();await settle();assert.equal(h.sampleCalls().length,3);assert.equal(h.timers.size,1);
});
test('UNKNOWN, DISABLED, STALE, BAD clear values and preserve sample-time meaning',async()=>{
  const h=await harness();await h.open();
  for(const [quality,label] of [['UNKNOWN','尚无采样'],['DISABLED','已停用'],['STALE','陈旧'],['BAD_TIMEOUT','异常（BAD_TIMEOUT）']]){
    h.routes.set(url,()=>response(good.map(s=>({...s,quality,value:999}))));await h.run('refreshSamples()');assert.equal(h.row(0)[1].textContent,'—');assert.equal(h.row(0)[3].textContent,label);
  }
  h.routes.set(url,()=>response([]));await h.run('refreshSamples()');assert.equal(h.row(0)[1].textContent,'—');assert.equal(h.row(0)[2].textContent,'—');assert.equal(h.row(0)[3].textContent,'尚无采样');
});
test('read failure clears previous GOOD and schedules bounded retry',async()=>{
  const h=await harness();await h.open();h.routes.set(url,()=>({error:503}));await h.run('refreshSamples()');assert.equal(h.row(0)[1].textContent,'—');assert.equal(h.row(0)[2].textContent,'—');assert.equal(h.row(0)[3].textContent,'读取失败');assert.match(h.elements['#samplesStatus'].textContent,/已清空/);assert.equal(h.timers.size,1);
  assert.equal(h.row(0)[1].attributes.title,'');h.routes.set(url,()=>response(good));await h.advance(5000);assert.equal(h.row(0)[3].textContent,'良好');
});
test('409 and inconsistent success stop polling until configuration is reloaded',async()=>{
  for(const reply of [{error:409},{...response(good),configVersion:3},{...response(good),deviceId:'other'}]){
    const h=await harness();await h.open();h.routes.set(url,()=>reply);await h.run('refreshSamples()');assert.match(h.elements['#samplesStatus'].textContent,/配置已更新/);assert.equal(h.row(0)[1].textContent,'—');assert.equal(h.timers.size,0);assert.equal(h.elements['#refreshSamples'].disabled,true);
    await h.advance(60000);await h.run('refreshSamples()');assert.equal(h.sampleCalls().length,2);
  }
});
test('late old-device samples cannot overwrite selected new-device data',async()=>{
  const h=await harness();await h.open();let release;h.routes.set(url,()=>new Promise(resolve=>{release=resolve;}));await h.advance(5000);
  const second={...device,deviceId:'second'};h.routes.set('/api/devices/second?configVersion=2',()=>({...metadata,device:second}));h.routes.set('/api/devices/second/samples?configVersion=2',()=>({...response(good.map(s=>({...s,value:7}))),deviceId:'second'}));
  await h.run("showDeviceDetail('second')");await settle();assert.equal(h.row(0)[1].textContent,'7 bar');release(response(good));await settle();assert.equal(h.row(0)[1].textContent,'7 bar');assert.equal(h.timers.size,1);
});
test('zero-point device creates no polling request or timer',async()=>{
  const h=await harness();h.routes.set('/api/devices/d%2FA?configVersion=2',()=>({...metadata,device:{...device,points:[]}}));await h.open();assert.equal(h.sampleCalls().length,0);assert.equal(h.timers.size,0);assert.equal(h.elements['#samplesEmpty'].hidden,false);
});
test('disabled gateway/device/all points are displayed from configuration without any samples request',async()=>{
  for(const disabled of [{...metadata,enabled:false,device},{...metadata,device:{...device,enabled:false}},{...metadata,device:{...device,points:device.points.map(p=>({...p,enabled:false}))}}]){
    const h=await harness();h.routes.set('/api/devices/d%2FA?configVersion=2',()=>disabled);await h.open();assert.equal(h.sampleCalls().length,0);assert.equal(h.timers.size,0);assert.equal(h.row(0)[3].textContent,'已停用');assert.equal(h.elements['#refreshSamples'].disabled,true);
  }
});

test('HTML defaults to one read with no repeating work until the user opts in',async()=>{
  const h=await harness({enableAuto:false});assert.equal(h.elements['#autoSamples'].checked,false);
  await h.open();assert.equal(h.sampleCalls().length,1);assert.equal(h.timers.size,0);assert.match(h.elements['#samplesStatus'].textContent,/显示上次读取的采样/);
  await h.advance(120000);assert.equal(h.sampleCalls().length,1);assert.equal(h.timers.size,0);
  h.document.hidden=true;h.document.listeners.visibilitychange();h.document.hidden=false;h.document.listeners.visibilitychange();h.window.listeners.pagehide();h.window.listeners.pageshow();await settle();assert.equal(h.sampleCalls().length,1);
  await h.elements['#refreshSamples'].listeners.click();assert.equal(h.sampleCalls().length,2);assert.equal(h.timers.size,0);
  h.elements['#autoSamples'].checked=true;h.elements['#autoSamples'].listeners.change();await settle();assert.equal(h.sampleCalls().length,3);assert.equal(h.timers.size,1);
});
