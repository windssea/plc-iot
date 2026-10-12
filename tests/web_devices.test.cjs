'use strict';
// Runs the shipped browser script against a small DOM and controlled HTTP replies.
// No browser, Broker, Hub, or external JS dependency is required.
const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const vm=require('node:vm');
const web=path.join(__dirname,'../src/plcnext_iot/provisioning/web');
const script=fs.readFileSync(path.join(web,'app.js'),'utf8');
const html=fs.readFileSync(path.join(web,'index.html'),'utf8');
class Element{
  constructor(tag='div'){this.tagName=tag;this.children=[];this.hidden=false;this.disabled=false;this.listeners={};this.attributes={};this._text='';this.className='';this.value='';this.checked=false;this.classList={add:()=>{},remove:()=>{}};}
  set textContent(value){this._text=String(value);this.children=[];}
  get textContent(){return this._text+this.children.map(n=>n.textContent).join('');}
  set innerHTML(value){throw Error('Unsafe HTML insertion: '+value);}
  append(...children){this.children.push(...children);}
  replaceChildren(...children){this._text='';this.children=children;}
  setAttribute(name,value){this.attributes[name]=String(value);}
  addEventListener(name,callback){this.listeners[name]=callback;}
  querySelectorAll(){return [];}
  reportValidity(){return true;}
}
const settle=()=>new Promise(resolve=>setImmediate(resolve));
const metadata={configured:true,gatewayId:'edge-001',configVersion:12,messageId:'cfg-12',enabled:true,report:{batchIntervalMs:1000,maxBatchPoints:500},timestamp:1};
const list={...metadata,devices:[{deviceId:'meter/A?B',name:'Pressure <img src=x onerror=alert(1)>',protocol:'modbus_tcp',enabled:false,pointCount:1}]};
const wire=JSON.parse(fs.readFileSync(path.join(__dirname,'../contracts/examples/valid/config-set.json'),'utf8'));
const device={...wire.config.devices[0],deviceId:'meter/A?B',name:list.devices[0].name};
const detail={...metadata,device};
async function harness(){
  const elements=Object.fromEntries([...html.matchAll(/id="([^"]+)"/g)].map(match=>['#'+match[1],new Element()]));
  elements.main=new Element('main');
  const routes=new Map([
    ['/api/status',()=>({configured:true,token:'csrf'})],
    ['/api/config',()=>({configured:true,gatewayId:'edge-001',host:'localhost',port:1883,tls:false})],
    ['/api/devices',()=>list],
    ['/api/devices/meter%2FA%3FB?configVersion=12',()=>detail]
  ]);
  const calls=[];
  const context=vm.createContext({document:{querySelector:selector=>{if(!elements[selector])throw Error('Missing element '+selector);return elements[selector];},createElement:tag=>new Element(tag)},setTimeout,clearTimeout,AbortController,fetch:async(url,options)=>{
    calls.push({url,options});
    assert.ok(!options.method||options.method==='GET','device views must not write configuration');
    const handler=routes.get(url);if(!handler)throw Error('Unexpected route '+url);
    const value=await handler();
    return {ok:!value.error,status:value.error||200,json:async()=>value};
  }});
  vm.runInContext(script,context);await settle();await settle();
  return {elements,routes,calls,run:code=>vm.runInContext(code,context),open:async()=>{vm.runInContext('viewDevices()',context);await settle();await settle();}};
}
test('device list retains malicious names as literal text and opens encoded version-bound detail',async()=>{
  const h=await harness();await h.open();
  const card=h.elements['#deviceList'].children[0];
  assert.match(card.textContent,/Pressure <img src=x onerror=alert\(1\)>/);
  assert.match(card.textContent,/Modbus TCP/);assert.match(card.textContent,/已停用/);
  h.routes.set('/api/devices/meter%2FA%3FB?configVersion=12',()=>({...detail,device:{...device,points:device.points.map(point=>({...point,reportMode:'change'}))}}));
  await card.children[0].children[1].listeners.click();
  assert.ok(h.calls.some(call=>call.url==='/api/devices/meter%2FA%3FB?configVersion=12'));
  const text=h.elements['#deviceDetailBody'].textContent;
  for(const value of ['192.168.10.101','Unit ID','uint16','保持寄存器','AB','0.1','Voltage','5000 ms'])assert.ok(text.includes(value),value);
  assert.ok(text.includes('变化上报'));
  assert.match(h.elements['#configFacts'].textContent,/v12/);
});
test('OPC UA shows connection, Node ID, every point and credential flags only',async()=>{
  const h=await harness();await h.open();
  const ua=JSON.parse(fs.readFileSync(path.join(__dirname,'../contracts/examples/valid/config-opcua.json'),'utf8')).config.devices[0];
  ua.connection.usernameSet=true;ua.connection.passwordSet=true;
  h.routes.set('/api/devices/meter%2FA%3FB?configVersion=12',()=>({...metadata,device:ua}));
  await h.run("showDeviceDetail('meter/A?B')");
  const text=h.elements['#deviceDetailBody'].textContent;
  assert.match(text,/OPC UA/);assert.match(text,/安全模式/);assert.match(text,/Node ID/);assert.match(text,/已设置/);
  for(const point of ua.points)assert.ok(text.includes(point.name));
});
test('uninitialized, not yet received and empty active configuration have distinct messages',async()=>{
  const h=await harness();
  for(const [data,phrase] of [[{...metadata,configured:false,configVersion:0,devices:[]},'尚未初始化'],[{...metadata,configVersion:0,devices:[]},'尚未收到'],[{...metadata,devices:[]},'本版本没有子设备']]){
    h.routes.set('/api/devices',()=>data);await h.open();assert.ok(h.elements['#devicesStatus'].textContent.includes(phrase));assert.equal(h.elements['#deviceList'].children.length,0);
  }
});
test('read errors support a successful manual retry',async()=>{
  const h=await harness();h.routes.set('/api/devices',()=>({error:503}));await h.open();
  assert.match(h.elements['#devicesStatus'].textContent,/无法读取/);assert.equal(h.elements['#refreshDevices'].disabled,false);
  h.routes.set('/api/devices',()=>list);await h.elements['#refreshDevices'].listeners.click();assert.equal(h.elements['#deviceList'].children.length,1);
});
test('deleted device and config conflict clear previous detail instead of displaying stale configuration',async()=>{
  const h=await harness();await h.open();await h.run("showDeviceDetail('meter/A?B')");assert.ok(h.elements['#deviceDetailBody'].children.length);
  for(const [error,phrase] of [[404,'已没有此设备'],[409,'配置已更新'],[503,'无法读取设备详情']]){
    h.routes.set('/api/devices/meter%2FA%3FB?configVersion=12',()=>({error}));await h.run("showDeviceDetail('meter/A?B')");
    assert.ok(h.elements['#devicesStatus'].textContent.includes(phrase));assert.equal(h.elements['#deviceDetailBody'].children.length,0);assert.equal(h.elements['#configFacts'].children.length,0);
  }
});
test('even an inconsistent success cannot label old detail with a new version',async()=>{
  const h=await harness();await h.open();h.routes.set('/api/devices/meter%2FA%3FB?configVersion=12',()=>({...detail,configVersion:13}));
  await h.run("showDeviceDetail('meter/A?B')");assert.match(h.elements['#devicesStatus'].textContent,/配置已更新/);assert.equal(h.elements['#deviceDetailBody'].children.length,0);
});
test('late list response cannot replace a newer list',async()=>{
  const h=await harness();let release;h.routes.set('/api/devices',()=>new Promise(resolve=>{release=resolve;}));h.run('viewDevices()');await settle();
  h.routes.set('/api/devices',()=>({...metadata,configVersion:13,devices:[]}));await h.run('showDeviceList()');
  release(list);await settle();assert.match(h.elements['#configFacts'].textContent,/v13/);assert.equal(h.elements['#deviceList'].children.length,0);
});
test('late detail response cannot overwrite returned list or platform settings',async()=>{
  const h=await harness();await h.open();let release;
  h.routes.set('/api/devices/meter%2FA%3FB?configVersion=12',()=>new Promise(resolve=>{release=resolve;}));const pending=h.run("showDeviceDetail('meter/A?B')");await settle();
  await h.elements['#backDevices'].listeners.click();release(detail);await pending;
  assert.equal(h.elements['#deviceDetail'].hidden,true);assert.equal(h.elements['#deviceList'].children.length,1);assert.equal(h.elements['#deviceDetailBody'].children.length,0);
  await h.elements['#platformView'].listeners.click();assert.equal(h.elements['#devicesPanel'].hidden,true);assert.equal(h.elements['#summary'].hidden,false);
});
test('initial platform summary remains visible and late refresh cannot reveal it over the device view',async()=>{
  const h=await harness();assert.equal(h.elements['#summary'].hidden,false);assert.equal(h.elements['#setup'].hidden,true);
  let release;h.routes.set('/api/config',()=>new Promise(resolve=>{release=resolve;}));const pending=h.run('refresh()');await settle();await h.open();
  release({configured:true,gatewayId:'edge-001',host:'localhost',port:1883,tls:false});await pending;
  assert.equal(h.elements['#summary'].hidden,true);assert.equal(h.elements['#setup'].hidden,true);assert.equal(h.elements['#devicesPanel'].hidden,false);
});
test('late platform edit cannot expose the form after navigating to device list',async()=>{
  const h=await harness();let release;h.routes.set('/api/config',()=>new Promise(resolve=>{release=resolve;}));const pending=h.elements['#edit'].listeners.click();await settle();await h.open();
  release({configured:true,gatewayId:'edge-001',host:'localhost',port:1883,tls:false});await pending;
  assert.equal(h.elements['#summary'].hidden,true);assert.equal(h.elements['#setup'].hidden,true);assert.equal(h.elements['#devicesPanel'].hidden,false);
});
