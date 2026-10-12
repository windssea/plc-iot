'use strict';
const form=document.querySelector('#setup'), notice=document.querySelector('#notice'),
      loading=document.querySelector('#loading'), summary=document.querySelector('#summary'),
      facts=document.querySelector('#facts'), state=document.querySelector('#state'),
      confirmBox=document.querySelector('#confirm');
const $=(selector)=>document.querySelector(selector);
let token='', editing=false, busy=false;
let deviceView=false, deviceRequest=0, pageEpoch=0, selectedDeviceId=null, deviceMeta=null;

const MESSAGES={INVALID_SETTINGS:'请检查端口、用户名/密码及 CA 证书。',
  CONNECTION_FAILED:'连接失败，请检查网络、Broker 地址、证书和凭据。',
  SAVE_FAILED:'设置保存失败，请检查设备存储后重试。', BUSY:'设备正在处理另一项操作，请稍后重试。',
  FORBIDDEN:'页面已失效，请刷新后重试。', NOT_CONFIGURED:'设备尚未初始化，请刷新页面后重新填写。',
  IDENTITY_IMMUTABLE:'设备 / 网关 ID 不可修改，请只修改平台连接设置。',
  CA_REQUIRES_TLS:'填写私有 CA 时必须同时开启 TLS 加密。',
  PASSWORD_REQUIRED:'使用用户名时必须填写密码。',
  PASSWORD_WITHOUT_USERNAME:'要改为匿名连接，请同时清空用户名与密码。'};

function say(text,ok){notice.className=ok?'success':'';notice.textContent=text;}
function buttons(disabled){form.querySelectorAll('button').forEach(b=>{b.disabled=disabled;});}

function render(data,note){
  loading.hidden=true;
  if(!data||!data.configured){summary.hidden=true;form.hidden=false;editing=false;applyMode();if(deviceView)form.hidden=true;return;}
  const nodes=[];
  for(const [label,value,extra] of [['设备 / 网关 ID',data.gatewayId,'不可修改'],
      ['Broker 地址',data.host],['端口',String(data.port)],
      ['TLS 加密',data.tls?'已开启':'未开启'],['用户名',data.username||'匿名连接'],
      ['密码',data.passwordSet?'已设置':'未设置'],['私有 CA',data.caSet?'已配置':'未配置']]){
    const dt=document.createElement('dt');dt.textContent=label;
    const dd=document.createElement('dd');dd.textContent=value;
    if(extra){const em=document.createElement('em');em.textContent=extra;dd.append(em);}
    nodes.push(dt,dd);
  }
  facts.replaceChildren(...nodes);
  state.textContent=note||'采集服务正在使用以上配置。';
  summary.hidden=deviceView;form.hidden=true;confirmBox.hidden=true;editing=false;applyMode();
}

// The saved device identity is read-only: the collection database is bound to it.
function applyMode(){
  $('#gatewayId').readOnly=editing;
  $('#identityNote').hidden=!editing;
  $('#identityHint').hidden=editing;
  $('#passwordNote').hidden=!editing;
  $('#passwordHint').hidden=editing;
  $('#removecaRow').hidden=!editing;
  $('#caHint').textContent=editing?'留空表示沿用已保存的 CA；使用公共可信证书时无需填写。'
                                  :'使用公共可信证书时无需填写。';
  $('#save').textContent=editing?'保存修改':'保存并启动';
  $('#cancel').hidden=!editing;
}

function fill(data){
  $('#gatewayId').value=data.gatewayId;$('#host').value=data.host;$('#port').value=data.port;
  $('#tls').checked=data.tls;$('#username').value=data.username||'';
  $('#password').value='';$('#caPem').value='';$('#removeca').checked=false;
  say('');
}

async function load(path){
  const response=await fetch(path,{cache:'no-store'});
  if(!response.ok)throw Error();
  return response.json();
}

async function refresh(note){
  const status=await load('/api/status');
  token=status.token;
  if(!status.configured)render(null);
  else render(await load('/api/config'),note);
}

async function waitApplied(target){
  if(typeof target!=='number'){state.textContent='设置已保存，采集服务正在重启。';return;}
  for(let i=0;i<150;i++){
    try{if((await load('/api/status')).appliedSeq>=target){
      state.textContent='新设置已应用，采集服务正在使用该配置；能否连上平台取决于网络与凭据。';return;}}catch{}
    await new Promise(resolve=>setTimeout(resolve,200));
  }
  state.textContent='设置已保存，但尚未确认生效，请查看设备日志。';
}

async function send(path){
  if(busy)return;
  const value=Object.fromEntries(new FormData(form));
  value.tls=$('#tls').checked;
  value.port=Number(value.port);
  if(editing&&$('#removeca').checked)value.removeCa=true;
  busy=true;buttons(true);say(path==='test'?'正在验证 Broker 连接…':'正在保存设置…');
  try{
    const response=await fetch('/api/'+path,{method:'POST',
      headers:{'Content-Type':'application/json','X-Setup-Token':token},
      body:JSON.stringify(value)});
    const data=await response.json().catch(()=>({}));
    if(response.ok){
      if(path==='test'){say('Broker 连接和认证成功。尚未验证采集配置 Topic 权限。',true);return;}
      if(data.code==='UPDATED'){await refresh('正在应用新设置…');await waitApplied(data.changeSeq);}
      else await refresh('设置已保存，采集服务正在启动。保存成功不代表设备已经联网。');
      return;
    }
    if(data.code==='ALREADY_CONFIGURED'){await refresh();return;}
    say(MESSAGES[data.code]||'操作失败，请重试。');
  }catch{say('连接中断。请刷新确认是否已经保存成功。');}
  finally{busy=false;buttons(false);}
}

form.addEventListener('submit',event=>{
  event.preventDefault();
  if(!form.reportValidity()||busy)return;
  if(editing){confirmBox.hidden=false;$('#confirmText').textContent=
    '确认修改平台连接设置？采集服务会重启，采集与平台上报将短暂中断，通常需要数秒。';return;}
  send('setup');
});
$('#test').addEventListener('click',()=>send('test'));
$('#confirmYes').addEventListener('click',()=>{confirmBox.hidden=true;send('setup');});
$('#confirmNo').addEventListener('click',()=>{confirmBox.hidden=true;});
$('#edit').addEventListener('click',async()=>{
  const epoch=pageEpoch;
  try{const data=await load('/api/config');
    if(deviceView||epoch!==pageEpoch)return;
    fill(data);editing=true;applyMode();
    summary.hidden=true;form.hidden=false;confirmBox.hidden=true;}
  catch{if(!deviceView&&epoch===pageEpoch)say('无法读取已保存的设置，请刷新页面后重试。');}
});
$('#cancel').addEventListener('click',async()=>{
  confirmBox.hidden=true;say('');
  try{await refresh();}catch{summary.hidden=deviceView;form.hidden=true;}
});
$('#tls').addEventListener('change',event=>{
  const port=$('#port');
  if(port.value==='8883'&&!event.target.checked)port.value='1883';
  else if(port.value==='1883'&&event.target.checked)port.value='8883';
});
// A typed CA and "remove the saved CA" contradict each other; keep one intent.
$('#removeca').addEventListener('change',event=>{if(event.target.checked)$('#caPem').value='';});
$('#caPem').addEventListener('input',event=>{if(event.target.value)$('#removeca').checked=false;});
$('#username').addEventListener('input',event=>{if(!event.target.value)$('#password').value='';});
refresh().catch(()=>{loading.textContent='无法读取设备状态，请刷新重试。';});

// Device views read the committed collection snapshot; they never change it.
const PROTOCOLS={modbusTcp:'Modbus TCP',modbus_tcp:'Modbus TCP',modbus:'Modbus TCP',opcua:'OPC UA'};
const AREAS={holding_register:'保持寄存器',input_register:'输入寄存器',coil:'线圈',discrete_input:'离散输入'};
const REPORT_MODES={cyclic:'周期',change:'变化上报',change_or_cyclic:'变化或周期上报'};
function node(tag,text,className){
  const result=document.createElement(tag);
  if(text!==undefined&&text!==null)result.textContent=String(text);
  if(className)result.className=className;
  return result;
}
function shown(value){return value===undefined||value===null||value===''?'—':String(value);}
function onOff(value){return value===true?'已启用':value===false?'已停用':'—';}
function factsInto(target,entries){
  target.replaceChildren(...entries.flatMap(([label,value])=>[node('dt',label),node('dd',shown(value))]));
}
function collectionMeta(data){
  deviceMeta=data;
  const entries=[['设备 / 网关 ID',data.gatewayId],['配置版本',data.configVersion>0?'v'+data.configVersion:'尚未下发'],['全局采集',onOff(data.enabled)]];
  if(data.configVersion>0){
    entries.push(['消息标识',data.messageId]);
    if(data.report)entries.push(['批量上报周期',shown(data.report.batchIntervalMs)+' ms'],['批量点数上限',data.report.maxBatchPoints]);
  }
  factsInto($('#configFacts'),entries);
}
function deviceState(text,error=false){$('#devicesStatus').textContent=text;$('#devicesStatus').className=error?'error':'';}
async function collectionLoad(path){
  const response=await fetch(path,{cache:'no-store'});
  if(!response.ok){const err=Error();err.status=response.status;throw err;}
  return response.json();
}
function viewDevices(){
  if(busy)return;
  deviceView=true;++pageEpoch;
  document.querySelector('main').classList.add('devices-view');
  $('#devicesPanel').hidden=false;summary.hidden=true;form.hidden=true;loading.hidden=true;
  $('#devicesView').setAttribute('aria-pressed','true');$('#platformView').setAttribute('aria-pressed','false');
  showDeviceList();
}
async function showDeviceList(){
  stopSamples();
  selectedDeviceId=null;
  const request=++deviceRequest;
  $('#devicesTitle').textContent='设备列表';$('#deviceList').hidden=false;$('#deviceDetail').hidden=true;
  $('#deviceList').replaceChildren();$('#configFacts').replaceChildren();
  $('#refreshDevices').disabled=true;deviceState('正在读取已下发的设备配置…');
  try{
    const data=await collectionLoad('/api/devices');
    if(!deviceView||request!==deviceRequest)return;
    collectionMeta(data);
    if(!data.devices.length){
      deviceState(!data.configured?'设备尚未初始化。请先在平台连接页完成设置。':data.configVersion===0?'尚未收到采集配置。请在 Hub 中配置子设备并下发。':'本版本没有子设备。');return;
    }
    deviceState('共 '+data.devices.length+' 台设备 · 展示的是本地保存的配置');
    const cards=data.devices.map(device=>{
      const card=node('article',undefined,'device-card');const header=node('div',undefined,'device-card-header');
      const heading=node('div');heading.append(node('h3',device.name),node('p',device.deviceId,'device-id'));
      const detail=node('button','详情');detail.type='button';detail.setAttribute('aria-label','查看 '+device.name+' 的详情');
      detail.addEventListener('click',()=>showDeviceDetail(device.deviceId));header.append(heading,detail);
      const info=node('dl',undefined,'device-card-facts');
      factsInto(info,[['南向协议',PROTOCOLS[device.protocol]||device.protocol],['配置状态',onOff(device.enabled)],['点位数',device.pointCount]]);
      card.append(header,info);return card;
    });
    $('#deviceList').replaceChildren(...cards);
  }catch{if(deviceView&&request===deviceRequest)deviceState('无法读取采集配置，请点击“刷新配置”重试。',true);}
  finally{if(request===deviceRequest)$('#refreshDevices').disabled=false;}
}
function detailSection(title,entries){
  const section=node('div',undefined,'detail-block');section.append(node('h3',title));
  const list=node('dl',undefined,'detail-facts');factsInto(list,entries);section.append(list);return section;
}
function renderDeviceDetail(device){
  const body=$('#deviceDetailBody'),connection=device.connection;
  const connectionFields=[['主机',connection.host],['端口',connection.port]];
  if(device.protocol==='opcua')connectionFields.push(['路径',connection.path],['安全策略',connection.securityPolicy],['安全模式',connection.securityMode],['用户名',connection.usernameSet?'已设置':'未设置'],['密码',connection.passwordSet?'已设置':'未设置']);
  else connectionFields.push(['Unit ID',connection.unitId]);
  connectionFields.push(['连接超时',shown(connection.connectTimeoutMs)+' ms'],['请求超时',shown(connection.requestTimeoutMs)+' ms'],['重试次数',connection.retryCount]);
  const nodes=[detailSection('设备信息',[['设备名称',device.name],['设备标识',device.deviceId],['南向协议',PROTOCOLS[device.protocol]||device.protocol],['配置状态',onOff(device.enabled)]]),detailSection('连接配置',connectionFields),node('h3','采集点位（'+device.points.length+'）','points-heading')];
  if(!device.points.length)nodes.push(node('p','该设备没有采集点位。'));
  for(const point of device.points){
    const entries=[['点位标识',point.pointId],['配置状态',onOff(point.enabled)],['数据类型',point.dataType],['单位',point.unit]];
    if(point.modbus)entries.push(['寄存器区域',AREAS[point.modbus.area]||point.modbus.area],['零基地址',point.modbus.address],['字节序',point.modbus.byteOrder]);
    if(point.opcua)entries.push(['Node ID',point.opcua.nodeId]);
    entries.push(['比例因子',point.scale],['偏移',point.offset],['采集周期',shown(point.pollIntervalMs)+' ms'],['报告模式',REPORT_MODES[point.reportMode]||point.reportMode],['报告周期',shown(point.reportIntervalMs)+' ms'],['死区',point.deadband],['失效时间',shown(point.staleAfterMs)+' ms']);
    nodes.push(detailSection(point.name||point.pointId,entries));
  }
  body.replaceChildren(...nodes);
}
async function showDeviceDetail(deviceId){
  stopSamples();
  const expectedVersion=deviceMeta&&deviceMeta.configVersion;
  const request=++deviceRequest;selectedDeviceId=deviceId;
  $('#devicesTitle').textContent='设备配置详情';$('#deviceList').hidden=true;$('#deviceDetail').hidden=false;
  $('#deviceDetailBody').replaceChildren();$('#configFacts').replaceChildren();
  $('#refreshDevices').disabled=true;deviceState('正在读取设备配置与点位…');
  try{
    const data=await collectionLoad('/api/devices/'+encodeURIComponent(deviceId)+'?configVersion='+expectedVersion);
    if(!deviceView||request!==deviceRequest)return;
    if(data.configVersion!==expectedVersion){deviceState('配置已更新，请返回设备列表刷新后查看详情。',true);return;}
    collectionMeta(data);renderDeviceDetail(data.device);startSamples(data);
    deviceState('以下设备和点位均来自本地保存的 v'+data.configVersion+' 配置。');
  }catch(error){if(deviceView&&request===deviceRequest)deviceState(error.status===409?
    '配置已更新，请返回设备列表刷新后查看详情。':error.status===404?'当前配置中已没有此设备。请返回设备列表查看最新配置。':'无法读取设备详情，请点击“刷新配置”重试。',true);}
  finally{if(request===deviceRequest)$('#refreshDevices').disabled=false;}
}
$('#devicesView').addEventListener('click',viewDevices);
$('#platformView').addEventListener('click',async()=>{
  if(busy)return;
  stopSamples();deviceView=false;++deviceRequest;++pageEpoch;selectedDeviceId=null;
  document.querySelector('main').classList.remove('devices-view');$('#devicesPanel').hidden=true;
  $('#devicesView').setAttribute('aria-pressed','false');$('#platformView').setAttribute('aria-pressed','true');
  loading.hidden=false;loading.textContent='正在读取设备状态…';summary.hidden=true;form.hidden=true;
  try{await refresh();}catch{loading.textContent='无法读取设备状态，请刷新重试。';}
});
$('#backDevices').addEventListener('click',showDeviceList);
$('#refreshDevices').addEventListener('click',()=>selectedDeviceId===null?showDeviceList():showDeviceDetail(selectedDeviceId));

// Read an existing, bounded runtime snapshot; never start collection or re-read
// the configuration database. There is one request and one next-refresh timer.
const SAMPLE_INTERVAL_MS=5000, SAMPLE_TIMEOUT_MS=5000;
let samples=null, samplesEpoch=0, pageActive=true;
function setText(target,value){if(target.textContent!==value)target.textContent=value;}
function sampleStatus(text,error=false){
  const status=$('#samplesStatus'),className=error?'error':'';
  setText(status,text);if(status.className!==className)status.className=className;
}
function cancelSampleWork(view){
  if(!view)return;
  if(view.timer!==null){clearTimeout(view.timer);view.timer=null;}
  if(view.timeout!==null){clearTimeout(view.timeout);view.timeout=null;}
  if(view.controller){view.controller.abort();view.controller=null;}
}
function stopSamples(){
  ++samplesEpoch;cancelSampleWork(samples);samples=null;
  $('#liveSamples').hidden=true;$('#refreshSamples').disabled=false;
}
function sampleViewActive(view){return samples===view&&view.epoch===samplesEpoch&&deviceView&&selectedDeviceId===view.deviceId&&!document.hidden&&pageActive&&!view.blocked;}
function clearSampleValues(view,quality){
  for(const row of view.rows.values()){
    setText(row.value,'—');setText(row.time,'—');setText(row.quality,quality);
    sampleRawTitle(row,'');
    if(row.quality.className!=='sample-quality unknown')row.quality.className='sample-quality unknown';
  }
}
function startSamples(data){
  const rows=new Map(),entries=[];
  for(const point of data.device.points){
    const row=node('tr'),name=node('td',point.name||point.pointId),value=node('td','—'),time=node('td','—'),quality=node('td','尚无采样','sample-quality unknown');
    name.append(node('small',point.pointId));row.append(name,value,time,quality);entries.push(row);
    rows.set(point.pointId,{point,value,time,quality});
  }
  $('#samplesBody').replaceChildren(...entries);$('#liveSamples').hidden=false;
  $('#samplesEmpty').hidden=entries.length>0;
  samples={epoch:samplesEpoch,deviceId:data.device.deviceId,gatewayId:data.gatewayId,version:data.configVersion,
    enabled:data.enabled===true&&data.device.enabled===true,rows,timer:null,timeout:null,controller:null,blocked:false};
  if(entries.length&&(!samples.enabled||data.device.points.every(point=>point.enabled!==true))){
    paintSamples(samples,{points:[]});samples.blocked=true;$('#refreshSamples').disabled=true;
    sampleStatus('采集已停用，不读取实时采样。启用并下发新配置后，请返回设备列表刷新。');return;
  }
  sampleStatus(entries.length?'正在读取终端实时采样…':'该设备没有采集点位。');
  if(entries.length&&sampleViewActive(samples))return refreshSamples();
}
function sampleTime(timestamp){
  if(typeof timestamp!=='number'||!Number.isFinite(timestamp)||timestamp<=0)return '—';
  const date=new Date(timestamp);
  return Number.isNaN(date.getTime())?'—':date.toLocaleString('zh-CN',{hour12:false})+'.'+String(date.getMilliseconds()).padStart(3,'0');
}
function sampleValue(value,point){
  // Display precision follows the configured source type; the API value stays
  // untouched and remains available in the cell's tooltip.
  return typeof value==='number'&&Number.isFinite(value)
    ?String(Number(value.toPrecision(point.dataType==='float32'?7:12))):String(value);
}
function sampleRawTitle(row,raw){
  if(row.rawText!==raw){row.value.setAttribute('title',raw?'原始值：'+raw:'');row.rawText=raw;}
}
function paintSamples(view,data){
  const points=new Map();
  for(const sample of data.points){
    if(!sample||typeof sample.pointId!=='string'||points.has(sample.pointId))throw Error('INVALID_SAMPLES');
    points.set(sample.pointId,sample);
  }
  for(const [pointId,row] of view.rows){
    const sample=points.get(pointId),disabled=!view.enabled||row.point.enabled!==true;
    const quality=disabled?'DISABLED':sample&&typeof sample.quality==='string'?sample.quality:'UNKNOWN';
    const labels={GOOD:'良好',UNKNOWN:'尚无采样',DISABLED:'已停用',STALE:'陈旧'};
    const label=labels[quality]||(quality.startsWith('BAD_')?'异常（'+quality+'）':'未知（'+quality+'）');
    const hasValue=quality==='GOOD'&&sample&&sample.value!==undefined&&sample.value!==null;
    const value=hasValue?sampleValue(sample.value,row.point)+(row.point.unit?' '+row.point.unit:''):'—';
    sampleRawTitle(row,hasValue?String(sample.value):'');
    setText(row.value,value);setText(row.time,disabled?'—':sampleTime(sample&&sample.timestamp));setText(row.quality,label);
    const color=quality==='GOOD'?'good':quality==='STALE'?'stale':quality.startsWith('BAD_')?'bad':'unknown';
    const className='sample-quality '+color;if(row.quality.className!==className)row.quality.className=className;
  }
}
async function refreshSamples(){
  const view=samples;
  if(!view||!view.rows.size||!sampleViewActive(view)||view.controller)return;
  if(view.timer!==null){clearTimeout(view.timer);view.timer=null;}
  const controller=new AbortController();view.controller=controller;$('#refreshSamples').disabled=true;
  let timeout;
  try{
    const data=await Promise.race([
      (async()=>{
        const response=await fetch('/api/devices/'+encodeURIComponent(view.deviceId)+'/samples?configVersion='+view.version,{cache:'no-store',signal:controller.signal});
        if(!response.ok){const error=Error();error.status=response.status;throw error;}
        return response.json();
      })(),
      new Promise((_,reject)=>{timeout=setTimeout(()=>{controller.abort();reject(Error('SAMPLE_TIMEOUT'));},SAMPLE_TIMEOUT_MS);view.timeout=timeout;})
    ]);
    if(!sampleViewActive(view)||view.controller!==controller)return;
    if(data.configVersion!==view.version||data.deviceId!==view.deviceId||data.gatewayId!==view.gatewayId){const error=Error();error.status=409;throw error;}
    if(!Array.isArray(data.points))throw Error('INVALID_SAMPLES');
    paintSamples(view,data);
    sampleStatus('最近读取：'+sampleTime(Date.now())+' · 下表时间是实际采集时间。'+($('#autoSamples').checked?' 每次请求完成后间隔 5 秒刷新。':' 自动刷新已暂停。'));
  }catch(error){
    if(!sampleViewActive(view)||view.controller!==controller)return;
    clearSampleValues(view,'读取失败');
    if(error.status===409||error.status===404){
      view.blocked=true;sampleStatus(error.status===409?'配置已更新，请返回设备列表刷新后查看实时采样。':'此设备已从配置中移除，请返回设备列表刷新。',true);
    }else sampleStatus('无法读取实时采样，当前值已清空。可手动重试；自动刷新开启时会稍后重试。',true);
  }finally{
    clearTimeout(timeout);
    if(view.controller===controller){
      view.controller=null;view.timeout=null;
      if(samples!==view)return;
      $('#refreshSamples').disabled=view.blocked;
      if(sampleViewActive(view)&&$('#autoSamples').checked)view.timer=setTimeout(refreshSamples,SAMPLE_INTERVAL_MS);
    }
  }
}
$('#refreshSamples').addEventListener('click',refreshSamples);
$('#autoSamples').addEventListener('change',()=>{
  if(!samples||samples.blocked)return;
  cancelSampleWork(samples);$('#refreshSamples').disabled=false;
  if($('#autoSamples').checked&&sampleViewActive(samples))refreshSamples();
  else sampleStatus('自动刷新已暂停，可点击“刷新采样”读取。');
});
function suspendSamples(){cancelSampleWork(samples);$('#refreshSamples').disabled=false;}
if(document.addEventListener)document.addEventListener('visibilitychange',()=>{
  if(document.hidden)suspendSamples();
  else if(samples&&$('#autoSamples').checked)refreshSamples();
});
if(typeof window!=='undefined'&&window.addEventListener){
  window.addEventListener('pagehide',()=>{pageActive=false;suspendSamples();});
  window.addEventListener('pageshow',()=>{pageActive=true;if(samples&&$('#autoSamples').checked)refreshSamples();});
}
