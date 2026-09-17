'use strict';
const form=document.querySelector('#setup'), notice=document.querySelector('#notice'),
      loading=document.querySelector('#loading'), summary=document.querySelector('#summary'),
      facts=document.querySelector('#facts'), state=document.querySelector('#state'),
      confirmBox=document.querySelector('#confirm');
const $=(selector)=>document.querySelector(selector);
let token='', editing=false, busy=false;

const MESSAGES={INVALID_SETTINGS:'请检查端口、用户名/密码及 CA 证书。',
  CONNECTION_FAILED:'连接失败，请检查网络、Broker 地址、证书和凭据。',
  SAVE_FAILED:'设置保存失败，请检查设备存储后重试。', BUSY:'设备正在处理另一项操作，请稍后重试。',
  FORBIDDEN:'页面已失效，请刷新后重试。', NOT_CONFIGURED:'设备尚未初始化，请刷新页面后重新填写。',
  IDENTITY_IMMUTABLE:'PLC ID 不可修改，请只修改平台连接设置。',
  CA_REQUIRES_TLS:'填写私有 CA 时必须同时开启 TLS 加密。',
  PASSWORD_REQUIRED:'使用用户名时必须填写密码。',
  PASSWORD_WITHOUT_USERNAME:'要改为匿名连接，请同时清空用户名与密码。'};

function say(text,ok){notice.className=ok?'success':'';notice.textContent=text;}
function buttons(disabled){form.querySelectorAll('button').forEach(b=>{b.disabled=disabled;});}

function render(data,note){
  loading.hidden=true;
  if(!data||!data.configured){summary.hidden=true;form.hidden=false;editing=false;applyMode();return;}
  const nodes=[];
  for(const [label,value,extra] of [['PLC ID',data.gatewayId,'不可修改'],
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
  summary.hidden=false;form.hidden=true;confirmBox.hidden=true;editing=false;applyMode();
}

// The saved PLC identity is read-only: the collection database is bound to it.
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
  try{fill(await load('/api/config'));editing=true;applyMode();
    summary.hidden=true;form.hidden=false;confirmBox.hidden=true;}
  catch{say('无法读取已保存的设置，请刷新页面后重试。');}
});
$('#cancel').addEventListener('click',async()=>{
  confirmBox.hidden=true;say('');
  try{await refresh();}catch{summary.hidden=false;form.hidden=true;}
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
