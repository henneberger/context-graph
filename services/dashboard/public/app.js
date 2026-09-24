import { mergeMetrics, mediaUrl, websocketUrl } from './state.js';
const $ = id => document.getElementById(id);
let authToken='',authWorkspace='',expiryTimer;
const authHeaders=()=>({'authorization':`Bearer ${authToken}`,'x-workspace-id':authWorkspace});
const state = {metrics:[],nodes:[],edges:[],streams:new Map(),entity:'',socket:null,hls:null,lastVideo:null,attempt:0,stopped:false,generation:0};
const metricFields='entity_id metric window_start window_end sample_count sum_value avg_value min_value max_value';
function notice(message='') { $('notice').textContent=message; $('notice').hidden=!message; }
function activity(title,detail,icon='↗') {
  $('activity').querySelector('.activity-empty')?.remove();
  const li=document.createElement('li'),symbol=document.createElement('span'),body=document.createElement('span'),sub=document.createElement('span'),time=document.createElement('time');
  symbol.className='event-icon'; symbol.textContent=icon; body.textContent=title; sub.className='event-detail';sub.textContent=detail;body.append(sub); time.className='event-time';time.textContent=new Date().toLocaleTimeString([], {hour12:false});li.append(symbol,body,time);$('activity').prepend(li);while($('activity').children.length>30) $('activity').lastChild.remove();
}
async function query(query,variables={}) {
  const response=await fetch('/graphql',{method:'POST',headers:{'content-type':'application/json',...authHeaders()},body:JSON.stringify({query,variables}),signal:AbortSignal.timeout(15000)});
  if (!response.ok) throw new Error(`Query service returned ${response.status}`);
  const result=await response.json();if(result.errors?.length) throw new Error(result.errors[0].message);return result.data;
}
async function history() {
  if(!authToken)return;
  const generation=state.generation;
  try {
    const data=await query(`query History($entity:String){metrics(entityId:$entity,limit:1000){${metricFields}} nodes(limit:100){node_id node_type} edges(limit:120){edge_id source_id target_id relation} media(entityId:$entity,limit:20){entity_id payload}}`,{entity:state.entity||null});
    if(generation!==state.generation)return;
    state.metrics=data.metrics;state.nodes=data.nodes;state.edges=data.edges;renderMetrics();renderGraph();
    for(const event of data.media){try {const payload=typeof event.payload==='string'?JSON.parse(event.payload):event.payload;if(payload?.playlistUri)addStream(payload,event.entity_id);}catch{}}
    notice();
  } catch(error){if(generation===state.generation)notice(`History unavailable: ${error.message}. Live updates will continue to reconnect.`);}
}
function connect() {
  const generation=state.generation;
  if(state.stopped||!authToken)return;
  const ws=new WebSocket(websocketUrl(window.location),'graphql-transport-ws');state.socket=ws;let acknowledged=false;let lastReceived=Date.now();
  const timer=setInterval(()=>{if(ws.readyState===WebSocket.OPEN){if(Date.now()-lastReceived>45000)ws.close(1000,'Heartbeat timeout');else ws.send(JSON.stringify({type:'ping'}));}},15000);
  const ackTimer=setTimeout(()=>{if(!acknowledged)ws.close(1000,'Connection acknowledgement timeout');},10000);
  ws.onopen=()=>{if(generation!==state.generation||!authToken){ws.close();return;}ws.send(JSON.stringify({type:'connection_init',payload:{authorization:`Bearer ${authToken}`,workspaceId:authWorkspace}}));};
  ws.onmessage=message=>{
    if(generation!==state.generation||!authToken)return;
    lastReceived=Date.now();let event;try{event=JSON.parse(message.data);}catch{return;}
    if(event.type==='ping'){ws.send(JSON.stringify({type:'pong',payload:event.payload}));return;}
    if(event.type==='connection_ack'){
      acknowledged=true;clearTimeout(ackTimer);state.attempt=0;$('status').textContent='Live connection';$('status-dot').classList.add('connected');
      const operations=[['metrics',`subscription($entity:String){metricUpdated(entityId:$entity){${metricFields}}}`],['video','subscription($entity:String){videoChunk(entityId:$entity){eventId entityId eventTime payload{streamId sequence uri playlistUri durationSeconds keyframeAligned}}}'],['graph','subscription($entity:String){graphUpdated(entityId:$entity){topic entityId payload}}']];
      operations.forEach(([id,query])=>ws.send(JSON.stringify({id,type:'subscribe',payload:{query,variables:{entity:state.entity||null}}})));return;
    }
    if(event.type==='error'||event.payload?.errors){notice(`Subscription error: ${JSON.stringify(event.payload)}`);return;}
    if(event.type!=='next')return;
    const data=event.payload?.data;
    if(data?.metricUpdated){const m=data.metricUpdated;state.metrics=mergeMetrics(state.metrics,[m]);renderMetrics();activity('Metric window closed',`${m.entity_id} · ${m.metric} · ${m.sample_count} samples`);}
    if(data?.videoChunk){const e=data.videoChunk;state.lastVideo=Date.parse(e.eventTime);addStream(e.payload,e.entityId,true);$('chunk-label').textContent=`Segment ${e.payload.sequence} · ${Number(e.payload.durationSeconds).toFixed(1)}s`;activity('Video segment ready',`${e.entityId} · segment ${e.payload.sequence}`,'▣');}
    if(data?.graphUpdated){activity('Context updated',data.graphUpdated.entityId||data.graphUpdated.topic,'⌘');scheduleGraph();}
  };
  ws.onclose=()=>{clearTimeout(timer);clearTimeout(ackTimer);if(generation!==state.generation||state.stopped)return;$('status').textContent='Reconnecting';$('status-dot').classList.remove('connected');setTimeout(()=>{if(generation===state.generation)connect();},Math.min(15000,700*2**state.attempt++)+Math.random()*500);};
  ws.onerror=()=>ws.close();
}
let graphTimer;function scheduleGraph(){if(!graphTimer)graphTimer=setTimeout(()=>{graphTimer=null;history();},3000);}
function renderMetrics(){
  const metrics=state.metrics,latest=metrics.at(-1);$('chart-empty').hidden=!!metrics.length;
  const latestByEntity=new Map();for(const m of metrics)latestByEntity.set(`${m.entity_id}:${m.metric}`,m);
  $('samples').textContent=metrics.length?[...latestByEntity.values()].reduce((n,m)=>n+Number(m.sample_count),0).toLocaleString():'—';
  $('metric-count').textContent=`${latestByEntity.size} entity / metric series`;$('average').textContent=latest?Number(latest.avg_value).toLocaleString(undefined,{maximumFractionDigits:2}):'—';$('metric-name').textContent=latest?`${latest.entity_id} · ${latest.metric}`:'No metric selected';$('last-update').textContent=latest?`Window ends ${new Date(latest.window_end).toLocaleTimeString()}`:'Waiting for data';drawChart();
}
function drawChart(){
 const canvas=$('chart'),rect=canvas.getBoundingClientRect(),dpr=devicePixelRatio||1;canvas.width=rect.width*dpr;canvas.height=rect.height*dpr;const ctx=canvas.getContext('2d');ctx.scale(dpr,dpr);const w=rect.width,h=rect.height,pad=36;
 const last=state.metrics.at(-1);const windows=new Map();for(const m of state.metrics.filter(m=>m.metric===last?.metric)){const bucket=windows.get(m.window_end)||{window_end:m.window_end,sum:0,count:0};bucket.sum+=Number(m.sum_value);bucket.count+=Number(m.sample_count);windows.set(m.window_end,bucket);}const series=[...windows.values()].slice(-60);const values=series.map(m=>m.sum/Math.max(1,m.count));let lo=Math.min(...values),hi=Math.max(...values);if(lo===hi){lo-=1;hi+=1;}const spread=hi-lo;lo-=spread*.15;hi+=spread*.15;
 ctx.font='9px DM Sans';ctx.textAlign='right';for(let i=0;i<5;i++){const y=20+(h-55)*i/4;ctx.strokeStyle='#25303b';ctx.setLineDash([3,5]);ctx.beginPath();ctx.moveTo(pad,y);ctx.lineTo(w-12,y);ctx.stroke();ctx.fillStyle='#657588';if(values.length)ctx.fillText((hi-(hi-lo)*i/4).toFixed(1),pad-7,y+3);}ctx.setLineDash([]);if(!values.length)return;
 const points=values.map((v,i)=>[pad+i*(w-pad-15)/Math.max(1,values.length-1),20+(hi-v)/(hi-lo)*(h-55)]);ctx.beginPath();points.forEach(([x,y],i)=>i?ctx.lineTo(x,y):ctx.moveTo(x,y));ctx.lineTo(points.at(-1)[0],h-35);ctx.lineTo(points[0][0],h-35);ctx.closePath();const gradient=ctx.createLinearGradient(0,20,0,h);gradient.addColorStop(0,'#9ae3c42d');gradient.addColorStop(1,'#9ae3c400');ctx.fillStyle=gradient;ctx.fill();ctx.beginPath();points.forEach(([x,y],i)=>i?ctx.lineTo(x,y):ctx.moveTo(x,y));ctx.strokeStyle='#9ae3c4';ctx.lineWidth=2;ctx.stroke();for(const [x,y]of points){ctx.beginPath();ctx.arc(x,y,2.5,0,Math.PI*2);ctx.fillStyle='#9ae3c4';ctx.fill();}ctx.fillStyle='#657588';ctx.textAlign='left';ctx.fillText(new Date(series[0].window_end).toLocaleTimeString(),pad,h-10);ctx.textAlign='right';ctx.fillText(new Date(series.at(-1).window_end).toLocaleTimeString(),w-12,h-10);
}
function renderGraph(){
 const svg=$('graph');svg.replaceChildren();const nodes=new Map(state.nodes.map(n=>[n.node_id,n]));for(const e of state.edges){if(!nodes.has(e.source_id))nodes.set(e.source_id,{node_id:e.source_id});if(!nodes.has(e.target_id))nodes.set(e.target_id,{node_id:e.target_id,node_type:'related'});}const selected=new Map();for(const e of state.edges){if(selected.size>32)break;selected.set(e.source_id,nodes.get(e.source_id));selected.set(e.target_id,nodes.get(e.target_id));}for(const n of nodes.values()){if(selected.size>=36)break;selected.set(n.node_id,n);}const visible=[...selected.values()];$('entities').textContent=nodes.size.toLocaleString();$('edge-count').textContent=`${state.edges.length} relationships`;$('graph-empty').hidden=!!visible.length;
 const ns='http://www.w3.org/2000/svg';const element=(tag,attrs)=>{const e=document.createElementNS(ns,tag);Object.entries(attrs).forEach(([k,v])=>e.setAttribute(k,v));return e;};const positions=new Map(visible.map((n,i)=>{const ring=i%2?1:.63;const angle=i/visible.length*Math.PI*2;return[n.node_id,[390+Math.cos(angle)*280*ring,140+Math.sin(angle)*102*ring]];}));
 for(const edge of state.edges){const a=positions.get(edge.source_id),b=positions.get(edge.target_id);if(!a||!b)continue;const line=element('line',{x1:a[0],y1:a[1],x2:b[0],y2:b[1],stroke:'#3e5e58','stroke-width':1});const title=element('title',{});title.textContent=edge.relation;line.append(title);svg.append(line);}
 for(const node of visible){const[x,y]=positions.get(node.node_id),color=node.node_type==='related'?'#a6a0df':'#9ae3c4';svg.append(element('circle',{cx:x,cy:y,r:15,fill:color+'12',stroke:color+'30'}),element('circle',{cx:x,cy:y,r:5,fill:color}));const label=element('text',{x,y:y+28,'text-anchor':'middle',fill:'#9facbd','font-family':'DM Sans','font-size':10});label.textContent=node.node_id.length>22?node.node_id.slice(0,20)+'…':node.node_id;svg.append(label);}
}
function addStream(payload,entity,live=false){
 const url=mediaUrl(payload.playlistUri);if(!url||!payload.streamId)return;state.streams.set(payload.streamId,{url,entity});let option=[...$('streams').options].find(o=>o.value===payload.streamId);if(!option){if(!state.streams.size||$('streams').options[0]?.value==='')$('streams').replaceChildren();option=document.createElement('option');option.value=payload.streamId;option.textContent=`${entity} / ${payload.streamId.slice(0,8)}`;$('streams').append(option);while(state.streams.size>50){const oldest=[...state.streams.keys()].find(key=>key!==$('streams').value);if(!oldest)break;state.streams.delete(oldest);[...$('streams').options].find(o=>o.value===oldest)?.remove();}if(state.streams.size===1||(live&&$('video').ended)){ $('streams').value=payload.streamId;playStream(payload.streamId); }}
}
function playStream(id){const stream=state.streams.get(id);if(!stream)return;state.hls?.destroy();state.hls=null;const video=$('video');$('video-empty').hidden=true;$('video-live').hidden=false;
 if(window.Hls?.isSupported()){const hls=new Hls({lowLatencyMode:true,liveSyncDurationCount:2,liveMaxLatencyDurationCount:4,maxBufferLength:12,backBufferLength:6});state.hls=hls;hls.loadSource(stream.url);hls.attachMedia(video);hls.on(Hls.Events.ERROR,(_,e)=>{if(e.fatal){notice(`Video playback interrupted: ${e.details}`);if(e.type===Hls.ErrorTypes.NETWORK_ERROR)hls.startLoad();else if(e.type===Hls.ErrorTypes.MEDIA_ERROR)hls.recoverMediaError();else hls.destroy();}});}else if(video.canPlayType('application/vnd.apple.mpegurl'))video.src=stream.url;else notice('This browser does not support HLS playback.');video.play().catch(()=>{});
}
$('streams').addEventListener('change',e=>playStream(e.target.value));$('apply').addEventListener('click',()=>{state.entity=$('entity').value.trim();state.generation++;state.socket?.close(1000,'Filter changed');state.hls?.destroy();state.metrics=[];state.streams.clear();state.lastVideo=null;$('latency').textContent='—';$('streams').replaceChildren();$('video').removeAttribute('src');$('video').load();$('video-empty').hidden=false;$('video-live').hidden=true;renderMetrics();history();connect();});$('entity').addEventListener('keydown',e=>{if(e.key==='Enter')$('apply').click();});new ResizeObserver(drawChart).observe($('chart'));setInterval(()=>{if(state.lastVideo)$('latency').textContent=`${Math.max(0,(Date.now()-state.lastVideo)/1000).toFixed(1)}s`;},1000);setInterval(history,30000);window.addEventListener('beforeunload',()=>{state.stopped=true;state.socket?.close();state.hls?.destroy();});$('auth-dialog').showModal();
$('auth-dialog').addEventListener('cancel',event=>{if(!authToken)event.preventDefault();});
async function signOut(reason=''){
 clearTimeout(expiryTimer);authToken='';authWorkspace='';state.generation++;state.socket?.close(1000,'Signed out');state.hls?.destroy();state.hls=null;state.metrics=[];state.nodes=[];state.edges=[];state.streams.clear();state.lastVideo=null;$('streams').replaceChildren();$('video').removeAttribute('src');$('video').load();$('video-empty').hidden=false;$('video-live').hidden=true;$('latency').textContent='—';$('activity').replaceChildren();renderMetrics();renderGraph();$('status').textContent='Signed out';$('status-dot').classList.remove('connected');$('account').textContent='Sign in';$('login-error').textContent=reason;notice();
 try{await fetch('/auth/logout',{method:'POST',credentials:'same-origin'});}catch{}
 if(!$('auth-dialog').open)$('auth-dialog').showModal();
}
$('account').addEventListener('click',()=>authToken?signOut():$('auth-dialog').showModal());
$('login-form').addEventListener('submit',async event=>{
 event.preventDefault();$('login-submit').disabled=true;$('login-error').textContent='';
 try{
  const response=await fetch('/auth/token',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({username:$('login-user').value,password:$('login-password').value}),signal:AbortSignal.timeout(10000)});
  if(!response.ok)throw new Error('Sign-in failed. Check your credentials.');
  const token=await response.json();const workspace=$('login-workspace').value.trim();
  const session=await fetch('/auth/session',{method:'POST',headers:{authorization:`Bearer ${token.access_token}`,'x-workspace-id':workspace},credentials:'same-origin',signal:AbortSignal.timeout(10000)});
  if(!session.ok)throw new Error('This workspace is not available to your account.');
  authToken=token.access_token;authWorkspace=workspace;state.generation++;$('login-password').value='';$('auth-dialog').close();$('account').textContent='Sign out';$('status').textContent='Connecting';expiryTimer=setTimeout(()=>signOut('Your session expired. Sign in again.'),Math.max(1,token.expires_in)*1000);history();connect();
 }catch(error){$('login-error').textContent=error.message;}finally{$('login-submit').disabled=false;}
});
