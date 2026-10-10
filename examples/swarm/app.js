import { endpoint } from './api.js';
import { freshness, knowledge, directConnections, hitVisible } from './knowledge.js';

const $ = id => document.getElementById(id);
const ctx = $('map').getContext('2d');
const cellSize = 30;
const pixelRatio=$('map').width/960;
let state, previous;
let sampledAt = performance.now();
let trafficRate = 0;
let peerRates = [];
const hotLinks = new Map();
const hotDirect = new Map();
let controlsBusy = false;
const nodeName = n => n === state.center ? 'COMMAND CENTER' : 'GLIDER-' + String(n + 1).padStart(2, '0');
const shortName = n => n === state.center ? 'CC' : 'G' + (n + 1);
const contactColor = kind => ({civil:'#83bdf5', hostile:'#fa937d', whale:'#c4a5f5', sperm_whale:'#e4a5ef', ocean_front:'#74dbe7'}[kind] || '#f6bc73');
const contactName = r => 'C' + r.id + ' · ' + (r.kind || 'contact').replaceAll('_', ' ');
const color = n => n === state.center ? '#b5a3f4' : '#5adbc6';
let toastUntil = 0;
let labels = [];
let initialized = false;
let selection = null;
let entities = [];
let zoom=1, offset={x:0,y:0}, gestureMoved=false;
const pointers=new Map();
const selectedReplica = () => Number($('node').value);

function selectNode(n) {
  $('hover-info').hidden=true;
  selection=null;
  $('node').value = n;
  $('view').value = 'local';
  draw();
}

function apply(next) {
  const now = performance.now();
  if (previous && next.delivered_bytes >= previous.delivered_bytes && now > sampledAt) {
    peerRates = next.bandwidth.map((stats,id)=>({tx: Math.max(0,stats.tx_bytes-previous.bandwidth[id].tx_bytes)*8/(now-sampledAt),rx:Math.max(0,stats.rx_bytes-previous.bandwidth[id].rx_bytes)*8/(now-sampledAt)}));
    trafficRate = (next.delivered_bytes - previous.delivered_bytes) * 1000 / (now - sampledAt);
    const old = new Map(previous.links.map(l => [l.a + ':' + l.b, l.bytes]));
    next.links.forEach(l => { if (l.bytes > (old.get(l.a + ':' + l.b) || 0)) hotLinks.set(l.a + ':' + l.b, now + 450); });
    const received = new Map(previous.direct_peers.flatMap((peers, observer) => peers.map(peer => [observer + ':' + peer.peer, peer.datagrams])));
    next.direct_peers.forEach((peers, observer) => peers.forEach(peer => {
      if (peer.datagrams > (received.get(observer + ':' + peer.peer) || 0)) hotDirect.set(observer + ':' + peer.peer, now + 450);
    }));
  } else { peerRates = []; trafficRate = 0; hotLinks.clear(); hotDirect.clear(); }
  sampledAt = now; previous = next; state = next;
  for (const id of ['node', 'peer', 'link-a', 'link-b']) {
    if ($(id).options.length !== state.nodes.length) {
      $(id).replaceChildren(...state.nodes.map((_, n) => new Option(nodeName(n), n)));
      if (id === 'link-b') $(id).value = 1;
    }
  }
  if(!initialized){$('node').value=state.center;initialized=true;}
  if (document.activeElement !== $('range')) $('range').value = state.range;
  draw();
}

async function command(action) {
  if (controlsBusy) return;
  controlsBusy = true;
  try {
    const response = await fetch(endpoint(action), { method: 'POST' });
    if (!response.ok) throw Error(await response.text());
    apply(await response.json()); $('error').textContent = '';
    if(action.startsWith('order/')) { $('toast').textContent='Order issued by CC · awaiting network delivery'; toastUntil=performance.now()+5000; }
  } catch (error) { $('error').textContent = error.message; }
  finally { controlsBusy = false; }
}

for (const button of document.querySelectorAll('[data-action]')) {
  button.onclick = () => command(button.id === 'play' ? (state.playing ? 'pause' : 'play') : button.dataset.action);
}
for (const button of document.querySelectorAll('[data-peer]')) {
  button.onclick = () => command('peer/' + $('peer').value + '/' + button.dataset.peer);
}
$('order-action').onchange = () => {
  const action=$('order-action').value;
  if(action) command('order/' + selectedReplica() + '/' + action);
  $('order-action').value='';
};
$('scenario-action').onchange=()=>{if($('scenario-action').value) command($('scenario-action').value);$('scenario-action').value='';};
$('peer-action').onchange=()=>{if($('peer-action').value) command('peer/'+$('peer').value+'/'+$('peer-action').value);$('peer-action').value='';};
$('link-toggle').onclick = () => command('link/' + $('link-a').value + '/' + $('link-b').value + '/toggle');
$('range').onchange = () => command('range/' + $('range').value);
$('view').onchange=()=>{selection=null;draw();};
$('object-action').onchange=()=>{if(selection?.kind==='zone' && $('object-action').value)command(selection.zone.kind==='storm'?'weather':'jammer');$('object-action').value='';};
$('node').onchange = $('show-connections').onchange = draw;
$('peer').onchange = $('link-a').onchange = $('link-b').onchange = draw;

function mapPoint(event) {
  const bounds=$('map').getBoundingClientRect();
  // object-fit: contain may letterbox the canvas inside its flexible workspace.
  const scale=Math.min(bounds.width/960,bounds.height/600);
  return {x:(event.clientX-bounds.left-(bounds.width-960*scale)/2)/scale,
    y:(event.clientY-bounds.top-(bounds.height-600*scale)/2)/scale};
}
function pick(event) {
  const point=mapPoint(event);
  return hitVisible(entities, {x:(point.x-offset.x)/zoom,y:(point.y-offset.y)/zoom});
}
function inspect(entity) { selection=entity;draw(); }
$('map').onclick=event=>{if(!state||gestureMoved)return;const entity=pick(event);if(entity?.kind==='peer')selectNode(entity.source);else inspect(entity);};
$('map').onmousemove=event=>{
  if(!state)return; const entity=pick(event), tip=$('hover-info'); tip.hidden=!entity;
  $('map').style.cursor=entity?'pointer':'default';
  if(entity){const bounds=$('map').getBoundingClientRect();tip.style.left=Math.max(0,Math.min(event.clientX-bounds.left+12,bounds.width-190))+'px';tip.style.top=Math.max(30,event.clientY-bounds.top-35)+'px';tip.textContent=entity.label+' · click to inspect';}
};
$('map').onmouseleave=()=>{$('hover-info').hidden=true;};
$('map').onkeydown=event=>{if(event.key==='Escape'){if(selection)inspect(null);else selectNode(state.center);}};

function constrainMap() {
  offset.x=Math.min(0,Math.max(960*(1-zoom),offset.x));
  offset.y=Math.min(0,Math.max(600*(1-zoom),offset.y));
}
function zoomAt(point, next) {
  next=Math.max(1,Math.min(5,next));
  offset={x:point.x-(point.x-offset.x)*next/zoom,y:point.y-(point.y-offset.y)*next/zoom};
  zoom=next;constrainMap();draw();
}
$('map').addEventListener('wheel',event=>{event.preventDefault();zoomAt(mapPoint(event),zoom*Math.exp(-event.deltaY*.002));},{passive:false});
$('map').ondblclick=event=>{event.preventDefault();zoomAt(mapPoint(event),zoom===1?2:1);};
$('map').onpointerdown=event=>{gestureMoved=false;pointers.set(event.pointerId,mapPoint(event));$('map').setPointerCapture(event.pointerId);};
$('map').addEventListener('pointermove',event=>{
  if(!pointers.has(event.pointerId))return;
  const before=[...pointers.values()],previousPoint=pointers.get(event.pointerId),point=mapPoint(event);
  pointers.set(event.pointerId,point);
  if(pointers.size===2){const after=[...pointers.values()];const distance=points=>Math.hypot(points[0].x-points[1].x,points[0].y-points[1].y);zoomAt({x:(after[0].x+after[1].x)/2,y:(after[0].y+after[1].y)/2},zoom*distance(after)/Math.max(1,distance(before)));gestureMoved=true;}
  else if(Math.hypot(point.x-previousPoint.x,point.y-previousPoint.y)>1){offset.x+=point.x-previousPoint.x;offset.y+=point.y-previousPoint.y;constrainMap();gestureMoved=true;draw();}
  $('hover-info').hidden=true;
});
$('map').onpointerup=$('map').onpointercancel=event=>pointers.delete(event.pointerId);

function drawSelection(known, local) {
  const panel=$('object-inspector'); panel.hidden=!selection;
  $('object-action').hidden=true;
  if(!selection)return;
  let title='', detail='';
  if(selection.kind==='contact') {
    const reports=local?known.reports.filter(r=>r.id===selection.id):state.truth_contacts.filter(r=>r.id===selection.id);
    if(!reports.length){selection=null;panel.hidden=true;return;}
    const r=reports.find(r=>r.source===selection.source)||reports[0];
    selection.position=r.position;
    title=contactName(r); detail=local?'Observed '+r.age.toFixed(0)+'s ago by '+shortName(r.source)+'.\n'+reports.map(v=>shortName(v.source)+': '+v.age.toFixed(0)+'s old · ('+v.position.x.toFixed(1)+', '+v.position.y.toFixed(1)+')').join('\n'):'Actual position ('+r.position.x.toFixed(1)+', '+r.position.y.toFixed(1)+'). Simulator classification.';
  } else if(selection.kind==='terrain') {
    title='Survey tile '+selection.x+' / '+selection.y;
    detail=local?'Coastline available in this replica.':'Actual coastline · simulator truth.';
  } else if(selection.kind==='link') {
    title=shortName(selection.a)+' ↔ '+shortName(selection.b);
    if(local){const peer=selection.b===selectedReplica()?selection.a:selection.b;const receipt=state.direct_peers[selectedReplica()].find(r=>r.peer===peer);detail=receipt?'Last direct ingress '+(receipt.age_ms/1000).toFixed(1)+'s ago.\n'+receipt.datagrams+' datagrams heard. Availability now is unknown.':'No ingress known.';}
    else {const link=state.links.find(l=>l.a===Math.min(selection.a,selection.b)&&l.b===Math.max(selection.a,selection.b));detail=link?(link.enabled?'Available':link.reason)+' · '+link.distance.toFixed(1)+' map units.':'Unknown link';}
  } else if(selection.kind==='zone') {
    const zone=state.disruptions.find(z=>z.kind===selection.zone.kind);
    if(!zone){selection=null;panel.hidden=true;return;}
    selection.position=zone.position;selection.zone=zone;
    title=zone.kind==='storm'?'Drifting storm':'Human jamming';detail='Communication paths crossing this zone are unavailable.\nRadius '+zone.radius+' map units · simulator truth. Motion and sensor observations continue.';
    $('object-action').hidden=local;
  }
  $('object-name').textContent=title;$('object-detail').textContent=detail;
}

function draw() {
  if (!state) return;
  const selected = selectedReplica();
  const mode = $('view').value;
  const local = mode === 'local';
  entities=[];
  const entries = state.nodes[selected];
  const known = knowledge(entries, state.ticks);
  const direct = directConnections(known, selected, state.direct_peers[selected], state.center, state.command_position);

  $('inspector-name').textContent=mode==='truth'?'Ground truth':nodeName(selected);
  $('inspector-note').textContent=mode==='truth'?'Simulator view. Click a glider to inspect its local knowledge.':selected===state.center?'Received reports. Unknown areas stay blank.':'Own navigation and received reports. Older positions fade.';
  $('order-controls').hidden=selected===state.center || mode==='truth';
  $('local-inspector').hidden=!local && !selection;
  $('mission-layout').style.gridTemplateColumns=local||selection?'':'1fr';
  document.querySelectorAll('#local-inspector > .inspector, #local-inspector > .knowledge-section').forEach(el=>el.hidden=!local);
  $('diagnostics').hidden=local;
  if(local) $('diagnostics').open=false;
  if(performance.now()>toastUntil) $('toast').textContent='';
  $('status').textContent = state.partitioned ? 'DISCONNECTED COMPONENTS' : state.divergent_keys ? 'RECONCILING' : 'CONVERGED';
  $('title').textContent = mode === 'truth' ? 'SIMULATOR / GROUND TRUTH' : nodeName(selected) + ' · LOCAL KNOWLEDGE';
  $('keys').textContent = entries.length + ' local keys';
  $('map-note').textContent = local ? 'Only information available to this peer' : 'Actual world · click a glider to enter its view';
  $('keys').hidden=true; $('sectors').hidden=true;
  labels=[];
  ctx.resetTransform();ctx.clearRect(0, 0, ctx.canvas.width, ctx.canvas.height);
  ctx.setTransform(zoom*pixelRatio,0,0,zoom*pixelRatio,offset.x*pixelRatio,offset.y*pixelRatio);
  for (let y = 0; y < 20; y++) for (let x = 0; x < 32; x++) {
    const cell = known.terrain.get(x + ',' + y);
    const visible = !local || cell;
    const detail = local ? cell?.detail : state.truth_detail[y * 32 + x];
    ctx.fillStyle = visible ? '#163442' : '#0a1420';
    ctx.fillRect(x * cellSize, y * cellSize, cellSize, cellSize);
    if (detail) for (let row=0;row<8;row++) for(let col=0;col<8;col++) {
      if (!(detail[row] & (1 << col))) continue;
      ctx.fillStyle = '#40594e';
      ctx.fillRect(x*cellSize+col*cellSize/8,y*cellSize+row*cellSize/8,cellSize/8,cellSize/8);
    }
    if(visible)entities.push({kind:'terrain',x,y,label:'Survey tile '+x+' / '+y});
  }
  if ($('show-connections').checked) {
    if (local) drawDirectLinks(direct, selected);
    else drawNetwork();
  }
  if (!local) drawWeather();
  if (local) {
    known.vehicles.forEach(v => { if (v.source !== selected) drawVehicle(v.position, v.source, v, false); });
    // Only the observer's own navigation position is locally available without replication.
    drawVehicle(state.positions[selected], selected, freshness(state.ticks, state.ticks, 30), true);
    if (selected !== state.center) drawVehicle(state.command_position, state.center, freshness(state.ticks, state.ticks), false);
    if ($('show-connections').checked) direct.forEach(link => {
      if (link.position && isDirectHot(selected, link.peer)) drawFlash(link.position);
    });
    known.reports.forEach(report => drawContact(report.position, report, known.contacts.includes(report) ? contactName(report) + ' · ' + report.age.toFixed(0) + 's' : '', known.contacts.includes(report)));
  } else {
    state.positions.forEach((p, n) => drawVehicle(p, n, freshness(state.ticks, state.ticks), n === selected, state.peer_states[n]));
    state.truth_contacts.forEach(r => drawContact(r.position, {...r,...freshness(state.ticks,state.ticks)},contactName(r),true));
  }
  $('sectors').textContent = known.sectors + ' sectors known locally';
  $('contact').textContent = known.contacts.length + ' contacts · ' + known.reports.length + ' latest reports · ' + known.discoveries.length + ' first discoveries';
  $('reports').replaceChildren(...known.contacts.map(r => {
    const row = document.createElement('div'); row.className = 'report peer-item';
    row.tabIndex=0;row.setAttribute('role','link');row.onclick=()=>inspect({kind:'contact',id:r.id,source:r.source});row.onkeydown=e=>{if(e.key==='Enter')row.click();};
    row.style.color = agedColor(contactColor(r.kind), r.amount);
    row.title=known.reports.filter(report=>report.id===r.id).map(report=>'Sensor '+shortName(report.source)+' · '+report.age.toFixed(0)+'s old').join('\n');
    row.textContent=contactName(r)+' · '+r.age.toFixed(0)+'s'+(r.stale?' · stale':'')+' · '+known.reports.filter(report=>report.id===r.id).length+' sources';
    return row;
  }));
  const visibleOrders = (known.orderHistory.length ? known.orderHistory : known.commands)
    .filter(o => selected === state.center || o.recipient === selected);
  const desired = new Map(known.commands.map(o => [o.recipient, o.sequence]));
  $('orders').replaceChildren(...visibleOrders.map(o=>{
    const row=document.createElement('div'); row.className='report';
    const ack=o.acknowledgement;
    const current=desired.get(o.recipient)===o.sequence;
    const status=ack ? (ack.applied ? 'applied / acknowledged' : 'expired / rejected') : o.expired ? 'deadline passed / no acknowledgement' : 'no acknowledgement';
    row.textContent=(current ? 'DESIRED ' : 'HISTORY ')+'#'+o.sequence+' → '+shortName(o.recipient)+' / '+o.action+' · '+status;
    return row;
  }));
  if (!$('orders').children.length) $('orders').textContent='No order known in this replica';
  $('order-action').disabled=controlsBusy;
  drawFleetKnowledge(known, selected);
  drawDirectPeers(direct, selected);
  $('count').textContent = state.nodes.length + ' (' + state.center + ' drones + CC)';
  $('components').textContent = state.groups.length;
  $('loss').textContent = state.loss + '%';
  $('budget').textContent = state.datagram_budget >= 1024 ? (state.datagram_budget / 1024).toFixed(1) + ' KiB' : state.datagram_budget + ' B';
  $('lost').textContent = state.loss_dropped + ' / ' + state.loss_offered;
  $('blocked').textContent = state.partition_dropped;
  $('divergent').textContent = state.divergent_keys; $('union').textContent = state.union_keys; $('rounds').textContent = state.rounds_total;
  const ratio = state.union_keys ? 1 - state.divergent_keys / state.union_keys : 1;
  $('progress').value = ratio; $('ratio').textContent = (ratio * 100).toFixed(1) + '% identical on every replica, including isolated peers';
  $('groups').textContent = state.groups.map(g => g.members.map(shortName).join(', ') + ': ' + (g.keys ? g.same * 100 / g.keys : 100).toFixed(0) + '% agreement').join(' · ');
  $('packets').textContent = state.delivered_datagrams; $('bytes').textContent = (state.delivered_bytes / 1024).toFixed(1) + ' KiB'; $('rate').textContent = (trafficRate / 1024).toFixed(1) + ' KiB/s';
  const ownTraffic=state.bandwidth[selected];
  const ownDelay=state.bandwidth_kbps ? ownTraffic.max_lane_queued_bytes*8/(state.bandwidth_kbps*1000) : 0;
  $('local-network-health').textContent=ownDelay>1?(selected===state.center?'Slowest transmit lane ≈ ':'Transmit backlog ≈ ')+ownDelay.toFixed(0)+'s. Updates may reach peers late.':ownTraffic.rx_dropped?'Receive capacity exceeded; some datagrams were lost.':' ';
  $('bandwidth-limit').textContent = state.bandwidth_kbps ? state.bandwidth_kbps + ' kbit/s gliders · CC per-link' : 'Unlimited';
  $('peer-bandwidth').replaceChildren(...state.bandwidth.map((stats,id)=>{
    const row=document.createElement('div'); row.className='report';
    const rate=peerRates[id] || {tx:0,rx:0};
    const delay=state.bandwidth_kbps ? stats.max_lane_queued_bytes*8/(state.bandwidth_kbps*1000) : 0;
    row.style.color=stats.queued_bytes ? '#f6bc73' : '#8194a7';
    row.textContent=shortName(id)+' · TX '+rate.tx.toFixed(1)+' / RX '+rate.rx.toFixed(1)+' kbit/s · queue '+(stats.queued_bytes/1024).toFixed(1)+' KiB (~'+delay.toFixed(1)+'s) · drops '+stats.tx_dropped+' TX / '+stats.rx_dropped+' RX';
    return row;
  }));
  $('phase').textContent = state.phase;
  $('time').textContent = Math.floor(state.seconds / 60) + ':' + String(Math.floor(state.seconds % 60)).padStart(2, '0');
  $('sim-status').textContent = state.playing ? 'running' : 'paused'; $('play').textContent=state.playing?'Ⅱ':'▷'; $('play').setAttribute('aria-label',state.playing?'Pause exploration':'Play exploration'); $('play').title=state.playing?'Pause exploration':'Play exploration';

  $('range-value').textContent = state.range.toFixed(0) + ' map units';
  $('peer-status').textContent = nodeName(Number($('peer').value)) + ': ' + state.peer_states[Number($('peer').value)] + ' (simulator truth)';
  const a = Number($('link-a').value), b = Number($('link-b').value);
  const link = state.links.find(l => l.a === Math.min(a, b) && l.b === Math.max(a, b));
  $('link-status').textContent = link ? (link.enabled ? 'Available' : link.reason) + ' · distance ' + link.distance.toFixed(1) : 'Choose two different peers';
  $('link-toggle').textContent = link?.manual_cut ? 'Remove manual cut' : 'Cut selected link';
  $('link-toggle').disabled = a === b;
  document.querySelectorAll('[data-act]').forEach(el => el.classList.toggle('active', state.scripted && state.phase.startsWith(el.dataset.act + ' /')));
  if(!local) drawNodeCards(selected);
  drawSelection(known,local);
  if(selection?.position){ctx.save();ctx.strokeStyle='#ffffff88';ctx.lineWidth=1;ctx.beginPath();ctx.arc(selection.position.x*cellSize,selection.position.y*cellSize,15,0,Math.PI*2);ctx.stroke();ctx.restore();}
}

function agedColor(hex, amount) {
  const original = [1, 3, 5].map(i => parseInt(hex.slice(i, i + 2), 16));
  return 'rgb(' + original.map((value, i) => Math.round(value + ([126, 139, 150][i] - value) * amount)).join(',') + ')';
}

function uncertainty(position, age, stroke) {
  if (age < 5) return;
  ctx.save(); ctx.setLineDash([3, 5]); ctx.strokeStyle = stroke; ctx.globalAlpha = .09;
  ctx.beginPath(); ctx.arc(position.x * cellSize, position.y * cellSize, Math.min(45, 8 + age * .35), 0, Math.PI * 2); ctx.stroke(); ctx.restore();
}

function drawVehicle(point, n, fresh, own, truthState) {
  entities.push({kind:'peer',source:n,position:point,label:shortName(n)+(own?' · own navigation':n===state.center?' · station':' · '+fresh.age.toFixed(0)+'s old')});
  const tint = agedColor(color(n), fresh.amount);
  if (!own) uncertainty(point, fresh.age, tint);
  ctx.save(); ctx.globalAlpha = fresh.alpha; ctx.translate(point.x * cellSize, point.y * cellSize);
  ctx.fillStyle = tint; ctx.strokeStyle = own ? '#ffffff' : tint; ctx.lineWidth = own ? 2 : 1;
  if (fresh.stale) ctx.setLineDash([3, 3]);
  ctx.beginPath();
  if (n === state.center) ctx.rect(-8, -8, 16, 16);
  else { ctx.moveTo(0, -9); ctx.lineTo(7, 8); ctx.lineTo(-7, 8); ctx.closePath(); }
  if (!fresh.stale) ctx.fill(); ctx.stroke(); ctx.setLineDash([]);
  ctx.globalAlpha = .85; ctx.font = '11px system-ui';
  if (truthState === 'stopped') { ctx.beginPath(); ctx.moveTo(-10, -10); ctx.lineTo(10, 10); ctx.moveTo(10, -10); ctx.lineTo(-10, 10); ctx.stroke(); }
  ctx.restore();
  drawLabel(shortName(n)+(truthState ? (truthState==='active'?'':' / '+truthState) : own?' / self':n===state.center?' / station':' / '+fresh.age.toFixed(0)+'s'),point,tint,false);
}

function drawLabel(text,point,tint,above) {
  if(!text)return;
  ctx.save();ctx.font='11px system-ui';ctx.fillStyle=tint;ctx.globalAlpha=.85;
  const width=ctx.measureText(text).width, x=point.x*cellSize, y=point.y*cellSize;
  const offsets=above?[[12,-12],[12,-28],[12,20],[-width-12,-12],[12,36]]:[[11,4],[11,-16],[11,22],[-width-11,4],[11,38]];
  const fits=([dx,dy])=>{const box={x:x+dx,y:y+dy-10,w:width,h:14};return box.x>=4&&box.x+box.w<=956&&box.y>=4&&box.y+box.h<=596&&!labels.some(r=>box.x<r.x+r.w+3&&box.x+box.w+3>r.x&&box.y<r.y+r.h+2&&box.y+box.h+2>r.y);};
  const [dx,dy]=offsets.find(fits)||offsets[0];labels.push({x:x+dx,y:y+dy-10,w:width,h:14});
  ctx.fillText(text,x+dx,y+dy);ctx.restore();
}

function drawContact(position, fresh, label, primary) {
  entities.push({kind:'contact',id:fresh.id,source:fresh.source,position,label:contactName(fresh)});
  const tint = agedColor(contactColor(fresh.kind), fresh.amount);
  if (primary) uncertainty(position, fresh.age, tint);
  ctx.save(); ctx.globalAlpha = fresh.alpha; ctx.fillStyle = tint; ctx.strokeStyle = tint;
  if (fresh.stale) ctx.setLineDash([3, 3]);
  ctx.beginPath(); ctx.arc(position.x * cellSize, position.y * cellSize, primary ? 7 : 4, 0, Math.PI * 2);
  if (primary && !fresh.stale) ctx.fill(); else ctx.stroke();
  ctx.restore(); drawLabel(label,position,tint,true);
}

function drawNetwork() {
  const now = performance.now();
  for (const link of state.links) {
    if (!link.enabled) continue;
    const a = state.positions[link.a], b = state.positions[link.b];
    entities.push({kind:'link',a:link.a,b:link.b,from:a,to:b,label:shortName(link.a)+' ↔ '+shortName(link.b)});
    const hot = (hotLinks.get(link.a + ':' + link.b) || 0) > now;
    ctx.strokeStyle = hot ? '#98dace55' : '#5adbc620'; ctx.lineWidth = hot ? .65 : .35;
    ctx.beginPath(); ctx.moveTo(a.x * cellSize, a.y * cellSize); ctx.lineTo(b.x * cellSize, b.y * cellSize); ctx.stroke();
  }
}

function drawWeather() {
  for(const zone of state.disruptions) {
    entities.push({kind:'zone',zone,position:zone.position,radius:zone.radius,label:zone.kind});
    ctx.save();const x=zone.position.x*cellSize,y=zone.position.y*cellSize,r=zone.radius*cellSize;
    const tint=zone.kind==='storm'?'#a7bfd1':'#edab86';
    const gradient=ctx.createRadialGradient(x,y,0,x,y,r);gradient.addColorStop(0,tint+'22');gradient.addColorStop(1,tint+'08');ctx.fillStyle=gradient;
    ctx.beginPath();ctx.arc(x,y,r,0,Math.PI*2);ctx.fill();ctx.strokeStyle=tint+'55';ctx.lineWidth=.7;ctx.setLineDash([3,7]);ctx.stroke();ctx.restore();
    drawLabel(zone.kind.toUpperCase(),zone.position,tint,true);
  }
}

const isDirectHot = (observer, peer) => (hotDirect.get(observer + ':' + peer) || 0) > performance.now();

function drawDirectLinks(links, selected) {
  const own = state.positions[selected];
  for (const link of links) {
    if (!link.position) continue;
    entities.push({kind:'link',a:selected,b:link.peer,from:own,to:link.position,label:shortName(selected)+' ↔ '+shortName(link.peer)});
    const hot = isDirectHot(selected, link.peer);
    ctx.save(); ctx.strokeStyle = hot ? '#98dace66' : link.active ? '#5adbc638' : '#8194a728';
    ctx.lineWidth = hot ? .7 : .4;
    if (!link.active) ctx.setLineDash([4, 7]);
    ctx.beginPath(); ctx.moveTo(own.x * cellSize, own.y * cellSize);
    ctx.lineTo(link.position.x * cellSize, link.position.y * cellSize); ctx.stroke(); ctx.restore();
  }
}

function drawFlash(position) {
  ctx.save(); ctx.strokeStyle = '#98dace60'; ctx.lineWidth = .6;
  ctx.beginPath(); ctx.arc(position.x * cellSize, position.y * cellSize, 12, 0, Math.PI * 2); ctx.stroke(); ctx.restore();
}

function drawDirectPeers(links, selected) {
  $('direct-peers').replaceChildren(...links.map(link => {
    const row = document.createElement('div'); row.className = 'report peer-item';
    row.tabIndex=0;row.setAttribute('role','link');row.onclick=()=>inspect({kind:'link',a:selected,b:link.peer});row.onkeydown=e=>{if(e.key==='Enter')row.click();};
    row.classList.toggle('flash', $('show-connections').checked && isDirectHot(selected, link.peer));
    row.style.color = link.active ? '#5adbc6' : '#8194a7';
    row.textContent = shortName(link.peer) + ' · ' + (link.active ? 'recent ingress' : 'silent / last heard') +
      ' · ' + (link.age_ms / 1000).toFixed(1) + 's real' + (link.position ? '' : ' · position unknown');
    return row;
  }));
  if (!links.length) $('direct-peers').textContent = 'No direct ingress observed by ' + shortName(selected);
}

function drawFleetKnowledge(known, selected) {
  const reports = new Map(known.vehicles.map(v => [v.source, v]));
  $('fleet').replaceChildren(...Array.from({ length: state.center }, (_, n) => {
    const v = reports.get(n); if(!v && n!==selected)return null; const row = document.createElement('div'); row.className = 'report';
    row.style.color = v ? agedColor('#5adbc6', v.amount) : '#8194a7';
    if(v || n===selected){row.classList.add('peer-item');row.tabIndex=0;row.setAttribute('role','link');row.onclick=()=>selectNode(n);row.onkeydown=e=>{if(e.key==='Enter')selectNode(n);};}
    row.textContent = shortName(n) + ' · ' + (n === selected ? 'own navigation' : v ? 'last report ' + v.age.toFixed(0) + 's ago' + (v.stale ? ' · silent / stale' : '') : 'unknown') + (v ? ' · battery ' + v.battery + '%' : '');
    return row;
  }).filter(Boolean));
}

function drawNodeCards(selected) {
  let buttons = $('nodes').querySelectorAll('button');
  if (buttons.length !== state.nodes.length) {
    $('nodes').replaceChildren(...state.nodes.map((_, n) => {
      const button = document.createElement('button'); button.className = 'node'; button.onclick = () => selectNode(n);
      const label = document.createElement('span'); label.textContent = shortName(n);
      const detail = document.createElement('small'); label.append(detail);
      const marker = document.createElement('span'); marker.style.color = '#f6bc73'; button.append(label, marker); return button;
    }));
    buttons = $('nodes').querySelectorAll('button');
  }
  buttons.forEach((button, n) => {
    button.classList.toggle('east', n === state.center); button.classList.toggle('selected', n === selected);
    button.querySelector('small').textContent = state.nodes[n].length + ' keys · ' + state.peer_states[n];
    const known = state.nodes[n].some(([key]) => key.startsWith('contact/'));
    button.lastChild.textContent = known ? '●' : '';
    button.setAttribute('aria-label', nodeName(n) + ', ' + state.nodes[n].length + ' keys, ' + state.peer_states[n]);
  });
}

async function poll() {
  try {
    if (!controlsBusy) {
      const response = await fetch(endpoint('state'));
      if (!response.ok) throw Error('State unavailable');
      const next = await response.json(); if (!controlsBusy) apply(next); $('error').textContent = '';
    }
  } catch (error) { $('error').textContent = error.message; }
  setTimeout(poll, 250);
}
poll();
