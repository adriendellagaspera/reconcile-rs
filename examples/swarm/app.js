import { freshness, knowledge, directConnections } from './knowledge.js';

const $ = id => document.getElementById(id);
const ctx = $('map').getContext('2d');
const cellSize = 30;
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
let lastDrone = 0;
const selectedReplica = () => $('view').value === 'command' ? state.center : Number($('node').value);

function selectNode(n) {
  $('hover-info').hidden=true;
  if(n!==state.center) lastDrone=n;
  $('node').value = n;
  $('view').value = n === state.center ? 'command' : 'local';
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
  if (document.activeElement !== $('range')) $('range').value = state.range;
  draw();
}

async function command(action) {
  if (controlsBusy) return;
  controlsBusy = true;
  try {
    const response = await fetch('/' + action, { method: 'POST' });
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
$('view').onchange=()=>{if($('view').value==='local')$('node').value=lastDrone;draw();};
$('node').onchange = $('show-connections').onchange = draw;
$('peer').onchange = $('link-a').onchange = $('link-b').onchange = draw;

function visiblePeers() {
  const selected=selectedReplica();
  if($('view').value==='truth') return state.positions.map((position,source)=>({position,source,age:0}));
  const known=knowledge(state.nodes[selected],state.ticks);
  return [...known.vehicles.filter(v=>v.source!==selected),{position:state.positions[selected],source:selected,age:0},...(selected!==state.center ? [{position:state.command_position,source:state.center,age:0}] : [])];
}
function hitPeer(event) {
  const bounds=$('map').getBoundingClientRect();
  const x=(event.clientX-bounds.left)*960/bounds.width, y=(event.clientY-bounds.top)*600/bounds.height;
  return visiblePeers().map(peer=>({...peer,distance:Math.hypot(x-peer.position.x*cellSize,y-peer.position.y*cellSize)})).sort((a,b)=>a.distance-b.distance).find(p=>p.distance<22);
}
$('map').onclick=event=>{if(!state)return;const peer=hitPeer(event);if(peer)selectNode(peer.source);};
$('map').onmousemove=event=>{
  if(!state)return; const peer=hitPeer(event), tip=$('hover-info'); tip.hidden=!peer;
  $('map').style.cursor=peer?'pointer':'crosshair';
  if(peer){const bounds=$('map').getBoundingClientRect();tip.style.left=Math.min(event.clientX-bounds.left+12,bounds.width-170)+'px';tip.style.top=Math.max(30,event.clientY-bounds.top-35)+'px';tip.textContent=shortName(peer.source)+' · '+(peer.source===selectedReplica()?'own navigation':peer.source===state.center?'fixed station':peer.age.toFixed(0)+'s old')+' · click to inspect';}
};
$('map').onmouseleave=()=>{$('hover-info').hidden=true;};
$('map').onkeydown=event=>{if(event.key==='Escape'){selectNode(state.center);}};

function draw() {
  if (!state) return;
  const selected = selectedReplica();
  const mode = $('view').value;
  const local = mode === 'local' || mode === 'command';
  const entries = state.nodes[selected];
  const known = knowledge(entries, state.ticks);
  const direct = directConnections(known, selected, state.direct_peers[selected], state.center, state.command_position);
  if (mode === 'command') $('node').value = state.center;
  $('node').disabled = mode === 'command';
  $('inspector-name').textContent=mode==='truth'?'Ground truth':nodeName(selected);
  $('inspector-note').textContent=mode==='truth'?'Simulator view. Click a glider to inspect its local knowledge.':selected===state.center?'Reports received by the station. Missing information stays unknown.':'Own navigation and received reports only. Older positions fade; failure of another peer cannot be inferred from silence.';
  $('order-controls').hidden=selected===state.center || mode==='truth';
  $('local-inspector').hidden=!local;
  $('mission-layout').style.gridTemplateColumns=local?'':'1fr';
  $('diagnostics').hidden=local;
  if(local) $('diagnostics').open=false;
  if(performance.now()>toastUntil) $('toast').textContent='';
  $('status').textContent = state.partitioned ? 'DISCONNECTED COMPONENTS' : state.divergent_keys ? 'RECONCILING' : 'CONVERGED';
  $('title').textContent = mode === 'truth' ? 'SIMULATOR / GROUND TRUTH' : nodeName(selected) + ' / ' + (mode === 'command' ? 'LOCAL SYNTHESIS' : 'ONBOARD KNOWLEDGE');
  $('keys').textContent = entries.length + ' local keys';
  $('map-note').textContent = local ? 'Only information available to this peer' : 'Actual world · click a glider to enter its view';
  $('keys').hidden=!local; $('sectors').hidden=!local;
  labels=[];
  ctx.clearRect(0, 0, 960, 600);
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
    ctx.strokeStyle = '#ffffff06'; ctx.strokeRect(x * cellSize, y * cellSize, cellSize, cellSize);
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
    state.truth_contacts.forEach(r => drawContact(r.position, {...freshness(state.ticks,state.ticks),kind:r.kind},contactName(r),true));
  }
  $('sectors').textContent = known.sectors + ' sectors known locally';
  $('contact').textContent = known.contacts.length + ' contacts · ' + known.reports.length + ' source reports available. Latest measurement per contact; no averaging.';
  $('reports').replaceChildren(...known.contacts.map(r => {
    const row = document.createElement('div'); row.className = 'report';
    row.style.color = agedColor(contactColor(r.kind), r.amount);
    row.title=known.reports.filter(report=>report.id===r.id).map(report=>'Sensor '+shortName(report.source)+' · '+report.age.toFixed(0)+'s old').join('\n');
    row.textContent=contactName(r)+' · '+r.age.toFixed(0)+'s'+(r.stale?' · stale':'')+' · '+known.reports.filter(report=>report.id===r.id).length+' sources';
    return row;
  }));
  $('orders').replaceChildren(...known.commands.filter(o=>selected===state.center || o.recipient===selected).map(o=>{
    const row=document.createElement('div'); row.className='report';
    const ack=o.acknowledgement;
    row.textContent='#'+o.sequence+' → '+shortName(o.recipient)+' / '+o.action+' · '+(ack ? (ack.applied ? 'applied / acknowledged' : 'expired / rejected') : o.expired ? 'deadline passed / no acknowledgement' : 'issued / awaiting acknowledgement');
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
  const ownDelay=state.bandwidth_kbps ? ownTraffic.queued_bytes*8/(state.bandwidth_kbps*1000) : 0;
  $('local-network-health').textContent=ownDelay>1?'Transmit backlog ≈ '+ownDelay.toFixed(0)+'s. Updates may reach peers late.':ownTraffic.rx_dropped?'Receive capacity exceeded; some datagrams were lost.':' ';
  $('bandwidth-limit').textContent = state.bandwidth_kbps ? state.bandwidth_kbps + ' kbit/s TX + RX per peer' : 'Unlimited';
  $('peer-bandwidth').replaceChildren(...state.bandwidth.map((stats,id)=>{
    const row=document.createElement('div'); row.className='report';
    const rate=peerRates[id] || {tx:0,rx:0};
    const delay=state.bandwidth_kbps ? stats.queued_bytes*8/(state.bandwidth_kbps*1000) : 0;
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
    const hot = (hotLinks.get(link.a + ':' + link.b) || 0) > now;
    ctx.strokeStyle = hot ? '#98dace55' : '#5adbc620'; ctx.lineWidth = hot ? .65 : .35;
    ctx.beginPath(); ctx.moveTo(a.x * cellSize, a.y * cellSize); ctx.lineTo(b.x * cellSize, b.y * cellSize); ctx.stroke();
  }
}

function drawWeather() {
  if (state.storm) {
    ctx.fillStyle = '#df8e6e15'; ctx.fillRect(15.5 * cellSize, 0, cellSize, 600);
    ctx.strokeStyle = '#df8e6e77'; ctx.setLineDash([8, 10]); ctx.beginPath(); ctx.moveTo(16 * cellSize, 0); ctx.lineTo(16 * cellSize, 600); ctx.stroke(); ctx.setLineDash([]);
    ctx.fillStyle = '#dfb594'; ctx.font = '12px system-ui'; ctx.fillText('WEATHER FRONT', 16 * cellSize + 10, 35);
  }
}

const isDirectHot = (observer, peer) => (hotDirect.get(observer + ':' + peer) || 0) > performance.now();

function drawDirectLinks(links, selected) {
  const own = state.positions[selected];
  for (const link of links) {
    if (!link.position) continue;
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
    const row = document.createElement('div'); row.className = 'report';
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
      const response = await fetch('/state');
      if (!response.ok) throw Error('State unavailable');
      const next = await response.json(); if (!controlsBusy) apply(next); $('error').textContent = '';
    }
  } catch (error) { $('error').textContent = error.message; }
  setTimeout(poll, 250);
}
poll();
