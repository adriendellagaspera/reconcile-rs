import { freshness, knowledge, directConnections } from './knowledge.js';

const $ = id => document.getElementById(id);
const ctx = $('map').getContext('2d');
const cellSize = 30;
let state, previous;
let sampledAt = performance.now();
let trafficRate = 0;
const hotLinks = new Map();
const hotDirect = new Map();
let controlsBusy = false;
const nodeName = n => n === state.center ? 'COMMAND CENTER' : 'GLIDER-' + String(n + 1).padStart(2, '0');
const shortName = n => n === state.center ? 'CC' : 'G' + (n + 1);
const contactColor = kind => ({civil:'#83bdf5', hostile:'#fa937d', whale:'#c4a5f5', sperm_whale:'#e4a5ef', ocean_front:'#74dbe7'}[kind] || '#f6bc73');
const contactName = r => 'C' + r.id + ' · ' + (r.kind || 'contact').replaceAll('_', ' ');
const color = n => n === state.center ? '#b5a3f4' : '#5adbc6';
const selectedReplica = () => $('view').value === 'command' ? state.center : Number($('node').value);

function selectNode(n) {
  $('node').value = n;
  $('view').value = n === state.center ? 'command' : 'local';
  draw();
}

function apply(next) {
  const now = performance.now();
  if (previous && next.delivered_bytes >= previous.delivered_bytes && now > sampledAt) {
    trafficRate = (next.delivered_bytes - previous.delivered_bytes) * 1000 / (now - sampledAt);
    const old = new Map(previous.links.map(l => [l.a + ':' + l.b, l.bytes]));
    next.links.forEach(l => { if (l.bytes > (old.get(l.a + ':' + l.b) || 0)) hotLinks.set(l.a + ':' + l.b, now + 450); });
    const received = new Map(previous.direct_peers.flatMap((peers, observer) => peers.map(peer => [observer + ':' + peer.peer, peer.datagrams])));
    next.direct_peers.forEach((peers, observer) => peers.forEach(peer => {
      if (peer.datagrams > (received.get(observer + ':' + peer.peer) || 0)) hotDirect.set(observer + ':' + peer.peer, now + 450);
    }));
  } else { trafficRate = 0; hotLinks.clear(); hotDirect.clear(); }
  sampledAt = now; previous = next; state = next;
  for (const id of ['node', 'peer', 'link-a', 'link-b']) {
    if ($(id).options.length !== state.nodes.length) {
      $(id).replaceChildren(...state.nodes.map((_, n) => new Option(nodeName(n), n)));
      if (id === 'link-b') $(id).value = 1;
    }
  }
  if ($('order-drone').options.length !== state.center) $('order-drone').replaceChildren(...state.nodes.slice(0,state.center).map((_,n)=>new Option(nodeName(n),n)));
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
  } catch (error) { $('error').textContent = error.message; }
  finally { controlsBusy = false; }
}

for (const button of document.querySelectorAll('[data-action]')) {
  button.onclick = () => command(button.id === 'play' ? (state.playing ? 'pause' : 'play') : button.dataset.action);
}
for (const button of document.querySelectorAll('[data-peer]')) {
  button.onclick = () => command('peer/' + $('peer').value + '/' + button.dataset.peer);
}
$('send-order').onclick = () => command('order/' + $('order-drone').value + '/' + $('order-action').value);
$('link-toggle').onclick = () => command('link/' + $('link-a').value + '/' + $('link-b').value + '/toggle');
$('range').onchange = () => command('range/' + $('range').value);
$('view').onchange = $('node').onchange = $('show-connections').onchange = draw;
$('peer').onchange = $('link-a').onchange = $('link-b').onchange = draw;
$('inspect-command').onclick = () => selectNode(state.center);
$('map').onclick = event => {
  if (!state || ['local', 'command'].includes($('view').value)) return;
  const bounds = $('map').getBoundingClientRect();
  const x = (event.clientX - bounds.left) * 960 / bounds.width;
  const y = (event.clientY - bounds.top) * 600 / bounds.height;
  const nearest = state.positions.map((p, n) => ({ n, distance: Math.hypot(x - p.x * cellSize, y - p.y * cellSize) })).sort((a, b) => a.distance - b.distance)[0];
  if (nearest.distance < 25) selectNode(nearest.n);
};

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
  $('status').textContent = state.partitioned ? 'DISCONNECTED COMPONENTS' : state.divergent_keys ? 'RECONCILING' : 'CONVERGED';
  $('title').textContent = mode === 'truth' ? 'SIMULATOR / GROUND TRUTH' : nodeName(selected) + ' / ' + (mode === 'command' ? 'LOCAL SYNTHESIS' : 'ONBOARD KNOWLEDGE');
  $('keys').textContent = entries.length + ' local keys';
  $('map-note').textContent = local ? 'Local knowledge · links = direct ingress · endpoints = last known positions' : 'Simulator truth · links = actual availability · flashes = delivered traffic';
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
  $('reports').replaceChildren(...known.reports.map(r => {
    const row = document.createElement('div'); row.className = 'report';
    row.style.color = agedColor(contactColor(r.kind), r.amount);
    row.textContent = contactName(r) + ' · ' + shortName(r.source) + ' · ' + r.age.toFixed(0) + 's · (' + r.position.x.toFixed(1) + ', ' + r.position.y.toFixed(1) + ')' + (r.stale ? ' · stale' : '');
    return row;
  }));
  $('orders').replaceChildren(...known.commands.filter(o=>selected===state.center || o.recipient===selected).map(o=>{
    const row=document.createElement('div'); row.className='report';
    const ack=o.acknowledgement;
    row.textContent='#'+o.sequence+' → '+shortName(o.recipient)+' / '+o.action+' · '+(ack ? (ack.applied ? 'applied / acknowledged' : 'expired / rejected') : o.expired ? 'deadline passed / no acknowledgement' : 'issued / awaiting acknowledgement');
    return row;
  }));
  if (!$('orders').children.length) $('orders').textContent='No order known in this replica';
  $('send-order').disabled=state.peer_states[state.center]==='stopped';
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
  $('phase').textContent = state.phase;
  $('time').textContent = Math.floor(state.seconds / 60) + ':' + String(Math.floor(state.seconds % 60)).padStart(2, '0');
  $('sim-status').textContent = state.playing ? 'running' : 'paused'; $('play').textContent = state.playing ? 'Pause exploration' : 'Play exploration';
  $('weather').textContent = state.storm ? 'Clear weather front' : 'Activate weather front';
  $('range-value').textContent = state.range.toFixed(0) + ' map units';
  $('peer-status').textContent = nodeName(Number($('peer').value)) + ': ' + state.peer_states[Number($('peer').value)] + ' (simulator truth)';
  const a = Number($('link-a').value), b = Number($('link-b').value);
  const link = state.links.find(l => l.a === Math.min(a, b) && l.b === Math.max(a, b));
  $('link-status').textContent = link ? (link.enabled ? 'Available' : link.reason) + ' · distance ' + link.distance.toFixed(1) : 'Choose two different peers';
  $('link-toggle').textContent = link?.manual_cut ? 'Remove manual cut' : 'Cut selected link';
  $('link-toggle').disabled = a === b;
  document.querySelectorAll('[data-act]').forEach(el => el.classList.toggle('active', state.scripted && state.phase.startsWith(el.dataset.act + ' /')));
  drawNodeCards(selected);
}

function agedColor(hex, amount) {
  const original = [1, 3, 5].map(i => parseInt(hex.slice(i, i + 2), 16));
  return 'rgb(' + original.map((value, i) => Math.round(value + ([126, 139, 150][i] - value) * amount)).join(',') + ')';
}

function uncertainty(position, age, stroke) {
  if (age < 5) return;
  ctx.save(); ctx.setLineDash([3, 5]); ctx.strokeStyle = stroke; ctx.globalAlpha = .22;
  ctx.beginPath(); ctx.arc(position.x * cellSize, position.y * cellSize, Math.min(90, 8 + age * .7), 0, Math.PI * 2); ctx.stroke(); ctx.restore();
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
  ctx.fillText(shortName(n) + (truthState && truthState !== 'active' ? ' / ' + truthState : own ? ' / self' : n === state.center ? ' / station' : ' / ' + fresh.age.toFixed(0) + 's'), 11, 4);
  if (truthState === 'stopped') { ctx.beginPath(); ctx.moveTo(-10, -10); ctx.lineTo(10, 10); ctx.moveTo(10, -10); ctx.lineTo(-10, 10); ctx.stroke(); }
  ctx.restore();
}

function drawContact(position, fresh, label, primary) {
  const tint = agedColor(contactColor(fresh.kind), fresh.amount);
  if (primary) uncertainty(position, fresh.age, tint);
  ctx.save(); ctx.globalAlpha = fresh.alpha; ctx.fillStyle = tint; ctx.strokeStyle = tint;
  if (fresh.stale) ctx.setLineDash([3, 3]);
  ctx.beginPath(); ctx.arc(position.x * cellSize, position.y * cellSize, primary ? 7 : 4, 0, Math.PI * 2);
  if (primary && !fresh.stale) ctx.fill(); else ctx.stroke();
  ctx.globalAlpha = .85; ctx.font = '12px system-ui'; ctx.fillText(label, position.x * cellSize + 12, position.y * cellSize - 12); ctx.restore();
}

function drawNetwork() {
  const now = performance.now();
  for (const link of state.links) {
    if (!link.enabled) continue;
    const a = state.positions[link.a], b = state.positions[link.b];
    const hot = (hotLinks.get(link.a + ':' + link.b) || 0) > now;
    ctx.strokeStyle = hot ? '#e7f6efaa' : '#5adbc640'; ctx.lineWidth = hot ? 2 : 1;
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
    ctx.save(); ctx.strokeStyle = hot ? '#e7f6efcc' : link.active ? '#5adbc680' : '#8194a755';
    ctx.lineWidth = hot ? 2.5 : 1;
    if (!link.active) ctx.setLineDash([4, 7]);
    ctx.beginPath(); ctx.moveTo(own.x * cellSize, own.y * cellSize);
    ctx.lineTo(link.position.x * cellSize, link.position.y * cellSize); ctx.stroke(); ctx.restore();
  }
}

function drawFlash(position) {
  ctx.save(); ctx.strokeStyle = '#e7f6efbb'; ctx.lineWidth = 2;
  ctx.beginPath(); ctx.arc(position.x * cellSize, position.y * cellSize, 15, 0, Math.PI * 2); ctx.stroke(); ctx.restore();
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
    const v = reports.get(n); const row = document.createElement('div'); row.className = 'report';
    row.style.color = v ? agedColor('#5adbc6', v.amount) : '#8194a7';
    row.textContent = shortName(n) + ' · ' + (n === selected ? 'own navigation' : v ? 'last report ' + v.age.toFixed(0) + 's ago' + (v.stale ? ' · silent / stale' : '') : 'unknown') + (v ? ' · battery ' + v.battery + '%' : '');
    return row;
  }));
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
