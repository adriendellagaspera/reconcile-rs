import { freshness, knowledge } from './knowledge.js';

const $ = id => document.getElementById(id);
const ctx = $('map').getContext('2d');
const cellSize = 30;
let state, previous;
let sampledAt = performance.now();
let trafficRate = 0;
const hotLinks = new Map();
let controlsBusy = false;
const nodeName = n => n === state.center ? 'COMMAND CENTER' : 'GLIDER-' + String(n + 1).padStart(2, '0');
const shortName = n => n === state.center ? 'CC' : 'G' + (n + 1);
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
  } else { trafficRate = 0; hotLinks.clear(); }
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
  } catch (error) { $('error').textContent = error.message; }
  finally { controlsBusy = false; }
}

for (const button of document.querySelectorAll('[data-action]')) {
  button.onclick = () => command(button.id === 'play' ? (state.playing ? 'pause' : 'play') : button.dataset.action);
}
for (const button of document.querySelectorAll('[data-peer]')) {
  button.onclick = () => command('peer/' + $('peer').value + '/' + button.dataset.peer);
}
$('link-toggle').onclick = () => command('link/' + $('link-a').value + '/' + $('link-b').value + '/toggle');
$('range').onchange = () => command('range/' + $('range').value);
$('view').onchange = $('node').onchange = draw;
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
  if (mode === 'command') $('node').value = state.center;
  $('node').disabled = mode === 'command';
  $('status').textContent = state.partitioned ? 'DISCONNECTED COMPONENTS' : state.divergent_keys ? 'RECONCILING' : 'CONVERGED';
  $('title').textContent = mode === 'truth' ? 'SIMULATOR / GROUND TRUTH' : mode === 'cluster' ? 'SIMULATOR / PAIR CONNECTIVITY' : nodeName(selected) + ' / ' + (mode === 'command' ? 'LOCAL SYNTHESIS' : 'ONBOARD KNOWLEDGE');
  $('keys').textContent = entries.length + ' local keys';
  $('map-note').textContent = local ? 'This replica only · gray = older · dashed = stale / last known' : mode === 'cluster' ? 'Distance-based links · flashes = delivered datagrams · no fixed relay' : 'Synthetic truth · actual failure states · click a peer to inspect';
  ctx.clearRect(0, 0, 960, 600);
  for (let y = 0; y < 20; y++) for (let x = 0; x < 32; x++) {
    const cell = known.terrain.get(x + ',' + y);
    const visible = !local || cell;
    const isLand = local ? cell?.land : state.truth_map[y * 32 + x];
    ctx.fillStyle = visible ? (isLand ? '#40594e' : '#163442') : '#0a1420';
    ctx.fillRect(x * cellSize, y * cellSize, cellSize, cellSize);
    ctx.strokeStyle = '#ffffff06'; ctx.strokeRect(x * cellSize, y * cellSize, cellSize, cellSize);
  }
  if (mode === 'cluster') drawNetwork();
  if (local) {
    known.vehicles.forEach(v => { if (v.source !== selected) drawVehicle(v.position, v.source, v, false); });
    // Only the observer's own navigation position is locally available without replication.
    drawVehicle(state.positions[selected], selected, freshness(state.ticks, state.ticks, 30), true);
    known.reports.forEach(report => drawContact(report.position, report, report === known.contact ? 'Contact · ' + report.age.toFixed(0) + 's' : '', report === known.contact));
  } else {
    state.positions.forEach((p, n) => drawVehicle(p, n, freshness(state.ticks, state.ticks), n === selected, state.peer_states[n]));
    if (state.truth_contact) drawContact(state.truth_contact, freshness(state.ticks, state.ticks), 'Contact / truth', true);
  }
  $('sectors').textContent = known.sectors + ' sectors known locally';
  $('contact').textContent = known.contact ? 'Latest: ' + shortName(known.contact.source) + ' · ' + known.contact.age.toFixed(0) + 's old' + (known.contact.stale ? ' · STALE' : '') + '. ' + known.reports.length + ' source reports available. No averaging.' : 'No observation in this replica';
  $('reports').replaceChildren(...known.reports.map(r => {
    const row = document.createElement('div'); row.className = 'report';
    row.style.color = agedColor('#f6bc73', r.amount);
    row.textContent = shortName(r.source) + ' · ' + r.age.toFixed(0) + 's · (' + r.position.x.toFixed(1) + ', ' + r.position.y.toFixed(1) + ')' + (r.stale ? ' · stale' : '');
    return row;
  }));
  drawFleetKnowledge(known, selected);
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
  ctx.fillText(shortName(n) + (truthState && truthState !== 'active' ? ' / ' + truthState : own ? ' / self' : ' / ' + fresh.age.toFixed(0) + 's'), 11, 4);
  if (truthState === 'stopped') { ctx.beginPath(); ctx.moveTo(-10, -10); ctx.lineTo(10, 10); ctx.moveTo(10, -10); ctx.lineTo(-10, 10); ctx.stroke(); }
  ctx.restore();
}

function drawContact(position, fresh, label, primary) {
  const tint = agedColor('#f6bc73', fresh.amount);
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
  if (state.storm) {
    ctx.fillStyle = '#df8e6e15'; ctx.fillRect(15.5 * cellSize, 0, cellSize, 600);
    ctx.strokeStyle = '#df8e6e77'; ctx.setLineDash([8, 10]); ctx.beginPath(); ctx.moveTo(16 * cellSize, 0); ctx.lineTo(16 * cellSize, 600); ctx.stroke(); ctx.setLineDash([]);
    ctx.fillStyle = '#dfb594'; ctx.font = '12px system-ui'; ctx.fillText('WEATHER FRONT', 16 * cellSize + 10, 35);
  }
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
