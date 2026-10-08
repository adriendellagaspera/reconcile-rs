'use strict';
const $ = id => document.getElementById(id);
const ctx = $('map').getContext('2d');
const colors = ['#5adbc6', '#9c9cf2'];
let state;
let previous;
let sampledAt = performance.now();
let trafficRate = 0;
let hotLinks = new Map();
let controlsBusy = false;
const cellSize = 30;
const nodeName = n => 'GLIDER-' + String(n + 1).padStart(2, '0');
const nodeGroup = n => Number(n >= Math.floor(state.nodes.length / 2));

function selectNode(n) {
  $('node').value = n;
  $('view').value = 'local';
  draw();
}

function apply(next) {
  const now = performance.now();
  if (previous && next.delivered_bytes >= previous.delivered_bytes && now - sampledAt > 0) {
    trafficRate = (next.delivered_bytes - previous.delivered_bytes) * 1000 / (now - sampledAt);
    const old = new Map(previous.links.map(l => [l.a + ':' + l.b, l.bytes]));
    next.links.forEach(l => {
      if (l.bytes > (old.get(l.a + ':' + l.b) || 0)) hotLinks.set(l.a + ':' + l.b, now + 450);
    });
  } else { trafficRate = 0; hotLinks.clear(); }
  sampledAt = now;
  previous = next;
  state = next;
  if ($('node').options.length !== state.nodes.length) {
    $('node').replaceChildren(...state.nodes.map((_, n) => new Option(nodeName(n), n)));
  }
  draw();
}

async function command(action) {
  const response = await fetch('/' + action, { method: 'POST' });
  if (!response.ok) throw Error('Control failed: ' + response.status);
  apply(await response.json());
}

for (const button of document.querySelectorAll('[data-action]')) {
  button.onclick = async () => {
    if (controlsBusy) return;
    controlsBusy = true;
    try {
      const action = button.id === 'play' ? (state.playing ? 'pause' : 'play') : button.dataset.action;
      await command(action);
      $('error').textContent = '';
    } catch (error) { $('error').textContent = error.message; }
    finally { controlsBusy = false; }
  };
}
$('view').onchange = $('node').onchange = draw;
$('west').onclick = () => selectNode(0);
$('east').onclick = () => selectNode(Math.floor(state.nodes.length / 2));
$('map').onclick = event => {
  if (!state || $('view').value === 'local') return;
  const bounds = $('map').getBoundingClientRect();
  const x = (event.clientX - bounds.left) * 960 / bounds.width;
  const y = (event.clientY - bounds.top) * 600 / bounds.height;
  const nearest = state.positions.map((p, n) => ({ n, distance: Math.hypot(x - p.x * cellSize, y - p.y * cellSize) })).sort((a, b) => a.distance - b.distance)[0];
  if (nearest.distance < 25) selectNode(nearest.n);
};

function draw() {
  if (!state) return;
  const selected = Number($('node').value);
  const mode = $('view').value;
  const entries = state.nodes[selected];
  const terrain = new Map(entries.filter(([key]) => key.startsWith('map/')).map(([, value]) => [value.Terrain.x + ',' + value.Terrain.y, value.Terrain]));
  const contacts = entries.filter(([key]) => key.startsWith('contact/')).map(([, value]) => value.Contact);
  const sectors = entries.filter(([key]) => key.startsWith('sector/')).length;
  $('status').textContent = state.partitioned ? 'NETWORK PARTITIONED' : state.divergent_keys ? 'RECONCILING' : 'CONVERGED';
  $('title').textContent = mode === 'truth' ? 'GROUND TRUTH' : mode === 'cluster' ? 'CLUSTER / CONNECTIVITY' : nodeName(selected) + ' / LOCAL KNOWLEDGE';
  $('keys').textContent = entries.length + ' local keys';
  $('map-note').textContent = mode === 'local' ? 'Only this replica’s knowledge · unknown cells stay dark' : mode === 'cluster' ? 'Permitted links · bright flashes = real delivered traffic' : 'Synthetic reality · click a vehicle to inspect its replica';
  ctx.clearRect(0, 0, 960, 600);
  for (let y = 0; y < 20; y++) for (let x = 0; x < 32; x++) {
    const cell = terrain.get(x + ',' + y);
    const visible = mode !== 'local' || cell;
    const isLand = mode === 'local' ? cell?.land : state.truth_map[y * 32 + x];
    ctx.fillStyle = visible ? (isLand ? '#40594e' : '#163442') : '#0a1420';
    ctx.fillRect(x * cellSize, y * cellSize, cellSize, cellSize);
    ctx.strokeStyle = '#ffffff06';
    ctx.strokeRect(x * cellSize, y * cellSize, cellSize, cellSize);
  }
  if (mode === 'cluster') drawNetwork();
  state.positions.forEach((point, n) => {
    // Local knowledge shows only the selected vehicle’s own navigation position.
    if (mode === 'local' && n !== selected) return;
    ctx.save();
    ctx.translate(point.x * cellSize, point.y * cellSize);
    ctx.fillStyle = colors[nodeGroup(n)];
    ctx.strokeStyle = n === selected ? '#ffffff' : '#09121c';
    ctx.lineWidth = 2;
    ctx.beginPath(); ctx.moveTo(0, -9); ctx.lineTo(7, 8); ctx.lineTo(-7, 8); ctx.closePath();
    ctx.fill(); ctx.stroke();
    ctx.fillStyle = '#dbe5ec'; ctx.font = '12px system-ui'; ctx.fillText('G' + (n + 1), 11, 4);
    ctx.restore();
  });
  if (mode === 'local') {
    contacts.forEach(contact => drawContact(contact.position, (state.ticks - contact.seen) * .5, 'Last observation'));
  } else if (state.truth_contact) { drawContact(state.truth_contact, 0, 'Contact / truth'); }
  $('sectors').textContent = sectors + ' sectors known locally';
  $('contact').textContent = contacts.length ? contacts.map(c => 'Seen by G' + (c.source + 1) + ' · ' + Math.max(0, (state.ticks - c.seen) * .5).toFixed(0) + 's old · (' + c.position.x.toFixed(1) + ', ' + c.position.y.toFixed(1) + ')').join('; ') : 'No local observation';
  $('count').textContent = state.nodes.length;
  $('components').textContent = state.partitioned ? 2 : 1;
  $('loss').textContent = state.loss + '%';
  $('budget').textContent = state.datagram_budget >= 1024 ? (state.datagram_budget / 1024).toFixed(1) + ' KiB' : state.datagram_budget + ' B';
  $('lost').textContent = state.loss_dropped + ' / ' + state.loss_offered;
  $('blocked').textContent = state.partition_dropped;
  $('divergent').textContent = state.divergent_keys;
  $('union').textContent = state.union_keys;
  $('rounds').textContent = state.rounds_total;
  const ratio = state.union_keys ? 1 - state.divergent_keys / state.union_keys : 1;
  $('progress').value = ratio;
  $('ratio').textContent = (ratio * 100).toFixed(1) + '% of union keys identical on all replicas';
  $('groups').textContent = state.groups.map(g => (g.group ? 'East' : 'West') + ': ' + (g.keys ? g.same * 100 / g.keys : 100).toFixed(0) + '% internal agreement').join(' · ');
  $('packets').textContent = state.delivered_datagrams;
  $('bytes').textContent = (state.delivered_bytes / 1024).toFixed(1) + ' KiB';
  $('rate').textContent = (trafficRate / 1024).toFixed(1) + ' KiB/s';
  $('phase').textContent = state.phase;
  $('time').textContent = Math.floor(state.seconds / 60) + ':' + String(Math.floor(state.seconds % 60)).padStart(2, '0');
  $('sim-status').textContent = state.playing ? 'running' : 'paused';
  $('play').textContent = state.playing ? 'Pause exploration' : 'Play exploration';
  document.querySelectorAll('[data-act]').forEach(el => el.classList.toggle('active', state.scripted && state.phase.startsWith(el.dataset.act + ' /')));
  drawNodeCards(selected);
}

function drawNetwork() {
  const now = performance.now();
  // Draw the actual two star components and their bridge, without inferred peer links.
  for (const link of state.links) {
    if (!link.enabled) continue;
    const a = state.positions[link.a], b = state.positions[link.b];
    const hot = (hotLinks.get(link.a + ':' + link.b) || 0) > now;
    ctx.strokeStyle = hot ? '#e7f6efaa' : colors[nodeGroup(link.a)] + '55';
    ctx.lineWidth = hot ? 2 : 1;
    ctx.beginPath(); ctx.moveTo(a.x * cellSize, a.y * cellSize); ctx.lineTo(b.x * cellSize, b.y * cellSize); ctx.stroke();
  }
  if (state.partitioned) {
    ctx.strokeStyle = '#df8e6e77'; ctx.setLineDash([8, 10]); ctx.beginPath(); ctx.moveTo(16 * cellSize, 25); ctx.lineTo(16 * cellSize, 575); ctx.stroke(); ctx.setLineDash([]);
    ctx.fillStyle = '#dfb594'; ctx.font = '12px system-ui'; ctx.fillText('PARTITION', 16 * cellSize + 10, 35);
  }
}

function drawContact(position, age, label) {
  ctx.save(); ctx.globalAlpha = Math.max(.3, 1 - age / 90);
  ctx.fillStyle = '#f6bc73'; ctx.beginPath(); ctx.arc(position.x * cellSize, position.y * cellSize, 6, 0, Math.PI * 2); ctx.fill();
  ctx.font = '12px system-ui'; ctx.fillText(label, position.x * cellSize + 12, position.y * cellSize - 12); ctx.restore();
}

function drawNodeCards(selected) {
  let buttons = $('nodes').querySelectorAll('button');
  if (buttons.length !== state.nodes.length) {
    $('nodes').replaceChildren(...state.nodes.map((_, n) => {
      const button = document.createElement('button');
      button.className = 'node'; button.onclick = () => selectNode(n);
      const label = document.createElement('span'); label.textContent = 'G' + (n + 1);
      const detail = document.createElement('small'); label.append(detail);
      const contact = document.createElement('span'); contact.style.color = '#f6bc73';
      button.append(label, contact); return button;
    }));
    buttons = $('nodes').querySelectorAll('button');
  }
  buttons.forEach((button, n) => {
    button.classList.toggle('east', nodeGroup(n) === 1); button.classList.toggle('selected', n === selected);
    button.querySelector('small').textContent = state.nodes[n].length + ' keys';
    const known = state.nodes[n].some(([key]) => key.startsWith('contact/'));
    button.lastChild.textContent = known ? '●' : '';
    button.setAttribute('aria-label', nodeName(n) + ', ' + state.nodes[n].length + ' keys' + (known ? ', contact known' : ''));
  });
}

async function poll() {
  try {
    if (!controlsBusy) {
      const response = await fetch('/state');
      if (!response.ok) throw Error('State unavailable');
      const next = await response.json();
      if (!controlsBusy) apply(next);
      $('error').textContent = '';
    }
  } catch (error) { $('error').textContent = error.message; }
  setTimeout(poll, 250);
}
poll();
