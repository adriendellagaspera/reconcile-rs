import { request } from "./api.js";
import { floodKnowledge, freshness } from "./knowledge.js";
const $ = (id) => document.getElementById(id),
  canvas = $("map"),
  ctx = canvas.getContext("2d");
let state,
  selected = 0,
  truth = false,
  showLinks = false,
  follow = true,
  zoom = 1,
  offset = { x: 0, y: 0 },
  entities = [],
  choice = null,
  busy = false,
  moved = false;
const pointers = new Map(),
  tracks = new Map();
const names = {
  people: "Possible people",
  bridge: "Bridge damage",
  road: "Blocked road",
};
const colors = {
  suspected: "#d09a38",
  confirmed: "#cd6552",
  dismissed: "#778878",
};
const name = (n) =>
  n === state?.center ? "Response center" : "UAV " + (n + 1);
const river = (y) => 0.6 + 1.7 * y + 0.8 * Math.sin(((y - 6) * Math.PI) / 3.5);
const villages = [
  { x: 10.6, y: 6, n: "Willow hamlet" },
  { x: 17.9, y: 9.5, n: "Mill village" },
  { x: 22.6, y: 13, n: "Eastbank" },
  { x: 7, y: 15, n: "Ridge farm" },
];
function view() {
  const b = canvas.getBoundingClientRect(),
    cover = getComputedStyle(canvas).objectFit === "cover",
    scale = (cover ? Math.max : Math.min)(b.width / 960, b.height / 600);
  return {
    b,
    scale,
    x: (960 - b.width / scale) / 2,
    y: (600 - b.height / scale) / 2,
    w: b.width / scale,
    h: b.height / scale,
  };
}
function constrain() {
  const v = view();
  const clamp = (a, start, size, world) =>
    Math.min(
      Math.max(0, start),
      Math.max(Math.min(0, start + size - world * zoom), a),
    );
  offset.x = clamp(offset.x, v.x, v.w, 960);
  offset.y = clamp(offset.y, v.y, v.h, 600);
}
function center() {
  if (!state) return;
  const p = state.positions[selected];
  offset = { x: 480 - p.x * 30 * zoom, y: 300 - p.y * 30 * zoom };
  constrain();
}
function point(e) {
  const v = view();
  return {
    x: ((e.clientX - v.b.left) / v.scale + v.x - offset.x) / zoom,
    y: ((e.clientY - v.b.top) / v.scale + v.y - offset.y) / zoom,
  };
}
async function command(action) {
  if (busy) return;
  busy = true;
  try {
    state = await request(action, "POST");
    $("error").textContent = "";
    if (action === "demo") {
      tracks.clear();
      choice = null;
    }
    draw();
  } catch (e) {
    $("error").textContent = e.message;
  } finally {
    busy = false;
  }
}
$("play").onclick = () => command(state.playing ? "pause" : "play");
$("restart").onclick = () => command("demo");
$("speed").onchange = () => command("speed/" + $("speed").value);
$("heal").onclick = () => command("heal");
$("disconnect").onclick = () =>
  command(
    "peer/" +
      selected +
      "/" +
      (state.peer_states[selected] === "active" ? "offline" : "online"),
  );
$("truth").onclick = () => {
  truth = !truth;
  draw();
};
$("links").onclick = () => {
  showLinks = !showLinks;
  draw();
};
$("map-center").onclick = () => {
  follow = true;
  center();
  draw();
};
$("map-overview").onclick = () => {
  follow = false;
  const on = $("map-wrap").classList.toggle("overview");
  $("map-overview").setAttribute("aria-pressed", String(on));
  zoom = 1;
  offset = { x: 0, y: 0 };
  constrain();
  draw();
};
const mobile = matchMedia("(max-width:700px)");
function sheet() {
  const open = $("sheet").classList.contains("expanded");
  $("inspector-toggle").setAttribute("aria-expanded", String(open));
  $("inspector-content").inert = mobile.matches && !open;
}
$("inspector-toggle").onclick = () => {
  $("sheet").classList.toggle("expanded");
  sheet();
};
mobile.addEventListener("change", sheet);
sheet();
new ResizeObserver(() => {
  if (follow) center();
  else constrain();
  draw();
}).observe(canvas);
canvas.onpointerdown = (e) => {
  moved = false;
  pointers.set(e.pointerId, { x: e.clientX, y: e.clientY });
  canvas.setPointerCapture(e.pointerId);
};
canvas.onpointermove = (e) => {
  if (!pointers.has(e.pointerId)) return;
  const before = [...pointers.values()],
    old = pointers.get(e.pointerId);
  pointers.set(e.pointerId, { x: e.clientX, y: e.clientY });
  const v = view();
  follow = false;
  if (pointers.size === 2) {
    const after = [...pointers.values()],
      d = (p) => Math.hypot(p[0].x - p[1].x, p[0].y - p[1].y);
    zoom = Math.max(1, Math.min(5, (zoom * d(after)) / Math.max(1, d(before))));
  } else {
    offset.x += (e.clientX - old.x) / v.scale;
    offset.y += (e.clientY - old.y) / v.scale;
  }
  moved = true;
  constrain();
  draw();
};
canvas.onpointerup = canvas.onpointercancel = (e) =>
  pointers.delete(e.pointerId);
canvas.onclick = (e) => {
  if (moved) return;
  const p = point(e);
  const hit = entities
    .map((a) => ({
      a,
      d: Math.hypot(a.position.x * 30 - p.x, a.position.y * 30 - p.y),
    }))
    .sort((a, b) => a.d - b.d)[0];
  if (hit?.d < 18) {
    if (hit.a.type === "aircraft") {
      selected = hit.a.id;
      choice = null;
      follow = true;
      center();
    } else choice = hit.a.id;
    draw();
  }
};
canvas.addEventListener(
  "wheel",
  (e) => {
    e.preventDefault();
    const p = point(e),
      old = zoom;
    zoom = Math.max(1, Math.min(5, zoom * Math.exp(-e.deltaY * 0.002)));
    offset.x += p.x * (old - zoom);
    offset.y += p.y * (old - zoom);
    follow = false;
    constrain();
    draw();
  },
  { passive: false },
);
function line(points, color, width = 1, dash = []) {
  ctx.save();
  ctx.strokeStyle = color;
  ctx.lineWidth = width;
  ctx.setLineDash(dash);
  ctx.beginPath();
  points.forEach((p, i) =>
    i ? ctx.lineTo(p.x * 30, p.y * 30) : ctx.moveTo(p.x * 30, p.y * 30),
  );
  ctx.stroke();
  ctx.restore();
}
function label(text, p, color = "#567062", dy = 0) {
  ctx.save();
  ctx.fillStyle = color;
  ctx.font = "10px system-ui";
  ctx.fillText(text, p.x * 30 + 10, p.y * 30 + dy);
  ctx.restore();
}
function terrain() {
  ctx.fillStyle = "#e1e8d5";
  ctx.fillRect(0, 0, 960, 600);
  for (let y = 0; y < 20; y++)
    for (let x = 0; x < 32; x++) {
      const t = Math.sin(x * 2.4 + y * 0.9) + Math.cos(y * 1.7 - x * 0.4);
      ctx.fillStyle = t > 1 ? "#ceddc4" : t < -0.8 ? "#e9e9d1" : "#dce5cf";
      ctx.fillRect(x * 30, y * 30, 30, 30);
    }
  for (const [x, y] of [
    [4, 3],
    [25, 4],
    [27, 16],
    [7, 17],
  ]) {
    ctx.save();
    ctx.fillStyle = "#b8ceb2";
    ctx.beginPath();
    ctx.ellipse(x * 30, y * 30, 75, 55, 0.3, 0, 7);
    ctx.fill();
    ctx.strokeStyle = "#9db89a88";
    for (let r = 25; r < 70; r += 15) {
      ctx.beginPath();
      ctx.ellipse(x * 30, y * 30, r, r * 0.7, 0.3, 0, 7);
      ctx.stroke();
    }
    ctx.restore();
  }
  const r = Array.from({ length: 81 }, (_, i) => ({
    x: river(i / 4),
    y: i / 4,
  }));
  line(r, "#a6cddd", 65);
  line(r, "#79b7ca", 20);
  line(r, "#b5dce7", 5);
  const roads = [
    [
      { x: 2, y: 6 },
      { x: 10.6, y: 6 },
      { x: 16.6, y: 9.5 },
      { x: 28, y: 9.5 },
    ],
    [
      { x: 4, y: 18 },
      { x: 7, y: 15 },
      { x: 17.9, y: 9.5 },
      { x: 22.6, y: 13 },
      { x: 30, y: 17 },
    ],
  ];
  for (const road of roads) {
    line(road, "#c6bdaa", 7);
    line(road, "#fff9e7", 4);
  }
  for (const v of villages) {
    for (let i = 0; i < 8; i++) {
      ctx.fillStyle = i % 2 ? "#bd9f83" : "#c7ae8c";
      ctx.fillRect(
        (v.x + (i % 4) * 0.15 - 0.2) * 30,
        (v.y + Math.floor(i / 4) * 0.18 - 0.1) * 30,
        3,
        3,
      );
    }
    label(v.n, { x: v.x, y: v.y }, "#746957", -12);
  }
  label("RIVER VALLEY · FLOOD RESPONSE", { x: 2, y: 1.4 }, "#7d9279");
  label("Floodplain", { x: river(16), y: 16 }, "#4e90a3");
  ctx.fillStyle = "#315c4d";
  ctx.fillRect(4 * 30 - 6, 10 * 30 - 6, 12, 12);
  label("RESPONSE BASE", { x: 4, y: 10 }, "#315c4d", -10);
  line(
    [
      { x: 2, y: 18.8 },
      { x: 7, y: 18.8 },
    ],
    "#617768",
    2,
  );
  label("5 km", { x: 3.3, y: 18.4 });
}
function draw() {
  if (!state?.flood) return;
  if (follow) center();
  const known = floodKnowledge(state.nodes[selected], state.ticks);
  const cc = floodKnowledge(state.nodes[state.center], state.ticks);
  const own = state.positions[selected];
  $("perspective").textContent = truth
    ? "SIMULATOR · GROUND TRUTH"
    : name(selected).toUpperCase() + " · LOCAL KNOWLEDGE";
  $("inspector-toggle").textContent = name(selected);
  $("play").textContent = state.playing ? "Pause" : "Play";
  $("speed").value = state.mission.speed;
  $("time").textContent =
    Math.floor(state.seconds / 60) +
    ":" +
    String(Math.floor(state.seconds % 60)).padStart(2, "0");
  const offline = state.peer_states[state.center] !== "active";
  $("status").classList.toggle("offline", offline);
  $("status").textContent = offline
    ? "Response-center uplink unavailable. Aircraft can retain and share observations."
    : "Response-center uplink restored. Reports propagate over available links.";
  $("phase").textContent = state.scripted
    ? state.ticks < 40
      ? "Search isolated settlements"
      : state.ticks < 600
        ? "Uplink interrupted · local knowledge continues"
        : state.ticks < 1800
          ? "Uplink restored · findings reach the response center"
          : state.divergent_keys
            ? "Mission paused · reconciling retained reports"
            : "Reports reconciled · exercise complete"
    : "Manual exploration · local decisions";
  $("center-status").textContent =
    !truth && selected !== state.center
      ? "Select Center to inspect the reports it has actually received."
      : cc.pois.length
        ? cc.pois
            .map(
              (p) =>
                names[p.kind] +
                " · " +
                p.finding +
                " · " +
                Math.round(p.age) +
                "s old",
            )
            .join(" / ")
        : "No findings received yet. Select an aircraft to inspect its local knowledge.";
  $("fleet").replaceChildren(
    ...Array.from({ length: state.center + 1 }, (_, id) => {
      const b = document.createElement("button");
      b.textContent = id === state.center ? "Center" : "U" + (id + 1);
      b.classList.toggle("active", id === selected);
      b.onclick = () => {
        selected = id;
        truth = false;
        choice = null;
        follow = true;
        draw();
      };
      return b;
    }),
  );
  const pois = truth
    ? state.flood.truth_pois.map((p) => ({
        ...p,
        finding: p.genuine ? "confirmed" : "dismissed",
        source: null,
        age: 0,
      }))
    : known.pois;
  $("reports").replaceChildren(
    ...pois.map((p) => {
      const b = document.createElement("button");
      b.className = "poi";
      const a = document.createElement("strong");
      a.textContent =
        (p.finding === "dismissed"
          ? "× "
          : p.finding === "confirmed"
            ? "● "
            : "◌ ") +
        names[p.kind] +
        " · " +
        p.finding;
      const small = document.createElement("small");
      small.textContent = truth
        ? "Simulator truth"
        : name(p.source) +
          " · " +
          Math.round(p.age) +
          "s old · " +
          known.reports.filter((r) => r.id === p.id).length +
          " source(s)";
      b.append(a, small);
      b.onclick = () => {
        choice = p.id;
        follow = false;
        offset = {
          x: 480 - p.position.x * 30 * zoom,
          y: 300 - p.position.y * 30 * zoom,
        };
        constrain();
        draw();
      };
      return b;
    }),
  );
  if (!pois.length) {
    const e = document.createElement("div");
    e.className = "empty";
    e.textContent =
      "No finding in this replica. The terrain is preloaded; observations must be acquired or received.";
    $("reports").append(e);
  }
  const chosen = pois.find((p) => p.id === choice);
  $("object-detail").textContent = chosen
    ? chosen.finding === "suspected"
      ? "Unconfirmed visual cue. A verification aircraft may investigate."
      : chosen.finding === "dismissed"
        ? "Verification dismissed this cue. Aircraft receiving the correction resume search."
        : "Confirmed simulated observation. Reports must still reach the response center."
    : "";
  const intents = truth
    ? state.flood.aircraft.map((a, recipient) => ({ ...a, recipient }))
    : known.intents;
  $("intents").replaceChildren(
    ...intents.map((i) => {
      const p = document.createElement("div");
      p.className = "metric";
      p.textContent =
        name(i.recipient) + " · " + i.role + (i.poi ? " · POI " + i.poi : "");
      return p;
    }),
  );
  if (!intents.length)
    $("intents").textContent = "No rerouting intention received.";
  $("truth").classList.toggle("active", truth);
  $("links").classList.toggle("active", showLinks);
  $("diagnostics").textContent =
    state.nodes.length +
    " actual replicas · " +
    state.loss +
    "% loss · " +
    state.bandwidth_kbps +
    " kbit/s · " +
    state.divergent_keys +
    " divergent keys. Aircraft motion is a kinematic illustration; on-station observation is drawn stationary.";
  ctx.resetTransform();
  ctx.clearRect(0, 0, 1920, 1200);
  ctx.setTransform(zoom * 2, 0, 0, zoom * 2, offset.x * 2, offset.y * 2);
  terrain();
  entities = [];
  const vehicles = truth
    ? state.positions.map((position, source) => ({
        position,
        source,
        ...freshness(state.ticks, state.ticks),
      }))
    : known.vehicles
        .filter((v) => v.source !== selected)
        .concat([
          {
            position: own,
            source: selected,
            ...freshness(state.ticks, state.ticks),
          },
        ]);
  if (showLinks) {
    const reported = new Map(vehicles.map((v) => [v.source, v.position]));
    reported.set(state.center, state.command_position);
    for (const l of state.links) {
      if (
        !truth &&
        !state.direct_peers[selected].some(
          (p) =>
            p.peer === (l.a === selected ? l.b : l.a === selected ? l.a : -1),
        )
      )
        continue;
      if (!truth && l.a !== selected && l.b !== selected) continue;
      const a = reported.get(l.a),
        b = reported.get(l.b);
      if (a && b)
        line(
          [a, b],
          truth ? (l.enabled ? "#4c8a6977" : "#a7786255") : "#6b927377",
          0.8,
          truth
            ? l.enabled
              ? []
              : [4, 4]
            : state.direct_peers[selected].some(
                  (p) =>
                    p.peer === (l.a === selected ? l.b : l.a) &&
                    p.age_ms < 5000,
                )
              ? []
              : [4, 4],
        );
    }
  }
  for (const p of pois) {
    entities.push({ type: "poi", id: p.id, position: p.position });
    ctx.save();
    ctx.translate(p.position.x * 30, p.position.y * 30);
    ctx.strokeStyle = colors[p.finding];
    ctx.fillStyle = colors[p.finding];
    ctx.lineWidth = 2;
    if (p.finding === "dismissed") {
      line(
        [
          { x: -0.15, y: -0.15 },
          { x: 0.15, y: 0.15 },
        ],
        colors.dismissed,
        2,
      );
      line(
        [
          { x: 0.15, y: -0.15 },
          { x: -0.15, y: 0.15 },
        ],
        colors.dismissed,
        2,
      );
    } else {
      ctx.beginPath();
      ctx.arc(0, 0, 7, 0, 7);
      if (p.finding === "confirmed") ctx.fill();
      else {
        ctx.setLineDash([3, 2]);
        ctx.stroke();
      }
    }
    ctx.restore();
    label(names[p.kind], p.position, colors[p.finding], -32);
  }
  for (const v of vehicles) {
    const id = v.source;
    if (id === state.center) continue;
    const key = selected + ":" + truth + ":" + id;
    const trail = tracks.get(key) || [];
    if (
      !trail.length ||
      Math.hypot(trail.at(-1).x - v.position.x, trail.at(-1).y - v.position.y) >
        0.03
    )
      trail.push(v.position);
    if (trail.length > 180) trail.shift();
    tracks.set(key, trail);
    line(trail, id === selected ? "#2c756888" : "#84978466", 1.5);
    const intention = intents.find((i) => i.recipient === id),
      target = pois.find((p) => p.id === intention?.poi);
    if (target) line([v.position, target.position], "#b28a5466", 1, [3, 4]);
    entities.push({ type: "aircraft", id, position: v.position });
    ctx.save();
    ctx.translate(v.position.x * 30, v.position.y * 30);
    ctx.fillStyle = id === selected ? "#235f50" : "#658479";
    ctx.strokeStyle = "#fff";
    ctx.lineWidth = 1.5;
    ctx.globalAlpha = v.alpha;
    const trailLast =
      trail.length > 1
        ? trail.at(-2)
        : { x: v.position.x - 1, y: v.position.y };
    ctx.rotate(
      Math.atan2(v.position.y - trailLast.y, v.position.x - trailLast.x) +
        Math.PI / 2,
    );
    ctx.beginPath();
    ctx.moveTo(0, -10);
    ctx.lineTo(3, -1);
    ctx.lineTo(11, 5);
    ctx.lineTo(2, 4);
    ctx.lineTo(2, 9);
    ctx.lineTo(-2, 9);
    ctx.lineTo(-2, 4);
    ctx.lineTo(-11, 5);
    ctx.lineTo(-3, -1);
    ctx.closePath();
    ctx.fill();
    ctx.stroke();
    ctx.restore();
    label(
      "U" +
        (id + 1) +
        (intention && intention.role !== "search"
          ? " · " + intention.role
          : ""),
      v.position,
      "#315d4f",
      18,
    );
  }
  if (selected !== state.center) {
    ctx.save();
    ctx.strokeStyle = "#315c4d66";
    ctx.beginPath();
    ctx.arc(own.x * 30, own.y * 30, state.flood.camera_radius_km * 30, 0, 7);
    ctx.stroke();
    ctx.restore();
  }
}
async function poll() {
  try {
    if (!busy) {
      state = await request("state");
      draw();
    }
  } catch (e) {
    $("error").textContent = e.message;
  }
  setTimeout(poll, 250);
}
poll();
