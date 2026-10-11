import { request } from "./api.js";

const names = ["A", "B", "C"];
const keys = ["a/config", "m/status", "z/version"];
const ranges = ["[−∞, m)", "[m, z)", "[z, +∞)"];
const scenes = [
  {
    title: "Local state. No central server.",
    description:
      "Each box runs an independent Rust replica. Start with one local write on A.",
    action: "Write on A",
    speaker:
      "« Chaque instance embarque son propre store. Les lectures sont locales ; aucune instance n’est le maître. Je vais écrire trois clés sur A. » Présenter les trois cases avant de cliquer.",
  },
  {
    title: "One write, three local copies.",
    description:
      "A writes locally. Its runtime propagates the dated entries to B and C.",
    action: "Isolate C + write",
    speaker:
      "« Le code applicatif écrit sur A. Le runtime propage les entrées ; B et C possèdent ensuite leur propre copie. En dessous, RSOS résume le contenu ordonné par plages. » Attendre l’accord des trois répliques, puis annoncer deux écritures indépendantes.",
  },
  {
    title: "Disconnected. Still writable.",
    description:
      "A writes “alpha”; isolated C writes “charlie” to the same key. B can learn from A.",
    action: "Reconnect C",
    speaker:
      "« C ne communique plus, mais peut écrire. A et C ont modifié la même clé sans voir l’autre écriture. Regardez les valeurs, leurs stamps et la seule plage qui diverge. » Laisser quelques secondes pour comparer. Ne pas présenter LWW comme une résolution métier.",
  },
  {
    title: "Reconnect. Compare. Merge.",
    description:
      "The real protocol resumes. The greatest stamp wins; convergence is observed, not scripted.",
    action: "Waiting for convergence…",
    speaker:
      "« Je rétablis uniquement les liens. Le protocole compare les résumés et échange du contenu ; LWW choisit le plus grand stamp. Quand les entrées datées sont identiques, la vue constate la convergence. » Terminer par les limites : conflit perdant écrasé, pas de consensus, transport en mémoire.",
  },
];
const $ = (id) => document.getElementById(id);
const element = (tag, text, className) => {
  const node = document.createElement(tag);
  if (text !== undefined) node.textContent = text;
  if (className) node.className = className;
  return node;
};
function signature(summary) {
  const node = element(
    "span",
    `${summary.count} · ${summary.fingerprint.slice(0, 12)}`,
    "fingerprint",
  );
  node.title = `${summary.count} dated entries · ${summary.fingerprint}`;
  node.tabIndex = 0;
  return node;
}
let previous,
  busy = false;
const events = [];
function event(text) {
  events.unshift(text);
  events.length = Math.min(events.length, 12);
  $("events").replaceChildren(...events.map((text) => element("li", text)));
}
function render(state) {
  const scene = scenes[state.stage];
  $("title").textContent = scene.title;
  $("description").textContent = scene.description;
  $("speaker").textContent = scene.speaker;
  $("next").textContent =
    state.stage === 3 && state.equal
      ? "Converged · reset to repeat"
      : scene.action;
  $("next").disabled = busy || !state.ready || state.stage === 3;
  $("reset").disabled = busy;
  [...$("steps").children].forEach((node, i) => {
    node.classList.toggle("active", i === state.stage);
    if (i === state.stage) node.setAttribute("aria-current", "step");
    else node.removeAttribute("aria-current");
  });
  $("agreement").textContent = state.equal
    ? "Dated entries agree"
    : "Dated entries differ";
  $("agreement").classList.toggle("diverged", !state.equal);
  const changed =
    !previous ||
    state.isolated !== previous.isolated ||
    state.nodes.some(
      (node, id) =>
        JSON.stringify(node.entries) !==
        JSON.stringify(previous.nodes[id].entries),
    );
  if (changed)
    $("replicas").replaceChildren(
      ...state.nodes.map((replica, id) => {
        const box = element("article", undefined, "replica");
        box.dataset.replica = names[id];
        const heading = element("div", undefined, "replica-title");
        heading.append(
          element("strong", names[id]),
          element(
            "small",
            id === 2 && state.isolated
              ? "ISOLATED · running"
              : "CONNECTED · running",
          ),
        );
        const local = element("div", undefined, "local");
        local.append(element("div", "ReplicatedMap · local snapshot", "layer"));
        const entries = element("div", undefined, "entries");
        for (const key of keys) {
          const entry = replica.entries.find((entry) => entry.key === key);
          const row = element("div", undefined, "entry");
          row.dataset.key = key;
          const value = entry?.value ?? "—";
          row.append(
            element("code", key),
            element("span", value, `value ${value}`),
          );
          entries.append(row);
        }
        local.append(entries);
        const stamp = replica.entries.find(
          (entry) => entry.key === "m/status",
        )?.stamp;
        local.append(
          element(
            "div",
            stamp
              ? `m/status stamp: ${stamp.physical}:${stamp.logical} @${stamp.node}`
              : "No dated entries yet",
            "stamp",
          ),
        );
        const backend = element("div", undefined, "backend");
        backend.append(
          element("div", "RSOS · ordered store · full range", "layer"),
          signature(replica.root),
        );
        box.append(
          heading,
          local,
          backend,
          element(
            "div",
            `reconcile-rs runtime · ${replica.rounds} rounds initiated`,
            "runtime",
          ),
        );
        return box;
      }),
    );
  [...$("replicas").children].forEach((box, id) => {
    box.querySelector(".runtime").textContent =
      `reconcile-rs runtime · ${state.nodes[id].rounds} rounds initiated`;
  });
  $("links").replaceChildren(
    ...[
      [0, 1],
      [0, 2],
      [1, 2],
    ].map(([a, b]) => {
      const directions = state.links.filter(
        (link) =>
          (link.a === a && link.b === b) || (link.a === b && link.b === a),
      );
      const connected = directions.every((link) => link.connected);
      const count = directions.reduce((sum, link) => sum + link.datagrams, 0);
      const bytes = directions.reduce((sum, link) => sum + link.bytes, 0);
      const node = element(
        "div",
        `${names[a]} ↔ ${names[b]} · ${connected ? "open" : "cut"}`,
        `link ${connected ? "" : "cut"}`,
      );
      node.append(element("span", `${count} rx · ${bytes} B`));
      return node;
    }),
  );
  if (changed)
    $("ranges").replaceChildren(
      ...ranges.map((label, i) => {
        const summaries = state.nodes.map((node) => node.ranges[i]);
        const different = summaries.some(
          (s) =>
            s.count !== summaries[0].count ||
            s.fingerprint !== summaries[0].fingerprint,
        );
        const row = element("tr", undefined, different ? "different" : "");
        row.append(element("td", label));
        summaries.forEach((summary) => {
          const cell = element("td");
          cell.append(signature(summary));
          row.append(cell);
        });
        return row;
      }),
    );
  if (previous && previous.stage !== state.stage) {
    const actions = [
      "",
      "Local bulk insert on A",
      "Cut C links; independent writes on A and C",
      "Reconnected C; reseeded peer discovery",
    ];
    event(actions[state.stage]);
  }
  state.nodes.forEach((node, id) => {
    if (
      !previous ||
      JSON.stringify(node.entries) ===
        JSON.stringify(previous.nodes[id].entries)
    )
      return;
    const value = node.entries.find((entry) => entry.key === "m/status")?.value;
    event(`${names[id]} snapshot observed: m/status = ${value ?? "absent"}`);
  });
  if (previous && !previous.equal && state.equal)
    event("Exact dated-entry agreement observed across A, B and C");
  previous = state;
}
async function command(path) {
  busy = true;
  $("next").disabled = $("reset").disabled = true;
  try {
    if (path === "reset") {
      previous = undefined;
      events.length = 0;
      $("events").replaceChildren();
    }
    const state = await request(path, "POST");
    busy = false;
    render(state);
    $("error").textContent = "";
  } catch (error) {
    $("error").textContent = String(error);
  } finally {
    busy = false;
  }
}
$("next").onclick = () => command("next");
$("reset").onclick = () => command("reset");
async function poll() {
  try {
    if (!busy) {
      render(await request("state"));
      $("error").textContent = "";
    }
  } catch (error) {
    $("error").textContent = String(error);
  }
  setTimeout(poll, 150);
}
poll();
