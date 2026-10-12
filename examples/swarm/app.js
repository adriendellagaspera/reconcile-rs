import { request } from "./api.js";
import { Session } from "./session.js";
const names = ["A", "B", "C"];
const $ = (id) => document.getElementById(id);
const make = (tag, text, cls) => {
  const e = document.createElement(tag);
  if (text !== undefined) e.textContent = text;
  if (cls) e.className = cls;
  return e;
};
const hex = (text) =>
  [...new TextEncoder().encode(text)]
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
let previous,
  state,
  pending = 0,
  writes = 0;
const actions = [];
function selection() {
  const start = Number($("start").value),
    end = Number($("end").value),
    peer = Number($("peer").value);
  if (
    !Number.isInteger(start) ||
    !Number.isInteger(end) ||
    start < 0 ||
    start >= end ||
    end > 1024
  )
    throw Error("Range: 0 ≤ from < to ≤ 1024");
  return { start, end, peer };
}
function signature(s) {
  const e = make("code", `${s.count} · ${s.fingerprint.slice(0, 10)}`);
  e.title = s.fingerprint;
  return e;
}
function render(s) {
  state = s;
  $("agreement").textContent = s.equal
    ? "Dated entries agree"
    : `${s.divergent} divergent keys`;
  $("agreement").style.color = s.equal ? "#426c58" : "#92631c";
  const changed =
    !previous ||
    JSON.stringify(s.nodes.map((n) => n.entries)) !==
      JSON.stringify(previous.nodes.map((n) => n.entries));
  if (!previous) {
    $("replicas").replaceChildren(
      ...names.map((name, id) => {
        const card = make("article", undefined, "replica");
        card.dataset.replica = name;
        const head = make("div", undefined, "replica-head");
        const toggle = make("button");
        toggle.type = "button";
        toggle.onclick = () =>
          act(`connect/${id}/${state.nodes[id].online ? 0 : 1}`);
        head.append(make("strong", name), toggle);
        const content = make("div", undefined, "content");
        content.append(
          make("div", undefined, "counts"),
          make("div", undefined, "bins"),
        );
        const caption = make("div", undefined, "bin-caption");
        caption.append(
          make("span", "0"),
          make("span", "1024 · 32 keys / tile"),
        );
        content.append(caption, make("div", undefined, "entries"));
        const backend = make("div", undefined, "backend");
        const runtime = make("div", undefined, "runtime");
        card.append(head, content, backend, runtime);
        return card;
      }),
    );
  }
  const from = Number($("start").value),
    to = Number($("end").value);
  [...$("replicas").children].forEach((card, id) => {
    const node = s.nodes[id],
      button = card.querySelector("button");
    button.textContent = node.online ? "Disconnect" : "Reconnect";
    button.classList.toggle("offline", !node.online);
    button.disabled = pending > 0;
    card.querySelector(".counts").textContent =
      `${node.live} live · ${node.tombstones} tombstones`;
    card.querySelector(".runtime").textContent =
      `Runtime · ${node.rounds} rounds initiated`;
    if (!card.querySelector(".bin")) {
      card.querySelector(".bins").replaceChildren(
        ...node.bins.map((bin, i) => {
          const others = s.nodes.map((n) => n.bins[i]);
          const different = others.some(
            (b) => b.count !== bin.count || b.fingerprint !== bin.fingerprint,
          );
          const entries = node.entries.filter(
            (e) => e.key >= i * 32 && e.key < (i + 1) * 32,
          );
          const deleted =
            entries.length > 0 && entries.every((e) => e.value === null);
          const e = make(
            "button",
            bin.count || "",
            `bin ${bin.count ? "populated" : ""} ${different ? "diverged" : ""} ${deleted ? "deleted" : ""}`,
          );
          e.type = "button";
          e.dataset.bin = i;
          e.title = `[${i * 32}, ${(i + 1) * 32}) · ${bin.count} entries · ${bin.fingerprint}`;
          e.setAttribute(
            "aria-label",
            `Select keys ${i * 32} to ${(i + 1) * 32}`,
          );
          e.onclick = () => {
            $("peer").value = id;
            $("start").value = i * 32;
            $("end").value = (i + 1) * 32;
            render(state);
          };
          return e;
        }),
      );
      card
        .querySelector(".backend")
        .replaceChildren(
          make("span", "RSOS · full range"),
          signature(node.root),
        );
    }
    card.querySelectorAll(".bin").forEach((b, i) => {
      const bin = node.bins[i],
        others = s.nodes.map((n) => n.bins[i]),
        entries = node.entries.filter(
          (e) => e.key >= i * 32 && e.key < (i + 1) * 32,
        );
      b.textContent = bin.count || "";
      b.classList.toggle("populated", bin.count > 0);
      b.classList.toggle(
        "diverged",
        others.some(
          (o) => o.count !== bin.count || o.fingerprint !== bin.fingerprint,
        ),
      );
      b.classList.toggle(
        "deleted",
        entries.length > 0 && entries.every((e) => e.value === null),
      );
      b.classList.toggle("selected", i * 32 < to && (i + 1) * 32 > from);
      b.title = `[${i * 32}, ${(i + 1) * 32}) · ${bin.count} entries · ${bin.fingerprint}`;
    });
    card
      .querySelector(".backend")
      .replaceChildren(make("span", "RSOS · full range"), signature(node.root));
    const selected = node.entries.filter((e) => e.key >= from && e.key < to),
      visible = selected.slice(0, 6);
    const table = make("table", undefined, "preview");
    const header = make("tr");
    header.append(make("th", "Key"), make("th", "Value"), make("th", "Writer"));
    table.append(header);
    for (const entry of visible) {
      const row = make("tr");
      row.dataset.key = entry.key;
      row.append(
        make("td", entry.key),
        make(
          "td",
          entry.value === null
            ? "† deleted"
            : entry.value === ""
              ? '""'
              : entry.value,
          entry.value === null ? "tombstone" : "",
        ),
        make("td", names[entry.stamp.node - 1]),
      );
      row.title = `HLC ${entry.stamp.physical}:${entry.stamp.logical} @${entry.stamp.node}`;
      table.append(row);
    }
    if (!visible.length) {
      const row = make("tr");
      const cell = make("td", "No entries in selection", "muted");
      cell.colSpan = 3;
      row.append(cell);
      table.append(row);
    }
    const entries = card.querySelector(".entries");
    entries.replaceChildren(table);
    if (selected.length > 6)
      entries.append(
        make("small", `${selected.length} selected · first 6 shown`),
      );
  });
  const pairs = [
    [0, 1],
    [0, 2],
    [1, 2],
  ];
  if (!$("links").children.length)
    $("links").replaceChildren(
      ...pairs.map(([a, b]) => {
        const box = make("div", undefined, "link"),
          label = make("div"),
          text = make("span"),
          stats = make("small"),
          button = make("button");
        label.append(text, stats);
        button.type = "button";
        button.onclick = () => {
          const link = state.links.find((l) => l.a === a && l.b === b);
          act(`link/${a}/${b}/${link.open ? 0 : 1}`);
        };
        box.append(label, button);
        return box;
      }),
    );
  [...$("links").children].forEach((box, i) => {
    const [a, b] = pairs[i],
      directions = s.links.filter(
        (l) => (l.a === a && l.b === b) || (l.a === b && l.b === a),
      ),
      link = directions[0];
    box.classList.toggle("cut", !link.connected);
    box.querySelector("span").textContent =
      `${names[a]} ↔ ${names[b]} · ${link.connected ? "open" : "unavailable"}`;
    box.querySelector("small").textContent =
      `${directions.reduce((n, l) => n + l.datagrams, 0)} received datagrams · ${directions.reduce((n, l) => n + l.bytes, 0)} B`;
    const button = box.querySelector("button");
    button.textContent = link.open ? "Cut link" : "Open link";
    button.disabled = pending > 0;
  });
  for (const id of ["write", "delete", "load", "reset", "heal"])
    $(id).disabled = pending > 0;
  if (changed)
    $("ranges").replaceChildren(
      ...Array.from({ length: 32 }, (_, i) => {
        const bins = s.nodes.map((n) => n.bins[i]);
        const row = make(
          "tr",
          undefined,
          bins.some(
            (b) =>
              b.count !== bins[0].count ||
              b.fingerprint !== bins[0].fingerprint,
          )
            ? "different"
            : "",
        );
        row.append(make("td", `[${i * 32}, ${(i + 1) * 32})`));
        bins.forEach((b) => {
          const cell = make("td");
          cell.append(signature(b));
          row.append(cell);
        });
        return row;
      }),
    );
  previous = s;
}
const session = new Session(request, render);
function feedback(text, error = false) {
  $("feedback").textContent = text;
  $("feedback").classList.toggle("error", error);
}
async function act(path) {
  pending++;
  feedback("Applying…");
  if (state) render(state);
  try {
    const s = await session.run(path, "POST");
    feedback(s.last_action);
    actions.unshift(s.last_action);
    actions.length = Math.min(actions.length, 8);
    $("activity").replaceChildren(...actions.map((a) => make("li", a)));
    $("error").textContent = "";
  } catch (error) {
    feedback(String(error), true);
  } finally {
    pending--;
    if (state) render(state);
  }
}
function write() {
  try {
    const { start, end, peer } = selection();
    if (new TextEncoder().encode($("value").value).length > 256)
      throw Error("Value: at most 256 UTF-8 bytes");
    act(`write/${peer}/${start}/${end}/${hex($("value").value)}`);
  } catch (e) {
    feedback(e.message, true);
  }
}
$("operations").onsubmit = (e) => {
  e.preventDefault();
  write();
};
$("delete").onclick = () => {
  try {
    const { start, end, peer } = selection();
    act(`delete/${peer}/${start}/${end}`);
  } catch (e) {
    feedback(e.message, true);
  }
};
$("load").onclick = () =>
  act(`write/${Number($("peer").value)}/0/128/${hex("shared")}`);
$("heal").onclick = () => act("heal");
$("reset").onclick = () => {
  $("stream").checked = false;
  actions.length = 0;
  act("reset");
};
for (const id of ["start", "end", "peer"])
  $(id).onchange = () => {
    if (state) render(state);
  };
setInterval(() => {
  if (!$("stream").checked || session.pending) return;
  try {
    const { start, end, peer } = selection();
    const key = start + (writes % (end - start));
    writes++;
    const value = `${$("value").value}:${writes}`;
    if (new TextEncoder().encode(value).length > 256)
      throw Error("Value: at most 256 UTF-8 bytes including live-write suffix");
    act(`write/${peer}/${key}/${key + 1}/${hex(value)}`);
  } catch (e) {
    $("stream").checked = false;
    feedback(e.message, true);
  }
}, 1000);
async function poll() {
  try {
    await session.poll();
    if (!previous) feedback("Starting…");
    else if ($("feedback").textContent === "Starting…") feedback("Ready");
    $("error").textContent = "";
  } catch (e) {
    $("error").textContent = String(e);
  }
  setTimeout(poll, 250);
}
poll();

document.addEventListener("keydown", (event) => {
  if (
    event.repeat ||
    event.ctrlKey ||
    event.metaKey ||
    event.altKey ||
    event.target.closest('input,select,textarea,[contenteditable="true"]')
  )
    return;
  const key = event.key.toLowerCase();
  if (["a", "b", "c"].includes(key)) {
    $("peer").value = names.indexOf(key.toUpperCase());
    if (state) render(state);
  } else if (key === "w" && !pending) write();
  else if (key === "d" && !pending) $("delete").click();
  else if (
    key === " " &&
    !pending &&
    state &&
    !event.target.closest("button,a")
  ) {
    const id = Number($("peer").value);
    act(`connect/${id}/${state.nodes[id].online ? 0 : 1}`);
  } else return;
  event.preventDefault();
});
