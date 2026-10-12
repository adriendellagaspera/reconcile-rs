import test from "node:test";
import assert from "node:assert/strict";
import { Session } from "./session.js";

test("a delayed poll is rendered before a queued write, never after its acknowledgement", async () => {
  let release;
  const order = [],
    renders = [];
  const session = new Session(
    async (path) => {
      order.push(path);
      if (path === "state")
        return new Promise(
          (resolve) => (release = () => resolve({ revision: 0 })),
        );
      return { revision: 1 };
    },
    (s) => renders.push(s.revision),
  );
  const poll = session.poll();
  await Promise.resolve();
  const write = session.run("write", "POST");
  assert.deepEqual(order, ["state"]);
  release();
  await poll;
  await write;
  await session.tail;
  assert.deepEqual(order, ["state", "write"]);
  assert.deepEqual(renders, [0, 1]);
  assert.equal(session.pending, 0);
});
test("polls do not accumulate behind commands and failures do not block later actions", async () => {
  let release;
  const seen = [];
  const session = new Session(
    async (path) => {
      seen.push(path);
      if (path === "hold") await new Promise((r) => (release = r));
      if (path === "bad") throw Error("invalid control");
      return path;
    },
    () => {},
  );
  const hold = session.run("hold", "POST");
  await Promise.resolve();
  await session.poll();
  assert.deepEqual(seen, ["hold"]);
  release();
  await hold;
  await assert.rejects(session.run("bad", "POST"), /invalid control/);
  await session.run("good", "POST");
  await session.tail;
  assert.deepEqual(seen, ["hold", "bad", "good"]);
  assert.equal(session.pending, 0);
});
