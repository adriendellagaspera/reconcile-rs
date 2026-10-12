const http = require("node:http");
const fs = require("node:fs");
const path = require("node:path");
const assert = require("node:assert/strict");
const { chromium } = require(process.env.PLAYWRIGHT_MODULE || "playwright");

(async () => {
  const root = path.resolve("target/swarm-pages");
  const server = http.createServer((request, response) => {
    if (request.url === "/reconcile-rs/probe")
      return response.end("<!doctype html><title>Runtime probe</title>");
    const relative =
      decodeURIComponent(
        new URL(request.url, "http://localhost").pathname,
      ).replace(/^\/reconcile-rs\//, "") || "index.html";
    const file = path.resolve(root, relative);
    if (
      !file.startsWith(root + path.sep) ||
      !fs.existsSync(file) ||
      !fs.statSync(file).isFile()
    ) {
      response.statusCode = 404;
      return response.end();
    }
    response.setHeader(
      "Content-Type",
      file.endsWith(".wasm")
        ? "application/wasm"
        : file.endsWith(".js")
          ? "text/javascript"
          : "text/html",
    );
    fs.createReadStream(file).pipe(response);
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  const base = `http://127.0.0.1:${server.address().port}/reconcile-rs/`;
  let browser;
  try {
    browser = await chromium.launch({ headless: true, args: ["--no-sandbox"] });
    const page = await browser.newPage();
    const errors = [];
    const apiRequests = [];
    page.on("pageerror", (error) => errors.push(error.message));
    page.on("request", (request) => {
      if (/\/state$|\/order\//.test(request.url()))
        apiRequests.push(request.url());
    });
    await page.goto(base + "probe");
    const proof = await page.evaluate(async () => {
      const bindings = await import("./pkg/reconcile_swarm_web.js");
      await bindings.default();
      return bindings.verify_runtime();
    });
    console.log(proof);
    await page.goto(base);
    await page.waitForFunction(
      () => document.querySelectorAll(".replica").length === 3,
    );
    const read = () =>
      page.evaluate(async () =>
        (
          await import(
            "./api.js" +
              new URL(document.querySelector('script[type="module"]').src)
                .search
          )
        ).request("state"),
      );
    const waitEqual = () =>
      page.waitForFunction(
        () =>
          document.getElementById("agreement").textContent ===
          "Dated entries agree",
        null,
        { timeout: 30000 },
      );
    async function action(id) {
      await page.locator("#" + id).click();
      await page.waitForFunction(
        () =>
          !document.getElementById("write").disabled &&
          document.getElementById("feedback").textContent !== "Applying…",
      );
    }
    async function write(peer, start, end, value) {
      await page.locator("#peer").selectOption(String(peer));
      await page.locator("#start").fill(String(start));
      await page.locator("#end").fill(String(end));
      await page.locator("#value").fill(value);
      await action("write");
      assert.ok(
        (await page.locator("#feedback").textContent()).includes("wrote"),
      );
    }
    await write(0, 0, 128, "shared");
    await waitEqual();
    assert.equal((await read()).nodes[2].live, 128);
    // Every replica can disconnect and originate local writes.
    for (let id = 0; id < 3; id++) {
      await page
        .locator(`[data-replica="${["A", "B", "C"][id]}"] .replica-head button`)
        .click();
      await page.waitForFunction(
        (id) =>
          !document.getElementById("write").disabled &&
          document.querySelectorAll(".replica-head button")[id].textContent ===
            "Reconnect",
        id,
      );
      await write(id, 16, 24, "local" + id);
      const s = await read();
      assert.equal(s.nodes[id].online, false);
      assert.ok(
        s.nodes[id].entries
          .filter((e) => e.key >= 16 && e.key < 24)
          .every((e) => e.value === "local" + id),
      );
      const other = (id + 1) % 3;
      await write(other, 16, 24, "remote" + id);
      await page.waitForTimeout(900);
      assert.ok(
        (await read()).nodes[id].entries
          .filter((e) => e.key >= 16 && e.key < 24)
          .every((e) => e.value === "local" + id),
      );
      await action("heal");
      await waitEqual();
    }
    await page.locator('[data-replica="C"] .replica-head button').click();
    await page.waitForFunction(
      () =>
        !document.getElementById("write").disabled &&
        document.querySelector('[data-replica="C"] .replica-head button')
          .textContent === "Reconnect",
    );
    await page.locator("#peer").selectOption("0");
    await page.locator("#start").fill("32");
    await page.locator("#end").fill("64");
    await action("delete");
    const deleted = await read();
    assert.equal(deleted.nodes[0].live, 96);
    assert.equal(deleted.nodes[0].tombstones, 32);
    assert.equal(deleted.nodes[2].live, 128);
    assert.ok(
      (await page.locator("#feedback").textContent()).includes("deleted 32"),
    );
    await page.screenshot({
      path: "/tmp/workshop-desktop.png",
      fullPage: true,
    });
    // Controls retain their DOM identity across polling; pointer gestures cannot be lost to redraw.
    await page
      .locator("#links button")
      .first()
      .evaluate((button) => {
        button.dataset.identity = "stable";
      });
    await page.waitForTimeout(700);
    assert.equal(
      await page.locator("#links button").first().getAttribute("data-identity"),
      "stable",
    );
    await page.locator("#links button").first().click();
    await page.waitForFunction(
      () =>
        !document.getElementById("write").disabled &&
        document.querySelector("#links button").textContent === "Open link",
    );
    await action("heal");
    await waitEqual();
    assert.equal((await read()).nodes[2].tombstones, 32);
    await write(1, 40, 41, "é / reinsert");
    await waitEqual();
    assert.equal(
      (await read()).nodes[0].entries.find((e) => e.key === 40).value,
      "é / reinsert",
    );
    await page.locator("#stream").check();
    await page.waitForTimeout(2200);
    await page.locator("#stream").uncheck();
    assert.ok((await read()).revision > deleted.revision + 3);
    // Invalid input produces persistent feedback instead of being cleared by the next poll.
    await page.locator("#start").fill("80");
    await page.locator("#end").fill("40");
    await action("delete");
    assert.ok(
      (await page.locator("#feedback").textContent()).includes("Range:"),
    );
    await page.waitForTimeout(750);
    assert.ok(
      (await page.locator("#feedback").textContent()).includes("Range:"),
    );
    await page.locator("#start").fill("0");
    await page.locator("#end").fill("32");
    const other = await browser.newPage();
    await other.goto(base);
    await other.waitForFunction(
      () => document.querySelectorAll(".replica").length === 3,
    );
    const empty = await other.evaluate(async () =>
      (
        await import(
          "./api.js" +
            new URL(document.querySelector('script[type="module"]').src).search
        )
      ).request("state"),
    );
    assert.equal(empty.nodes[0].live, 0);
    await other.close();
    await page.setViewportSize({ width: 390, height: 844 });
    assert.equal(
      await page.evaluate(
        () => document.documentElement.scrollWidth > innerWidth,
      ),
      false,
    );
    await page.screenshot({ path: "/tmp/workshop-mobile.png", fullPage: true });
    await action("reset");
    assert.ok((await read()).nodes.every((n) => n.entries.length === 0));
    await page.locator("h1").click();
    await page.keyboard.press("b");
    assert.equal(await page.locator("#peer").inputValue(), "1");
    await page.keyboard.press("Space");
    await page.waitForFunction(
      () =>
        document.querySelector('[data-replica="B"] .replica-head button')
          .textContent === "Reconnect" &&
        !document.getElementById("write").disabled,
    );
    await page.keyboard.press("w");
    await page.waitForFunction(
      () =>
        document.getElementById("feedback").textContent.includes("B: wrote") &&
        !document.getElementById("write").disabled,
    );
    assert.equal((await read()).nodes[1].live, 32);
    await page.keyboard.press("d");
    await page.waitForFunction(
      () =>
        document
          .getElementById("feedback")
          .textContent.includes("B: deleted") &&
        !document.getElementById("write").disabled,
    );
    assert.equal((await read()).nodes[1].tombstones, 32);
    await action("heal");
    await waitEqual();
    await write(0, 0, 1024, "dense");
    await waitEqual();
    assert.ok((await read()).nodes.every((node) => node.live === 1024));
    await assert.rejects(
      page.evaluate(async () =>
        (
          await import(
            "./api.js" +
              new URL(document.querySelector('script[type="module"]').src)
                .search
          )
        ).request("invalid", "POST"),
      ),
      /invalid control/,
    );
    assert.equal(await page.locator("#error").textContent(), "");
    assert.deepEqual(errors, []);
    assert.deepEqual(apiRequests, []);
    await page.setViewportSize({ width: 1440, height: 1000 });
    await page.locator('a[href^="architecture.html"]').click();
    assert.equal(await page.locator("section").count(), 6);
    assert.equal(await page.locator("svg").count(), 5);
    assert.equal(
      await page.evaluate(
        () => document.documentElement.scrollWidth > innerWidth,
      ),
      false,
    );
    await page.screenshot({
      path: "/tmp/workshop-architecture.png",
      fullPage: true,
    });
    await page.setViewportSize({ width: 390, height: 844 });
    assert.equal(
      await page.evaluate(
        () => document.documentElement.scrollWidth > innerWidth,
      ),
      false,
    );
    await page.screenshot({
      path: "/tmp/workshop-architecture-mobile.png",
      fullPage: true,
    });
    console.log(
      "Playground: free writes, every offline replica, conflicting ranges, tombstones/reinsert, stable controls, persistent feedback, stream, reset, independent visitors and architecture pages passed",
    );
  } finally {
    if (browser) await browser.close();
    server.close();
  }
})().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
