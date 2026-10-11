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
      decodeURIComponent(request.url).replace(/^\/reconcile-rs\//, "") ||
      "index.html";
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
      page.evaluate(async () => (await import("./api.js")).request("state"));
    const advance = async () => {
      await page.waitForFunction(
        () => !document.getElementById("next").disabled,
      );
      await page.locator("#next").click();
    };
    assert.equal((await read()).stage, 0);
    await page.screenshot({ path: "/tmp/runtime-empty.png", fullPage: true });
    await advance();
    await page.waitForFunction(
      () =>
        document.getElementById("agreement").textContent ===
        "Dated entries agree",
    );
    const shared = await read();
    assert.equal(shared.stage, 1);
    assert.equal(shared.equal, true);
    assert.equal(shared.nodes[0].entries.length, 3);
    assert.deepEqual(shared.nodes[0].entries, shared.nodes[2].entries);
    await page.screenshot({ path: "/tmp/runtime-shared.png", fullPage: true });
    await advance();
    await page.waitForFunction(() => !document.getElementById("next").disabled);
    const divergent = await read();
    assert.equal(divergent.stage, 2);
    assert.equal(divergent.equal, false);
    assert.equal(divergent.isolated, true);
    assert.equal(await page.locator("#ranges .different").count(), 1);
    const value = (id) =>
      page
        .locator(`[data-replica="${id}"] [data-key="m/status"] .value`)
        .textContent();
    assert.equal(await value("A"), "alpha");
    assert.equal(await value("B"), "alpha");
    assert.equal(await value("C"), "charlie");
    await page.waitForTimeout(1700);
    assert.equal(
      await value("C"),
      "charlie",
      "isolated C must retain its own write",
    );
    await page.locator("#presenter summary").click();
    assert.ok(
      (await page.locator("#speaker").textContent()).includes("sans voir"),
    );
    await page.locator("#presenter summary").click();
    await page.screenshot({
      path: "/tmp/runtime-partition.png",
      fullPage: true,
    });
    const other = await browser.newPage();
    await other.goto(base);
    await other.waitForFunction(
      () => document.querySelectorAll(".replica").length === 3,
    );
    const independent = await other.evaluate(async () =>
      (await import("./api.js")).request("state"),
    );
    assert.equal(independent.stage, 0);
    assert.equal(independent.nodes[0].entries.length, 0);
    await other.close();
    await page.setViewportSize({ width: 390, height: 844 });
    assert.equal(
      await page.evaluate(
        () => document.documentElement.scrollWidth > innerWidth,
      ),
      false,
    );
    const cards = await page.locator(".replica").all();
    for (const card of cards)
      assert.ok((await card.boundingBox()).width <= 390);
    assert.ok((await page.locator("#ranges").boundingBox()).width <= 390);
    await page.screenshot({ path: "/tmp/runtime-mobile.png", fullPage: true });
    await advance();
    await page.waitForFunction(
      () =>
        document.getElementById("next").textContent ===
        "Converged · reset to repeat",
    );
    const healed = await read();
    assert.equal(healed.stage, 3);
    assert.equal(healed.isolated, false);
    assert.equal(healed.equal, true);
    assert.deepEqual(healed.nodes[0].entries, healed.nodes[2].entries);
    assert.deepEqual(healed.nodes[0].root, healed.nodes[2].root);
    assert.equal(await page.locator("#ranges .different").count(), 0);
    const contenders = [
      divergent.nodes[0].entries[1],
      divergent.nodes[2].entries[1],
    ];
    const compare = (a, b) =>
      a.stamp.physical - b.stamp.physical ||
      a.stamp.logical - b.stamp.logical ||
      a.stamp.node - b.stamp.node;
    const winner = contenders.sort(compare).at(-1);
    assert.deepEqual(healed.nodes[0].entries[1], winner);
    assert.ok(healed.links.every((link) => link.connected));
    assert.ok(
      healed.links.some((link) => link.datagrams > 0 && link.bytes > 0),
    );
    await page.setViewportSize({ width: 1280, height: 900 });
    await page.screenshot({
      path: "/tmp/runtime-converged.png",
      fullPage: true,
    });
    await page.locator("#reset").click();
    await page.waitForFunction(
      () => document.getElementById("next").textContent === "Write on A",
    );
    const reset = await read();
    assert.equal(reset.stage, 0);
    assert.ok(reset.nodes.every((node) => node.entries.length === 0));
    await assert.rejects(
      page.evaluate(async () =>
        (await import("./api.js")).request("invalid", "POST"),
      ),
      /Invalid lab request/,
    );
    assert.deepEqual(errors, []);
    assert.deepEqual(apiRequests, []);
    assert.equal(await page.locator("#error").textContent(), "");
    console.log(
      "Runtime lab: local writes, isolation, exact LWW convergence, RSOS ranges, independent visitors, reset and mobile layout passed",
    );
  } finally {
    if (browser) await browser.close();
    server.close();
  }
})().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
