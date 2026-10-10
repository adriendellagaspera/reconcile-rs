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
    const mission = await page.evaluate(async () => {
      const { Fleet } = await import("./pkg/reconcile_swarm_web.js");
      const fleet = new Fleet(0, 6, 1200, 250);
      try {
        fleet.command("peer/1/offline");
        fleet.command("peer/6/offline");
        for (let i = 0; i < 65; i++) fleet.step();
        let state = JSON.parse(fleet.state());
        const local = state.nodes[0].find(([k]) => k === "poi/01/00")?.[1].Poi;
        if (!local) throw Error("Camera did not discover nearby cue");
        if (state.nodes[6].some(([, v]) => v.Poi))
          throw Error("Center obtained isolated findings");
        if (state.flood.aircraft[1].role !== "search")
          throw Error("Isolated verifier read another replica");
        fleet.command("heal");
        let verified = false,
          confirmed = false;
        const deadline = Date.now() + 45000;
        while (Date.now() < deadline) {
          fleet.step();
          state = JSON.parse(fleet.state());
          verified ||= state.flood.aircraft[1].role === "verify";
          confirmed ||= state.nodes[1].some(
            ([, v]) =>
              v.Poi?.id === 1 &&
              v.Poi.source === 1 &&
              v.Poi.finding === "confirmed",
          );
          if (
            verified &&
            confirmed &&
            state.nodes[6].some(
              ([, v]) => v.Poi?.id === 1 && v.Poi.finding === "confirmed",
            )
          )
            return { verified, confirmed };
          await new Promise((r) => setTimeout(r, 50));
        }
        throw Error(
          "Received finding did not cause verification and reach center",
        );
      } finally {
        fleet.free();
      }
    });
    assert.equal(mission.verified, true);
    assert.equal(mission.confirmed, true);
    console.log(
      "Flood: isolated knowledge, real report delivery, bounded local rerouting, verification and center reception passed",
    );
    await page.goto(base);
    await page.waitForFunction(
      () => document.getElementById("fleet").children.length === 7,
    );
    await page.waitForFunction(
      () => document.querySelector("#reports .poi"),
      null,
      { timeout: 30000 },
    );
    await page.locator("#play").click();
    await page.waitForFunction(
      () => document.getElementById("play").textContent === "Play",
    );
    await page.locator("#reports .poi").first().click();
    assert.ok((await page.locator("#object-detail").textContent()).length > 0);
    const first = await page.evaluate(async () =>
      (await import("./api.js")).request("state"),
    );
    assert.equal(first.center, 6);
    assert.equal(first.flood.cruise_kmh, 72);
    assert.equal(first.flood.camera_radius_km, 0.1);
    await page.screenshot({
      path: process.env.SWARM_SCREENSHOT_PREFIX
        ? process.env.SWARM_SCREENSHOT_PREFIX + "-desktop.png"
        : "/tmp/swarm-desktop.png",
    });
    const other = await browser.newPage();
    await other.goto(base);
    await other.waitForFunction(
      () => document.getElementById("fleet").children.length === 7,
    );
    const independent = await other.evaluate(async () =>
      (await import("./api.js")).request("state"),
    );
    assert.equal(independent.playing, true);
    await other.close();
    await page.locator("#fleet button").last().click();
    assert.equal(
      await page.locator("#perspective").textContent(),
      "RESPONSE CENTER · LOCAL KNOWLEDGE",
    );
    assert.equal(
      await page.locator("#reports .poi").count(),
      0,
      "offline center must not display simulator POIs",
    );
    await page.locator("details summary").click();
    await page.locator("#truth").click();
    assert.equal(await page.locator("#reports .poi").count(), 4);
    await page.locator("#fleet button").first().click();
    await page.setViewportSize({ width: 390, height: 844 });
    assert.equal(
      await page.evaluate(
        () => document.documentElement.scrollWidth > innerWidth,
      ),
      false,
    );
    assert.ok((await page.locator("#map").boundingBox()).height > 844 * 0.8);
    assert.equal(
      await page.locator("#inspector-content").evaluate((e) => e.inert),
      true,
    );
    await page.screenshot({ path: "/tmp/swarm-mobile.png" });
    await page.locator("#inspector-toggle").click();
    assert.equal(
      await page.locator("#inspector-content").evaluate((e) => e.inert),
      false,
    );
    await page.screenshot({ path: "/tmp/swarm-mobile-details.png" });
    await page.locator("#inspector-toggle").click();
    await page.locator("#map-overview").click();
    assert.equal(
      await page.locator("#map").evaluate((e) => getComputedStyle(e).objectFit),
      "contain",
    );
    await page.locator("#map-overview").click();
    await page.locator("#map-center").click();
    assert.deepEqual(errors, []);
    assert.deepEqual(apiRequests, []);
    assert.equal(await page.locator("#error").textContent(), "");
    console.log(
      "Flood UI: local/truth separation, independent visitors, finding inspector and full-screen mobile map passed",
    );
  } finally {
    if (browser) await browser.close();
    server.close();
  }
})().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
