import init, { Fleet } from './pkg/reconcile_swarm_web.js';

const ready = init().then(() => {
  const fleet = new Fleet(35, 12, 1200, 10);
  setInterval(() => fleet.step(), 500);
  return fleet;
});
ready.catch(error => postMessage({ fatal: String(error) }));

self.onmessage = async ({ data: { id, path, method } }) => {
  try {
    const fleet = await ready;
    if (method !== 'POST' && !(method === 'GET' && path === 'state')) throw Error('Invalid simulation request');
    const state = JSON.parse(method === 'POST' ? fleet.command(path) : fleet.state());
    postMessage({ id, state });
  } catch (error) {
    postMessage({ id, error: String(error) });
  }
};
