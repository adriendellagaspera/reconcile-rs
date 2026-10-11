import init, { Lab } from './pkg/reconcile_swarm_web.js';
const ready = init().then(() => new Lab());
ready.catch(error => postMessage({ fatal: String(error) }));
self.onmessage = async ({ data: { id, path, method } }) => {
  try {
    const lab = await ready;
    if (!(method === 'GET' && path === 'state') && !(method === 'POST' && ['next', 'reset'].includes(path))) throw Error('Invalid lab request');
    const state = JSON.parse(method === 'POST' ? lab.command(path) : lab.state());
    postMessage({ id, state });
  } catch (error) { postMessage({ id, error: String(error) }); }
};
