import { apiBase } from './config.js';

export function endpoint(path, base = apiBase, page = globalThis.location.href) {
  const origin = new URL(page);
  const target = base ? new URL(base) : new URL('.', origin);
  if (!['http:', 'https:'].includes(target.protocol) || target.username || target.password || target.search || target.hash) {
    throw Error('The simulation API must be an HTTP(S) URL without credentials or query parameters.');
  }
  if (origin.protocol === 'https:' && target.protocol !== 'https:') {
    throw Error('The simulation API must use HTTPS.');
  }
  if (!base && origin.hostname.endsWith('.github.io')) {
    throw Error('Simulation server not configured. Set the SWARM_API_URL repository variable and redeploy Pages.');
  }
  target.pathname = target.pathname.replace(/\/$/, '') + '/' + path;
  return target.href;
}
