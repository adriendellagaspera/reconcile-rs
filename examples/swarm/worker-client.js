export class WorkerClient {
  constructor(worker = new Worker(new URL('./worker.js', import.meta.url), { type: 'module' }), timeoutMs = 30000) {
    this.worker = worker;
    this.timeoutMs = timeoutMs;
    this.pending = new Map();
    this.nextId = 0;
    this.failure = null;
    worker.onmessage = ({ data }) => {
      if (data.fatal) return this.fail(Error(data.fatal));
      const pending = this.pending.get(data.id);
      if (!pending) return;
      this.pending.delete(data.id);
      clearTimeout(pending.timer);
      data.error ? pending.reject(Error(data.error)) : pending.resolve(data.state);
    };
    worker.onerror = event => this.fail(Error(event.message || 'Simulation worker failed'));
    worker.onmessageerror = () => this.fail(Error('Invalid simulation worker message'));
  }

  fail(error) {
    this.failure = error;
    this.worker.terminate();
    for (const pending of this.pending.values()) {
      clearTimeout(pending.timer);
      pending.reject(error);
    }
    this.pending.clear();
  }

  request(path, method = 'GET') {
    if (this.failure) return Promise.reject(this.failure);
    if (this.pending.size >= 16) return Promise.reject(Error('Simulation is busy'));
    return new Promise((resolve, reject) => {
      const id = ++this.nextId;
      const timer = setTimeout(() => this.fail(Error('Simulation worker timed out')), this.timeoutMs);
      this.pending.set(id, { resolve, reject, timer });
      try { this.worker.postMessage({ id, path, method }); }
      catch (error) { this.fail(error); }
    });
  }
}
