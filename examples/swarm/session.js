// Serialize commands with reads so an older snapshot cannot overwrite an action response.
export class Session {
  constructor(request, onState) {
    this.request = request;
    this.onState = onState;
    this.tail = Promise.resolve();
    this.pending = 0;
  }
  run(path, method = "GET") {
    this.pending++;
    const result = this.tail.then(async () => {
      const state = await this.request(path, method);
      this.onState(state);
      return state;
    });
    this.tail = result.catch(() => {}).finally(() => this.pending--);
    return result;
  }
  poll() {
    return this.pending ? Promise.resolve() : this.run("state");
  }
}
