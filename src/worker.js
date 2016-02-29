/*
A fairly simple Worker base class to extend from. Extend and override the
`handle*` methods to provide some utility. Each worker has references to the
topic and channel it is registered on as well as the pipeline it is a member of,
the reader that is delegating it messages, and something configuration.
*/
export default class Worker {
  // Should be overridden to return topic configuration
  static topics() {
    return [];
  }

  constructor(topic, channel, pipeline, reader, config = {}) {
    this.topic = topic;
    this.channel = channel;
    this.pipeline = pipeline;
    this.reader = reader;
    this.config = config;
  }

  handleMessage(message, callback) {
    callback();
  }

  onMessage(message) {
    this.handleMessage(message, (...args) => {});
  }

  handleDiscard(message, callback) {
    callback();
  }

  onDiscard(message) {
    this.handleDiscard(message, (...args) => {});
  }

  handleError(err, callback) {
    callback();
  }

  onError(err) {
    this.handleError(err, (...args) => {});
  }

  handleConnected(host, port, callback) {
    callback();
  }

  onConnected(host, port) {
    this.handleConnected(host, port, () => {});
  }

  handleClosed(host, port, callback) {
    callback();
  }

  onClosed(host, port) {
    this.handleClosed(host, port, () => {});
  }
}
