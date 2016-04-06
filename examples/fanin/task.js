import async from 'async';
import crypto from 'crypto';
import redis from 'redis';
import uuid from 'uuid';

class Task {
  constructor(options = {}) {
    this.tKey = options.tKey || uuid.v1();
    this.eCount = options.eCount;
    this.pollDelay = options.pollDelay || 60;
    this.maxPollCount = options.maxPollCount || 20;
    this.redis = redis.createClient();
  }

  computeDelayMs() {
    return this.pollDelay * 1000;
  }

  validKey() {
    return `${this.tKey}:v`;
  }

  initialize(callback) {
    this.redis.setex(this.validKey(), 60 * 30, 1, callback);
  }

  isValid(callback) {
    this.redis.get(this.validKey(), (err, value) => {
      callback(err, parseInt(value, 10) === 1);
    });
  }

  messagesKey() {
    return `${this.tKey}:m`;
  }

  messageId(message) {
    return crypto.createHash('sha1')
      .update(JSON.stringify(message.json()))
      .digest('hex');
  }

  addMessage(message, dataToAdd, callback) {
    const mKey = this.messageId(message);
    const args = [];
    for (const key in dataToAdd) {
      args.push(key);
      args.push(dataToAdd[key]);
    }
    async.series([
      callback => this.redis.hmset(mKey, ...args, callback),
      callback => this.redis.sadd(this.messagesKey(), mKey, callback)
    ], callback);
  }

  fetchMessages(callback) {
    async.waterfall([
      callback => this.redis.smembers(this.messagesKey(), callback),
      (mKeys, callback) => {
        async.concatSeries(mKeys, (mKey, callback) => {
          this.redis.hgetall(mKey, callback);
        }, callback);
      }
    ], callback);
  }

  isComplete(callback) {
    this.redis.scard(this.messagesKey(), (err, count) => {
      callback(err, count == this.eCount);
    });
  }
}

export default Task;
