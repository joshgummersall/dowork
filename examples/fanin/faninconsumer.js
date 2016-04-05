import Task from './task'
import async from 'async';
import {Worker} from '../../src/index';

export default class FanInConsumer extends Worker {
  static topics() {
    return [{
      topic: 'fanned_in'
    }];
  }

  handleMessage(message, done) {
    const task = new Task(message.json());
    async.waterfall([
      callback => task.isValid(callback),
      (isValid, callback) => {
        if (!isValid) {
          // TODO(JOSH): fix to include handling timeouts
          this.pipeline.log('debug', 'task invalid');
          callback(null, false);
        } else {
          task.isComplete(callback);
        }
      },
      (isComplete, callback) => {
        if (!isComplete) {
          // TODO(JOSH): fix to poll
          this.pipeline.log('debug', 'task incomplete');
          callback(null, []);
          // Handle incomplete state
        } else {
          task.fetchMessages(callback);
        }
      },
      (messages, callback) => {
        this.pipeline.log('debug', 'Fanned in messages', messages);
        callback();
      }
    ], done);
  }
}
