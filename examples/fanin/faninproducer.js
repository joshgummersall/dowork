import Task from './task'
import async from 'async';
import {Worker} from '../../src/index';

export default class FanInProducer extends Worker {
  taskConfig(message) {
    return message.json();
  }

  onMessage(message) {
    const task = new Task(this.taskConfig(message));
    async.waterfall([
      callback => task.isValid(callback),
      (isValid, callback) => {
        if (isValid) {
          this.handleMessage(message, task, callback);
        } else {
          callback(new Error(`Task ${task.tKey} is invalid`));
        }
      }
    ], err => {
      if (err) {
        this.pipeline.log('error', 'worker error', err);
        message.requeue(this.config.requeueDelay, this.config.backoff || false);
      } else {
        message.finish();
      }
    });
  }

  finishMessage(message, task, topic, dataToAdd, done) {
    if (topic && dataToAdd) {
      task.addMessage(message, dataToAdd, (err) => {
        if (err) {
          done(err);
        } else {
          this.pipeline.publish(topic, message, done);
        }
      });
    } else {
      done();
    }
  }
}

