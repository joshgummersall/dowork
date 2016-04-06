import Task from './task'
import async from 'async';
import {Worker} from '../../src/index';

export default class FanOutProducer extends Worker {
  taskConfig(message) {
    return {};
  }

  onMessage(message) {
    const tast = new Task(this.taskConfig(message));
    async.series([
      callback => task.initialize(callback),
      callback => this.handleMessage(message, task, callback)
    ], err => {
      if (err) {
        this.pipeline.log('error', 'worker error', err);
        message.requeue(this.config.requeueDelay, this.config.backoff || false);
      } else {
        message.finish();
      }
    });
  }
}