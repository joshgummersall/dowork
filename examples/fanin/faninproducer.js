import Task from './task'
import async from 'async';
import {Worker} from '../../src/index';

export default class FanInProducer extends Worker {
  static topics() {
    return [{
      topic: 'fanned_out'
    }];
  }

  handleMessage(message, done) {
    this.pipeline.log('debug', 'Starting message', message.json());
    const {n} = message.json();
    const task = new Task(message.json());
    task.isValid((err, isValid) => {
      this.pipeline.log('debug', 'task.isValid', err, isValid);

      if (err) {
        done(err);
      } else if (!isValid) {
        done(new Error(`Task ${task.tKey} expired`));
      } else {
        this.finishMessage(task, message, {n}, done);
      }
    });
  }

  finishMessage(task, message, dataToAdd, done) {
    this.pipeline.log('debug', 'Adding task message',
        message.json(), dataToAdd);
    task.addMessage(message, dataToAdd, (err) => {
      if (err) {
        done(err);
      } else {
        this.pipeline.publish('fanned_in', message, done);
      }
    });
  }
}
