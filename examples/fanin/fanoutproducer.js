import Task from './task'
import async from 'async';
import {Worker} from '../../src/index';

export default class FanOutProducer extends Worker {
  static topics() {
    return [{
      topic: 'fan_out'
    }];
  }

  handleMessage(message, done) {
    const task = new Task({eCount: 10});

    task.initialize(err => {
      if (err) {
        return done(err);
      }

      async.timesSeries(task.eCount, (n, callback) => {
        this.pipeline.log('debug', 'Publishing fan out message', n);
        this.pipeline.publish('fanned_out', {
          tKey: task.tKey,
          eCount: task.eCount,
          n
        }, callback);
      }, done);
    });
  }
}
