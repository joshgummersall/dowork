import joi from 'joi';
import {FanInProducer, FanInConsumer, FanOutProducer} from '../../src/fanning';
import {Pipeline} from '../src/index';

class First extends FanOutProducer {
  static topics() {
    return [{
      topic: 'fan_out'
    }];
  }

  taskConfig(message) {
    return {
      eCount: 10
    };
  }

  handleMessage(message, task, done) {
    async.timesSeries(task.eCount, (n, callback) => {
      this.pipeline.log('debug', 'Publishing fan out message', n);
      this.pipeline.publish('fanned_out', {
        tKey: task.tKey,
        eCount: task.eCount,
        n
      }, callback);
    }, done);
  }
}

class Second extends FanInProducer {
  static topics() {
    return [{
      topic: 'fanned_out'
    }];
  }

  handleMessage(message, task, done) {
    this.finishMessage(message, task, 'fanned_in', {
      n: message.json().n
    }, done);
  }
}

export default class Third extends FanInConsumer {
  static topics() {
    return [{
      topic: 'fanned_in'
    }];
  }

  handleMessage(message, task, messages, done) {
    this.pipeline.log('debug', 'Fanned in messages', messages);
    callback();
  }
}

const taskMessage = joi.object().keys({
  tKey: joi.string().required(),
  eCount: joi.number(),
  pollDelay: joi.number()
});

const topics = {
  fan_out: joi.object(),
  fanned_out: taskMessage.keys({
    n: joi.number().required()
  }),
  fanned_in: taskMessage.keys({
    attempts: joi.number().default(0)
  })
};

const pipeline = new Pipeline(topics, [First, Second, Third]);

const command = process.argv[2];
if (command === 'start') {
  pipeline.start(err => {
    if (err) {
      throw(err);
    } else {
      console.log('Fan out pipeline started...');
    }
  });
} else if (command === 'publish') {
  pipeline.publish('fan_out', {foo: 'bar'}, console.log);
}
