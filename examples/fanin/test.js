import FanInConsumer from './faninconsumer'
import FanInProducer from './faninproducer'
import FanOutProducer from './fanoutproducer'
import joi from 'joi';
import {Pipeline} from '../../src/index'

const TaskMessage = joi.object().keys({
  tKey: joi.string().required(),
  eCount: joi.number(),
  pollDelay: joi.number()
});

const Topics = {
  fan_out: joi.object(),
  fanned_out: TaskMessage.keys({
    n: joi.number().required()
  }),
  fanned_in: TaskMessage
};

const FanOutPipeline = new Pipeline(Topics, [
  FanInConsumer,
  FanInProducer,
  FanOutProducer
]);

const command = process.argv[2];

if (command === 'start') {
  FanOutPipeline.start(err => {
    if (err) {
      throw(err);
    } else {
      console.log('Fan out pipeline started...');
    }
  });
} else if (command === 'publish') {
  FanOutPipeline.publish('fan_out', {foo: 'bar'}, console.log);
}
