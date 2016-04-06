import Task from './task'
import Worker from '../worker'
import async from 'async';

export default class FanInConsumer extends Worker {
  taskConfig(message) {
    return message.json();
  }

  onMessage(message) {
    const task = new Task(this.taskConfig(message));
    async.auto({
      isValid: callback => {
        task.isValid(callback);
      },
      isComplete: callback => {
        task.isComplete(callback);
      },
      handle: ['isValid', 'isComplete', (callback, {isValid, isComplete}) => {
        if (isValid && !isComplete) {
          this.handleValidIncomplete(message, task, callback);
        } else if (!isValid && isComplete) {
          this.handleInvalidComplete(message, task, callback);
        } else if (!isValid && !isComplete) {
          this.handleInvalidIncomplete(message, task, callback);
        } else {
          task.fetchMessages((err, messages) => {
            if (err) {
              callback(err);
            } else {
              this.handleMessage(message, task, messages, callback);
            }
          });
        }
      }]
    }, err => {
      if (err) {
        this.pipeline.log('error', 'worker error', err);
        message.requeue(this.config.requeueDelay, this.config.backoff || false);
      } else {
        message.finish();
      }
    });
  }

  handleValidIncomplete(message, task, done) {
    const toPublish = message.json();
    toPublish.attempts++;

    if (toPublish.attempts > task.maxPollCount) {
      done(new Error(`Task ${task.tKey} max poll count reached`));
    } else {
      setTimeout(() => {
        this.pipeline.publish(this.topic, toPublish, callback);
      }, task.computeDelayMs());
    }
  }

  handleInvalidComplete(message, task, done) {
    done();
  }

  handleInvalidIncomplete(message, task, done) {
    done();
  }

  handleMessage(message, task, messages, done) {
    done();
  }
}
