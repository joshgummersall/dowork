import _ from 'underscore';
import async from 'async';
import joi from 'joi';
import winston from 'winston';
import {Writer, Reader} from 'nsqjs';

/*
Contains pipeline-specific logic, including set of workers to start up
and a schema for topics.
*/
export default class Pipeline {
  constructor(topics = {}, workers = [], config = {}) {
    this.topics = topics;
    this.workers = workers;

    // Fill in default config options
    this.config = _.defaults(config, {
      joi: {
        allowUnknown: true,
        stripUnknown: true
      },
      winston: {
        level: 'debug',
        console: true,
        loggers: []
      },
      reader: {
        nsqdTCPAddresses: '127.0.0.1:4150',
        backoff: false,
        requeueDelay: 60
      },
      writer: {
        nsqd: {
          host: '127.0.0.1',
          port: 4150
        }
      }
    });

    // Set up Winston properly, including log level, extra loggers, and console
    // logging (specified via `winston` config options).
    winston.level = this.config.winston.level;
    this.config.winston.loggers.map(l => {
      winston.add(l.logger, l.options);
    });
    if (!this.config.winston.console) {
      winston.remove(winston.transports.Console);
    }
  }

  log(level, ...args){
    winston.log(level, ...args);
  }

  validate(topic, message, callback) {
    const schema = this.topics[topic];
    if (!schema) {
      return callback(new Error(`No schema for #{topic}`));
    }

    const json = typeof message.json === 'function' ? message.json() : message;
    joi.validate(json, schema, this.config.joi, callback);
  }

  createWriter() {
    const {host, port} = this.config.writer.nsqd;
    return new Writer(host, port, this.config.writer);
  }

  publish(topic, message, callback) {
    const writer = this.createWriter();

    // Wrapped in once for safety, closes writer and forwards args to callback
    const done = _.once((err, ...args) => {
      if (err) {
        this.log('error', 'publish error', err);
      }
      writer.close();
      callback(err, ...args);
    });

    writer.once(Writer.READY, () => {
      async.waterfall([
        callback => {
          this.validate(topic, message, callback);
        },
        (validated, callback) => {
          writer.publish(topic, validated, callback);
        }
      ], done);
    });
    writer.once(Writer.ERROR, done);
    writer.connect();
  }

  start(callback) {
    if (!this.workers.length) {
      return callback(new Error('No workers'));
    }

    async.eachSeries(this.workers, (Worker, callback) => {
      async.eachSeries(Worker.topics(), (options, callback) => {
        const topic = options.topic;
        const channel = options.channel || 'default';

        // Allows workers to override global config with topic-specific values.
        const config = _.extend({}, this.config.reader, options.config);

        // Construct reader and worker for message processing.
        const reader = new Reader(topic, channel, config);
        const worker = new Worker(topic, channel, this, reader, config);

        const messageHandler = (handlerFn) => {
          return (message) => {
            this.validate(topic, message, (err) => {
              // Do not requeue invalid messages. Log the error and move on.
              if (err) {
                this.log('error', err);
                return message.finish();
              }

              worker[handlerFn](message);
            });
          }
        };

        const simpleHandler = (handlerFn) => {
          return (...args) => {
            worker[handlerFn](...args);
          }
        };

        // Register all pertinent reader events, delegating to worker.
        reader.on(Reader.MESSAGE, messageHandler('onMessage'));
        reader.on(Reader.DISCARD, messageHandler('onDiscard'));
        reader.on(Reader.ERROR, simpleHandler('onError'));
        reader.on(Reader.NSQD_CONNECTED, simpleHandler('onConnected'));
        reader.on(Reader.NSQD_CLOSED, simpleHandler('onClosed'));

        reader.connect();
        callback();

      }, callback);

    }, callback);
  }
}
