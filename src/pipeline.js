const _ = require('underscore');
const async = require('async');
const joi = require('joi');
const request = require('request');
const winston = require('winston');
const {Writer, Reader} = require('nsqjs');

/*
Contains pipeline-specific logic, including set of workers to start up
and a schema for topics.
*/
module.exports = class Pipeline {
  constructor(topics = {}, workers = [], config = {}) {
    this.topics = topics;
    this.workers = workers;

    // Fill in default config options
    this.config = _.defaults(config, {
      winston: {
        level: 'debug',
        console: true,
        loggers: []
      },
      reader: {
        nsqdTCPAddresses: '127.0.0.1:4150',
        backoff: 10
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

    // Create a writer instance to use for publishing
    const {host, port} = this.config.writer.nsqd;
    this.writer = new Writer(host, port, this.config.writer);
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
    joi.validate(json, schema, {allowUnknown: false}, callback);
  }

  publish(topic, message, callback) {
    // Wrapped in once for safety, closes writer and forwards args to callback
    const done = _.once(...args => {
      this.writer.close();
      callback(...args);
    });

    this.writer.once(Writer.READY, () => {
      async.series([
        callback => {
          this.validate(topic, message, callback);
        },
        callback => {
          this.writer.publish(topic, message, callback);
        }
      ], done);
    });

    this.writer.once(Writer.ERROR, done);
    this.writer.connect();
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
        reader = new Reader(topic, channel, config);
        worker = new Worker(topic, channel, this, reader, config);

        const messageHandler = (handlerFn) => {
          (message) => {
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
          (...args) => {
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
