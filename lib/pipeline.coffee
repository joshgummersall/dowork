_ = require 'underscore'
async = require 'async'
joi = require 'joi'
request = require 'request'
winston = require 'winston'
{Reader} = require 'nsqjs'

# Contains pipeline-specific logic, including set of workers to start up
# and a schema for topics.
module.exports = class Pipeline
  constructor: (@topics = {}, @workers = [], @config = {}) ->
    @config = _.defaults @config,
      backoff: 10

  validate: (topic, message, callback) ->
    schema = @topics[topic]
    return callback new Error "No schema for #{topic}!" unless schema

    json = if _.isFunction message.json then message.json() else message
    joi.validate json, schema,
      allowUnknown: true
      stripUnknown: true
      callback

  publish: (topic, message, callback) ->
    async.series [
      (callback) =>
        @validate topic, message, callback

      (callback) =>
        # TODO(Josh): use Writer to publish instead of HTTP request.
        request.post "http://#{@config.nsqdHTTPAddress}/put?topic=#{topic}",
          json: message
          callback

    ], callback

  start: (callback) ->
    return callback new Error "No workers!" unless @workers.length

    async.eachSeries @workers, (Worker, callback) =>
      async.eachSeries Worker.topics, ({topic, channel, config}, callback) =>
        channel or= 'default'

        # Allows workers to override global config with topic-specific values.
        config = _.extend {}, @config, config
        reader = new Reader topic, channel, config
        worker = new Worker topic, channel, this, reader, config

        # Include validation prior to message processing.
        reader.on Reader.MESSAGE, (message) =>
          @validate topic, message, (err) ->
            # Do not requeue invalid messages. Log the error and move on.
            if err
              winston.log 'error', err
              return message.finish()

            # Note: worker is responsible for finishing or requeuing message.
            worker.handleMessage message

        reader.connect()
        callback()

      , callback

    , callback
