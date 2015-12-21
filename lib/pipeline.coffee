_ = require 'underscore'
async = require 'async'
joi = require 'joi'
request = require 'request'
winston = require 'winston'
{Writer, Reader} = require 'nsqjs'

###
Contains pipeline-specific logic, including set of workers to start up
and a schema for topics.

@author josh.gummersall@moz.com (Josh Gummersall)
###
module.exports = class Pipeline
  constructor: (@topics = {}, @workers = [], @config = {}) ->
    # Fill in default config options
    @config = _.defaults @config,
      winston:
        level: 'debug'
        console: true
        loggers: []
      reader:
        nsqdTCPAddresses: '127.0.0.1:4150'
        backoff: 10
      writer:
        nsqd:
          host: '127.0.0.1'
          port: 4150

    # Set up Winston properly, including log level, extra loggers, and console
    # logging (specified via `winston` config options).
    winston.level = @config.winston.level
    winston.add logger, options for {logger, options} in @config.winston.loggers
    winston.remove winston.transports.Console unless @config.winston.console

    # Create a writer instance to use for publishing
    {host, port} = @config.writer.nsqd
    @writer = new Writer host, port, @config.writer

  log: (level, args...) ->
    winston.log level, args...

  validate: (topic, message, callback) ->
    schema = @topics[topic]
    return callback new Error "No schema for #{topic}" unless schema

    json = message.json?() or message
    joi.validate json, schema,
      allowUnknown: false
      callback

  publish: (topic, message, callback) ->
    # Wrapped in once for safety, closes writer and forwards args to callback
    done = _.once (args...) =>
      @writer.close()
      callback args...

    @writer.once Writer.READY, =>
      async.series [
        (callback) =>
          @validate topic, message, callback

        (callback) =>
          @writer.publish topic, message, callback

      ], done

    @writer.once Writer.ERROR, done
    @writer.connect()

  start: (callback) ->
    return callback new Error "No workers" unless @workers.length

    async.eachSeries @workers, (Worker, callback) =>
      async.eachSeries Worker.topics, ({topic, channel, config}, callback) =>
        channel or= 'default'

        # Allows workers to override global config with topic-specific values.
        config = _.extend {}, @config.reader, config

        # Construct reader and worker for message processing.
        reader = new Reader topic, channel, config
        worker = new Worker topic, channel, this, reader, config

        messageHandler = (handlerFn) =>
          (message) =>
            @validate topic, message, (err) =>
              # Do not requeue invalid messages. Log the error and move on.
              if err
                @log 'error', err
                return message.finish()

              worker[handlerFn] message

        simpleHandler = (handlerFn) ->
          (args...) ->
            worker[handlerFn] args...

        # Register all pertinent reader events, delegating to worker.
        reader.on Reader.MESSAGE, messageHandler 'onMessage'
        reader.on Reader.DISCARD, messageHandler 'onDiscard'
        reader.on Reader.ERROR, simpleHandler 'onError'
        reader.on Reader.NSQD_CONNECTED, simpleHandler 'onConnected'
        reader.on Reader.NSQD_CLOSED, simpleHandler 'onClosed'

        reader.connect()
        callback()

      , callback

    , callback
