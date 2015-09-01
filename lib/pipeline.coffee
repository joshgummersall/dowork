_ = require 'underscore'
async = require 'async'
joi = require 'joi'
request = require 'request'
{Reader} = require 'nsqjs'

# Contains pipeline-specific logic, including set of workers to start up
# and a schema for topics.
module.exports = class Pipeline
  constructor: (@topics = {}, @workers = [], @config = {}) ->
    process.on 'SIGINT', ->
      process.exit()

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
        request.post "http://#{@config.nsqdHTTPAddress}/put?topic=#{topic}",
          json: message
          callback

    ], callback

  start: (callback) ->
    return callback new Error "No workers!" unless @workers.length

    async.eachSeries @workers, (Worker, callback) =>
      async.eachSeries Worker.topics, ({topic, channel, config}, callback) =>
        channel or= 'default'

        worker = new Worker topic, channel
        reader = new Reader topic, channel, _.extend {}, @config, config

        # Include validation prior to message processing.
        reader.on 'message', (message) =>
          @validate topic, message, (err) ->
            if err
              console.error err
              return message.finish()

            worker.handleMessage topic, channel, message

        reader.connect()
        callback()

      , callback

    , callback
