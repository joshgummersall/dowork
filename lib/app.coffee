_ = require 'underscore'
async = require 'async'
joi = require 'joi'
{Reader} = require 'nsqjs'

# Contains app-specific logic, including set of workers to start up
# and a schema for topics.
module.exports = class App
  constructor: (@topics = {}, @workers = [], @config = {}) ->
    process.on 'SIGINT', ->
      process.exit()

  validateMessage: (topic, messageJson, callback) ->
    schema = @topics[topic]
    return callback new Error "No schema for #{topic}!" unless schema

    joi.validate messageJson, schema,
      allowUnknown: true
      stripUnknown: true
      callback

  publishMessage: (topic, message, callback) ->
    async.series [
      (callback) =>
        @validateMessage topic, message, callback

      (callback) ->
        # TODO(Josh): Implement
        callback()

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
          @validateMessage topic, message.json(), (err) ->
            if err
              console.error err
              return message.finish()

            worker.handleMessage topic, channel, message

        reader.connect()
        callback()

      , callback

    , callback
