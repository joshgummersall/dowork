_ = require 'underscore'
async = require 'async'
{Reader} = require 'nsqjs'

# Contains app-specific logic, including set of workers to start up
# and a schema for topics.
module.exports = class App
  constructor: (@topics = {}, @workers = [], @config = {}) ->
    process.on 'SIGINT', ->
      process.exit()

  publish: (topic, message, callback) ->
    callback()

  start: (callback) ->
    return callback new Error "No workers!" unless @workers.length

    async.eachSeries @workers, (Worker, callback) =>
      async.eachSeries Worker.topics, ({topic, channel, config}, callback) =>
        channel or= 'default'

        worker = new Worker topic, channel
        reader = new Reader topic, channel, _.extend {}, @config, config

        reader.on 'message', (message) ->
          worker.handleMessage topic, channel, message

        reader.connect()
        callback()

      , callback

    , callback
