winston = require 'winston'

module.exports = class Worker
  @topics: []

  constructor: (@topic, @channel, @pipeline, @reader, @config = {}) ->
    @config.backoff or= 10

  performJob: (message, done) ->
    done()

  handleMessage: (message) ->
    @performJob message, (err) =>
      if err
        winston.log 'error', 'performJob failed!', err
        return message.requeue @config.backoff, false

      message.finish()
