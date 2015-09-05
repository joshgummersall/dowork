winston = require 'winston'

module.exports = class Worker
  @topics: []

  # Workers are passed the topic and channel they are registered on as well as
  # a handle to the reader that will delegate it messages. This can be used to
  # pause and unpause the delegation of messages if necessary. Workers are also
  # passed the config that was used to instantiate the reader.
  constructor: (@topic, @channel, @reader, @config = {}) ->
    @config.backoff or= 10

  performJob: (message, done) ->
    done()

  handleMessage: (message) ->
    @performJob message, (err) =>
      if err
        winston.log 'error', 'performJob failed!', err
        return message.requeue @config.backoff, false

      message.finish()
