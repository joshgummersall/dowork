module.exports = class Worker
  @topics: []

  performJob: (topic, channel, message, done) ->
    done()

  handleMessage: (topic, channel, message) ->
    @performJob topic, channel, message, (err) ->
      if err
        console.error err
        return message.requeue 10, false

      message.finish()
