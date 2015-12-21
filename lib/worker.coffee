###
A fairly simple Worker base class to extend from. Extend and override the
`handle*` methods to provide some utility. Each worker has references to the
topic and channel it is registered on as well as the pipeline it is a member of,
the reader that is delegating it messages, and something configuration.

@author josh.gummersall@moz.com (Josh Gummersall)
###
module.exports = class Worker
  @topics: []

  constructor: (@topic, @channel, @pipeline, @reader, @config = {}) ->

  handleMessage: (message, callback) ->
    callback()

  onMessage: (message) ->
    @handleMessage message, (args...) ->

  handleDiscard: (message, callback) ->
    callback()

  onDiscard: (message) ->
    @handleDiscard message, (args...) ->

  handleError: (err, callback) ->
    callback()

  onError: (err) ->
    @handleError err, (args...) ->

  handleConnected: (host, port, callback) ->
    callback()

  onConnected: (host, port) ->
    @handleConnected host, port, ->

  handleClosed: (host, port, callback) ->
    callback()

  onClosed: (host, port) ->
    @handleClosed host, port, ->
