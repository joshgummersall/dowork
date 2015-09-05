Worker = require '../lib/worker'
should = require 'should'
sinon = require 'sinon'

describe 'Worker', ->
  beforeEach ->
    @sandbox = sinon.sandbox.create()

    @worker = new Worker 'topic', 'channel', {}, {}

  afterEach ->
    @sandbox.restore()

  describe '.handleMessage', ->
    it 'finishes a message if no error occurs', (done) ->
      message =
        finish: ->
          done()
        requeue: ->
          done new Error 'Unexpected call to .requeue'

      @worker.handleMessage message

    it 'requeues a message if an error occurs', (done) ->
      workerMock = @sandbox.mock @worker
      workerMock.expects('performJob')
        .once()
        .yields new Error 'Worker error...'

      message =
        finish: ->
          done new Error 'Unexpected call to .finish'
        requeue: ->
          workerMock.verify()
          done()

      @worker.handleMessage message
