Worker = require '../lib/worker'
should = require 'should'
sinon = require 'sinon'

describe 'Worker', ->
  beforeEach ->
    @sandbox = sinon.sandbox.create()
    @worker = new Worker 'topic', 'channel', {}, {}, {}

  afterEach ->
    @sandbox.restore()
