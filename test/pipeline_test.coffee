Pipeline = require '../lib/pipeline'
Worker = require '../lib/worker'
should = require 'should'
sinon = require 'sinon'

describe 'Pipeline', ->
  beforeEach ->
    @sandbox = sinon.sandbox.create()

  afterEach ->
    @sandbox.restore()

  describe '.validate', ->
    it 'validates a well formed message'
    it 'does not validate a poorly formed message'

  describe '.publish', ->
    it 'publishes a well formatted message'
    it 'yields an error for an invalid message'

  describe '.start', ->
    it 'throws if there are no workers'
    it 'starts up workers and connects'
