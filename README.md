dowork
======

An [NSQJS](https://github.com/dudleycarr/nsqjs) framework for data processing
in Node.js.

### Introduction

[NSQ](http://nsq.io/) is a great tool for creating scalable distributed
data processing systems, however the barrier to entry is relatively high.
[NSQJS](https://github.com/dudleycarr/nsqjs) is a useful library but does not
offer much in terms of structuring a collection of workers. Enter dowork!

### Overview

dowork provides two basic units, the pipeline and the worker. A worker is
a single component of a pipeline that listens on a set of topics and channels,
reponding to messages published to those topics. A pipeline is a collection
of related workers designed to accomplish some task.

### Quick Example

The following example defines a pipeline that would be used to fetch a large
number of resources (either XML or JSON), parse them into Javascript objects,
and then write those objects to files.

```coffeescript
request = require 'request'
{
  Pipeline
  Worker
} = require 'dowork'

class ResourceFetcher extends Worker
  @topics: [
    topic: 'resource'
    channel: 'fetch'
  ]

  performJob: (message, done) ->
    {url} = message.json()

    request.get url, (err, resp, body) =>
      return done err if err

      switch resp.headers['content-type']
        when 'application/xml'
          nextTopic = 'xml_resource'
        when 'application/json'
          nextTopic = 'json_resource'
        else
          return done()

      @pipeline.publish nextTopic, {body}, done

class JsonParser extends Worker
  @topics: [
    topics: 'json_resource'
    channel: 'parse'
  ]

  performJob: (message, done) ->
    {body} = message.json()
    parsed = JSON.parse body
    @pipeline.publish 'parsed_resource', {parsed}, done

class XmlParser extends Worker
  @topics: [
    topics: 'xml_resource'
    channel: 'parse'
  ]

  performJob: (message, done) ->
    {body} = message.json()
    xml2js.parseString body, (err, parsedXml) =>
      return done err if err

      @pipeline.publish 'parsed_resource', {parsed}, done

class FileWriter extends Worker
  @topics: [
    topic: 'parsed_resource'
    channel: 'write'
  ]

  performJob: (message, done) ->
    {parsed} = message.json()
    fs.writeFile 'SOME_RANDOM_FILE.json', JSON.stringify(parsed, null, 2),
      done

schemas =
  resource:
    url: joi.string().required()

  json_resource: joi.object().keys
    body: joi.string().required()

  xml_resource: joi.object().keys
    body: joi.string().required()

  parsed_resource: joi.object().keys
    parsed: joi.object()

pipeline = new Pipeline schemas, [
  ResourceFetcher
  XmlParser
  JsonParser
  FileWriter
]

pipeline.start (err) ->
  throw err if err

  console.log 'Pipeline has started...'

async.eachSeries [
  'http://somejsonresource.com/entity_id'
  'http://anotherjsonresource.com/entity_id'
  'http://somexmlresource.com/entity_id'
], (url, callback) ->
  pipeline.publish 'resource', {url}, callback
, (err) ->
  throw err if err
```
