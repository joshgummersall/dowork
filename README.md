dowork
======

An [NSQJS](https://github.com/dudleycarr/nsqjs) based framework for data
processing pipelines using Node.js.

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

### Worker

A worker is a single componenet of a pipeline. In terms of NSQ primitives, a
single worker subscribes to a topic and channel (optionally, multiple topic and
channel pairs). Each worker then accepts NSQ messages, does some unit of work
related to those messages, and either halts the pipeline or publishes any number
of messages to continue processing.

To create a worker you need to inherit from the Worker base class, specifying a
set of topics to subscribe to and overriding the `handleMessage` function to
do some work when receiving messages. Each worker receives a reference to the
topic and channel it is subscribed to as well as a reference to the pipeline it
is a member of, the reader that is delegating it messages, and the configuration
used to construct it.

See the
[examples](https://github.com/joshgummersall/dowork/blob/master/examples)
directory for some sample code.

### Pipeline

A pipeline is comprised of three things. The first is a schema object that maps
a set of topic names to a [joi]() schema defining the shape of the messages that
will be published to that topic. The second is a list of worker classes that
make up the pipeline. The third is a configuration object that allows to you
specify some NSQJS-style configuration around readers and writers. The default
configuration should suffice for deployment locally, but you will want to tinker
with this if deploying to a production environment.

See the
[examples](https://github.com/joshgummersall/dowork/blob/master/examples)
directory for some sample code.
