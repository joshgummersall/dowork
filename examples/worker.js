const {Worker} = require('dowork');
const fs = require('fs');
const request = require('request');

exports.Searcher = class Searcher extends Worker {
  static topics() {
    return [{
      topic: 'search'
    }];
  }

  handleMessage(message, callback) {
    const url = message.json().url;
    if (!url) {
      return callback();
    }

    this.pipeline.log('Fetching url', url);
    request.get(url, (err, resp, body) => {
      this.pipeline.publish('response', {
        url: url,
        response: body
      }, callback);
    });
  }
}

exports.Storer = class Storer extends Worker {
  static topics() {
    return [{
      topic: 'response'
    }];
  }

  handleMessage(message, callback) {
    const {url, response} = message.json();
    this.pipeline.log('Storing response for', url);
    fs.writeFile(url, response, callback);
  }
}
