const {Pipeline} = require('dowork');
const {Searcher, Storer} = require('./worker');
const joi = require('joi');

// Quick schema describing the topics we support
const topics = {
  search: joi.object().keys({
    url: joi.string().required()
  }),
  response: joi.object().keys({
    url: joi.string().required(),
    response: joi.string().required()
  })
};

const pipeline = new Pipeline(topics, [Searcher, Storer]);
pipeline.start((err) => {
  if (err) {
    throw(err);
  }

  pipeline.log('Processing has begun...');
});
