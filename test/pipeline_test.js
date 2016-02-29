import Pipeline from '../src/pipeline';
import joi from 'joi';
import should from 'should';

let pipeline;

describe('Pipeline', () => {
  describe('.validate', () => {
    beforeEach(() => {
      const topics = {
        topic: joi.object().keys({
          bar: joi.string().required()
        })
      };

      pipeline = new Pipeline(topics, [], {});
    });

    it('validates a well formed message', (done) => {
      pipeline.validate('topic', {bar: 'should validate'}, done);
    });

    it('does not validate a poorly formed message', (done) => {
      pipeline.validate('topic', {bar: 2}, (err) => {
        should.exist(err);
        done();
      });
    });

    it('does not validate a message with extra keys', (done) => {
      pipeline.validate('topic', {foo: 1, bar: 'str'}, (err) => {
        should.exist(err);
        done();
      });
    });
  });
});
