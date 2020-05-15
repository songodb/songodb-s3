const { ObjectS3 } = require('./lib/object')

module.exports = exports = createObjectS3

function createObjectS3(s3, bucket) {
  return new ObjectS3(s3, bucket)
}