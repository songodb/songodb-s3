module.exports = exports = {
  getJSON,
  putJSON,
  deleteJSON,
  listJSON
}

async function getJSON(s3, bucket, key) {
  let params = {
    Bucket: bucket,
    Key: key,
    ResponseContentType: 'application/json'
  }
  let data = null
  try {
    data = await s3.getObject(params).promise() 
    data.Body = JSON.parse(data.Body.toString('utf-8'))
  } catch (err) {
    if (err.code != "NoSuchKey") throw err
  }
  return data
}

async function putJSON(s3, bucket, key, json) {
  let params = {
    Bucket: bucket,
    Key: key,
    Body: JSON.stringify(json),
    ContentType: 'application/json'
  }
  return await s3.putObject(params).promise()
}

async function deleteJSON(s3, bucket, keys) {
  if (typeof keys === 'string') {
    keys = [ keys ]
  }
  let params = {
    Bucket: bucket,
    Delete: {
      Objects: keys.map(Key => { return { Key } })
    }
  }
  let data = await s3.deleteObjects(params).promise()
  // Return data.Deleted in order of keys given
  let lookup = data.Deleted.reduce((h, item) => { 
    h[item.Key] = item
    return h 
  }, { })
  data.Deleted = keys.map(key => lookup[key])
  return data
}

async function listJSON(s3, bucket, prefix, options) {
  options = options || { }
  let params = {
    Bucket: bucket,
    Prefix: prefix,
    MaxKeys: options.MaxKeys || 100,
    ContinuationToken: options.ContinuationToken,
    Delimiter: options.Delimiter
  }
  return await s3.listObjectsV2(params).promise()
}
