const { getJSON, putJSON, deleteJSON, listJSON } = require('./s3')

class ObjectS3 {

  constructor(s3, bucket, options) {
    this.s3 = s3
    this.bucket = bucket
    this.options = options || { }
  }

  async getOne(key, options) {
    options = Object.assign({ }, this.options, options || { })
    return await getJSON(this.s3, this.bucket, key, options)
  }

  async getMultiple(keys, options) {
    return await Promise.all(keys.map(key => this.getOne(key, options)))
  }

  async getPrefix(prefix, options) {
    options = Object.assign({ }, this.options, options || { })
    let list = await this.list(prefix, options)
    let data = await this.getMultiple(list.Contents.map(data => data.Key), options)
    list.Contents.forEach((item, i) => {
      Object.assign(item, data[i])
    })
    return list
  }

  async putOne(key, object, options) {
    options = Object.assign({ }, this.options, options || { })
    return await putJSON(this.s3, this.bucket, key, object, options)
  }

  async putMultiple(keys, objects, options) {
    options = Object.assign({ }, this.options, options || { })
    let promises = await keys.map((key, i) => this.putOne(key, objects[i], options))
    return await Promise.all(promises)
  }

  async deleteOne(key, options) {
    options = Object.assign({ }, this.options, options || { })
    let data = (await deleteJSON(this.s3, this.bucket, key, options))
    return {
      "Deleted": data.Deleted[0],
      "Errors": data.Errors
    }
  }

  async deleteMultiple(keys, options) {
    options = Object.assign({ }, this.options, options || { })
    return await deleteJSON(this.s3, this.bucket, keys, options)
  }

  async deletePrefix(prefix, options) {
    let list = await this.list(prefix, options)
    let data = await this.deleteMultiple(list.Contents.map(data => data.Key), options)
    list.Contents.forEach((item, i) => {
      item.Deleted = data.Deleted[i].Key
    })
    list.Deleted = data.Deleted
    list.Errors = data.Errors
    return list
  }

  async list(prefix, options) {
    options = options || { }
    return await listJSON(this.s3, this.bucket, prefix, options)
  }
}

module.exports = exports = { 
  ObjectS3
}