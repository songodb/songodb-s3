require('dotenv').config()
const { ObjectS3 } = require('../lib/object')
const { getJSON, putJSON, deleteJSON, listJSON } = require('../lib/s3')

const AWS = require('aws-sdk')
const BUCKET = process.env.BUCKET
const s3 = new ObjectS3(new AWS.S3(), BUCKET)

describe('get', () => {
  let keys = [ 'getPrefix/sub1/obj1.json', 'getPrefix/sub1/obj2.json', 'getPrefix/sub2/obj3.json' ]
  let objects = [ { name: "obj1" }, { name: "obj2" }, { name: "obj3" } ]
  beforeAll(async () => {
    await s3.putMultiple(keys, objects)
  })
  afterAll(async () => {
    await s3.deleteMany(keys)
  })
  it ('should get all json objects given a prefix', async () => {
    let result = await s3.getPrefix('getPrefix/')
    expect(result).toMatchObject({
      IsTruncated: false,
      KeyCount: 3,
      MaxKeys: 100
    })
    expect(result.Contents).toMatchObject(keys.map((key, i) => { return { Key: key, Body: objects[i] } }))
  })
  it ('should get partial list of json objects given a prefix', async () => {
    let result = await s3.getPrefix('getPrefix/', { MaxKeys: 2 })
    expect(result).toMatchObject({
      IsTruncated: true,
      KeyCount: 2,
      MaxKeys: 2,
      NextContinuationToken: expect.anything()
    })
  })
  it ('should get the "subfolders" given a prefix and delimiter', async () => {
    let result = await s3.getPrefix('getPrefix/', { MaxKeys: 2, Delimiter: '/' })
    expect(result).toEqual({
      "IsTruncated": false,
      "Contents": [],
      "Name": "songodb-s3.songo.io",
      "Prefix": "getPrefix/",
      "Delimiter": "/",
      "MaxKeys": 2,
      "CommonPrefixes": [
        {
          "Prefix": "getPrefix/sub1/"
        },
        {
          "Prefix": "getPrefix/sub2/"
        }
      ],
      "KeyCount": 2
    })
  })
})

describe('put', () => {

  afterAll(async () => {
    await s3.deletePrefix("put/")
  })
  
  it ('should put an individual object', async () => {
    let key = "put/obj1.json"
    let object = { hello: "world" }
    let result = await s3.putOne(key, object)
    expect(result).toEqual({
      "ETag": expect.anything()
    })
  })

  it ('should put multiple objects', async () => {
    let keys = [ 'put/obj2.json', 'put/obj3.json', 'put/obj4.json' ]
    let objects = [ { name: "obj2" }, { name: "obj3" }, { name: "obj4" } ]
    let result = await s3.putMultiple(keys, objects)
    expect(result.length).toBe(3)
    expect(result).toEqual([
      { "ETag": expect.anything() }, { "ETag": expect.anything() }, { "ETag": expect.anything() }
    ])
  })
})

describe('delete', () => {
  let keys = [ 'delete/obj1.json', 'delete/obj2.json', 'delete/obj3.json' ]
  let objects = [ { name: "obj1" }, { name: "obj2" }, { name: "obj3" } ]

  it ('should delete an individual object', async () => {
    await s3.putOne(keys[0], objects[0])
    let result = await s3.deleteOne(keys[0])
    expect(result).toMatchObject({
      Deleted: { Key: "delete/obj1.json" },
      Errors: [ ]
    })
  })
  it ('should delete multiple keys', async () => {
    await s3.putMultiple(keys, objects)
    let result = await s3.deleteMultiple(keys)
    expect(result).toEqual({
      Deleted: keys.map(key => { return { Key: key } }),
      Errors: [ ]
    })
  })
  it ('should delete all keys under a prefix', async () => {
    await s3.putMultiple(keys, objects)
    let result = await s3.deletePrefix('delete/')
    expect(result).toMatchObject({
      Deleted: [ { Key: 'delete/obj1.json' }, { Key: 'delete/obj2.json' }, { Key: 'delete/obj3.json' } ]
    })
  })
  it ('should not throw delete prefix that does not exist', async () => {
    await s3.putMultiple(keys, objects)
    await s3.deletePrefix('delete/')
    let result = await s3.deletePrefix('delete/')
    expect(result).toEqual({
      "IsTruncated": false,
      "Contents": [],
      "Name": "songodb-s3.songo.io",
      "Prefix": "delete/",
      "MaxKeys": 100,
      "CommonPrefixes": [],
      "KeyCount": 0,
      "Deleted": [],
      "Errors": []
    })
  })
})

