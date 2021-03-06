require('dotenv').config()
const { getJSON, putJSON, deleteJSON, listJSON } = require('../lib/s3')

const AWS = require('aws-sdk')
const s3 = new AWS.S3()
const BUCKET = process.env.BUCKET

describe('putJSON', () => {
  let key = 'putJSON.json'
  afterAll(async () => {
    await deleteJSON(s3, BUCKET, key)
  })
  it ('should write an object to S3', async () => {
    let obj = { hello: "world" }
    let result = await putJSON(s3, BUCKET, key, obj)
    expect(result).toMatchObject({
      ETag: expect.anything()
    })
    let { Body } = await getJSON(s3, BUCKET, key)
    expect(Body).toEqual(obj)
  })
})

describe('getJSON', () => {
  let key = 'getJSON.json'
  let obj = { hello: "world" }
  beforeAll(async () => {
    await putJSON(s3, BUCKET, key, obj)
  })
  afterAll(async () => {
    await deleteJSON(s3, BUCKET, key)
  })
  it ('should get an object from S3', async () => {
    let result = await getJSON(s3, BUCKET, key)
    expect(result).toEqual({
      "AcceptRanges": "bytes",
      "Body": obj,
      "ContentLength": 17,
      "ContentType": "application/json",
      "ETag": expect.anything(),
      "LastModified": expect.anything(),
      "Metadata": {}
    })
  })
  it ('should return null if key does not exist', async () => {
    let result = await getJSON(s3, BUCKET, 'badkey.json')
    expect(result).toBe(null)
  })
})

describe('deleteJSON', () => {
  let key = 'deleteJSON.json'
  beforeEach(async () => {
    await putJSON(s3, BUCKET, key, { })
  })
  afterAll(async () => {
    await deleteJSON(s3, BUCKET, key)
  })
  it ('should delete an object to S3', async () => {
    let result = await deleteJSON(s3, BUCKET, key)
    expect(result).toEqual({
      "Deleted": [ { "Key": "deleteJSON.json" } ],
      "Errors": []
    })
  })
  it ('should not throw if key does not exist', async () => {
    let result = await deleteJSON(s3, BUCKET, 'badkey.json')
    expect(result).toEqual({
      "Deleted": [ { "Key": "badkey.json" } ],
      "Errors": []
    })
  })
})

describe('listJSON', () => {
  let keys = [ 'list/folder/obj1.json', 'list/folder/obj2.json', 'list/folder/obj3.json' ]
  let objects = [ { }, { }, { } ]
  beforeEach(async () => { 
    await Promise.all(keys.map((key, i) => { return putJSON(s3, BUCKET, key, objects[i]) }))
  })
  afterEach(async () => {
    await deleteJSON(s3, BUCKET, keys)
  })
  it ('should list all objects with default 100 MaxKeys', async () => {
    let result = await listJSON(s3, BUCKET, 'list/folder/')
    expect(result).toMatchObject({
      IsTruncated: false,
      KeyCount: 3,
      MaxKeys: 100
    })
    expect(result.ContinuationToken).toBe(undefined)
    expect(result.Contents.map(data => data.Key).sort()).toEqual(keys)
  })
  it ('should return all objects using continuation token', async () => {
    let result = await listJSON(s3, BUCKET, 'list/folder/', { MaxKeys: 2 })
    expect(result).toMatchObject({
      IsTruncated: true,
      KeyCount: 2,
      MaxKeys: 2,
      NextContinuationToken: expect.anything()
    })
    let token = result.NextContinuationToken
    result = await listJSON(s3, BUCKET, 'list/folder/', { MaxKeys: 2, ContinuationToken: token })
    expect(result).toMatchObject({
      IsTruncated: false,
      KeyCount: 1,
      MaxKeys: 2
    })
    // For some reason ContinuationToken is supplied even when isTruncated is false
    // expect(result.ContinuationToken).toBe(undefined)
  })
  it ('should return only one level deep using delimiter', async () => {
    await putJSON(s3, BUCKET,'list/folder2/obj1.json' , { })
    let result = await listJSON(s3, BUCKET, 'list/', { Delimiter: '/' })
    await deleteJSON(s3, BUCKET, 'list/folder2/obj1.json')
    expect(result).toMatchObject({
      IsTruncated: false,
      Contents: [],
      Prefix: 'list/',
      Delimiter: '/',
      MaxKeys: 100,
      CommonPrefixes: [ { Prefix: 'list/folder/' }, { Prefix: 'list/folder2/' } ],
      KeyCount: 2
    })
  })
  it ('should return empty content if prefix does not exist', async() => {
    let result = await listJSON(s3, BUCKET, 'doesnotexist/')
    expect(result).toEqual({
      "IsTruncated": false,
      "Contents": [],
      "Name": "songodb-s3.songo.io",
      "Prefix": "doesnotexist/",
      "MaxKeys": 100,
      "CommonPrefixes": [],
      "KeyCount": 0
    })
  })
})
