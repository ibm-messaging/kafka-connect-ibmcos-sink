# kafka-connect-ibmcos-sink

Kafka Connect Sink Connector for [IBM Cloud Object
Storage](https://console.bluemix.net/catalog/services/cloud-object-storage).

Using this connector, you can:
  - Copy data from a Kafka topic into object storage.
  - Flexibly batch together multiple Kafka records into a single object for
    efficient retrieval from object storage.
  - Preserve ordering of Kafka data. Kafka ordering of records within a
    partition is maintained as the data is written into object storage.
  - Transfer data _exactly-once_ (subject to some restrictions, see below).


## Building

```shell
$ gradle shadowJar
```


## Configuration

- `os.api.key` _(required)_ - API key used to connect to the Object Storage
          service instance.

- `os.bucket.location`_(required)_ - Location of the Object Storage service
          bucket, for example: `eu-gb`.

- `os.bucket.name` _(required)_ - Name of the Object Storage service bucket to
          write data into.

- `os.bucket.resiliency` _(required)_ - Resiliency of the Object Storage bucket.
          Must be one of: `cross-region`, `regional`, or `single-site`.

- `os.endpoint.visibility` _(optional)_ - Specify `public` to connect to the
          Object Storage service over the public internet, or `private` to
          connect from a connector running inside the IBM Cloud network, for
          example from an IBM Cloud Kubernetes Service cluster. The default is
          `public`.

- `os.object.deadline.seconds` _(optional)_ - The number of seconds (as measured
          wall clock time for the Connect Task instance) between reading the
          first record from Kafka, and writing all of the records read so far
          into an object storage object. This can be useful in situations where
          there are long pauses between Kafka records being produced to a topic,
          as it ensures that any records received by this connector will always
          be written into object storage within the specified period of time.

- `os.object.interval.seconds` _(optional)_ - :construction: not implemented
          yet :construction: The number of seconds (as measured
          by the timestamps in Kafka records) between reading the first record
          from Kafka, and writing all of the records read so far into an object
          storage object.

- `os.object.records` _(optional)_ - The maximum number of Kafka records to
          combine into a object.

- `os.service.crn`_(required)_ - CRN for the Object Storage service instance.

Note that while the configuration properties `os.object.deadline.seconds`,
`os.interval.seconds`, and `os.object.records` are all listed as optional`-
*at least one* of these properties *must* be set to a non-default value.


## Combining multiple Kafka records into an object

:construction: partially implemented :construction:

Typically Kafka records are much smaller than the maximum size an object storage
object. And while it is possible to create an object for each Kafka record this
is usually not an efficient way to use object storage. This connector offers
three different controls for deciding how much Kafka data gets combined into a
object:

- `os.object.records`
- `os.object.interval.seconds`
- `os.object.deadline.seconds`

At least one of these configuration properties must be specified. If more than
one property is specified then an object is written at the point the first of
these limits is reached.

For example, given the following configuration:
```
os.object.records=100
os.object.deadline.seconds=60
```
Assuming that at least one Kafka record is available to be written into an
object then objects will be created either: every minute, or after 100 Kafka
records have been received, whichever condition occurs first.

## Exactly once delivery

:construction: not implemented yet :construction:

This connector can be used to provide exactly once delivery of Kafka records
into object storage. When used like this, data is copied without loss or
duplication.

Exactly once delivery requires that Kafka records are combined into objects in a
completely deterministic way. Which is to say that given a stream of Kafka
records to process, the connector will always group the same records into each
object storage object.

Using either the `os.object.records` property or the
`os.object.interval.seconds` property (or both together) will result in
deterministic processing of Kafka records into objects. However the
`os.object.deadline.seconds` option cannot be used for exactly once delivery as
the grouping of Kafka records into objects is dependent on the speed at which
the system hosting the connector can process records.




