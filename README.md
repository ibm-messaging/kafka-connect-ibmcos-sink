# kafka-connect-ibmcos-sink

Kafka Connect Sink Connector for [IBM Cloud Object
Storage](https://console.bluemix.net/catalog/services/cloud-object-storage).

Using this connector, you can:
  - Copy data from a Kafka topic into IBM Cloud Object Storage.
  - Flexibly batch together multiple Kafka records into a single object for
    efficient retrieval from object storage.
  - Preserve ordering of Kafka data. Kafka ordering of records within a
    partition is maintained as the data is written into object storage.
  - Transfer data exactly once (subject to some restrictions, see [Exactly once delivery](#exactly-once-delivery)).

## Content
- [Building the connector](#building-the-connector)
- [Configuration](#configuration)
- [Combining multiple Kafka records into an object](#combining-multiple-kafka-records-into-an-object)
- [Exactly once delivery](#exactly-once-delivery)
- [Provisioning an IBM Cloud Object Storage Service instance](#provisioning-an-ibm-cloud-object-storage-service-instance)
- [Running the connector](#running-the-connector)

## Building the connector

To build the connector, you must have the following installed:

* [git](https://git-scm.com/)
* [Gradle 4.0 or later](https://gradle.org/)
* Java 8 or later

Clone the repository with the following command:

```shell
git clone https://github.com/ibm-messaging/kafka-connect-ibmcos-sink
```

Change directory into the `kafka-connect-ibmcos-sink` directory:

```shell
cd kafka-connect-ibmcos-sink
```

Build the connector using Gradle:

```shell
$ gradle shadowJar
```

## Configuration

- `cos.api.key` _(required)_ - API key used to connect to the Cloud Object Storage
          service instance.

- `cos.bucket.location`_(required)_ - Location of the Cloud Object Storage service
          bucket, for example: `eu-gb`.

- `cos.bucket.name` _(required)_ - Name of the Cloud Object Storage service bucket to
          write data into.

- `cos.bucket.resiliency` _(required)_ - Resiliency of the Cloud Object Storage bucket.
          Must be one of: `cross-region`, `regional`, or `single-site`.

- `cos.endpoint.visibility` _(optional)_ - Specify `public` to connect to the
          Cloud Object Storage service over the public internet, or `private` to
          connect from a connector running inside the IBM Cloud network, for
          example from an IBM Cloud Kubernetes Service cluster. The default is
          `public`.

- `cos.object.deadline.seconds` _(optional)_ - The number of seconds (as measured
          wall clock time for the Connect Task instance) between reading the
          first record from Kafka, and writing all of the records read so far
          into a Cloud Object Storage object. This can be useful in situations where
          there are long pauses between Kafka records being produced to a topic,
          as it ensures that any records received by this connector will always
          be written into object storage within the specified period of time.

- `cos.object.interval.seconds` _(optional)_ - The number of seconds (as measured
          by the timestamps in Kafka records) between reading the first record
          from Kafka, and writing all of the records read so far into a Cloud Object
          Storage object.

- `cos.object.records` _(optional)_ - The maximum number of Kafka records to
          combine into a object.

- `cos.service.crn`_(required)_ - CRN for the Cloud Object Storage service instance.

Note that while the configuration properties `cos.object.deadline.seconds`,
`cos.interval.seconds`, and `cos.object.records` are all listed as optional,
*at least one* of these properties *must* be set to a non-default value.


## Combining multiple Kafka records into an object

Typically Kafka records are much smaller than the maximum size an object storage
object. And while it is possible to create an object for each Kafka record this
is usually not an efficient way to use Cloud Object Storage. This connector offers
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
cos.object.records=100
cos.object.deadline.seconds=60
```
Assuming that at least one Kafka record is available to be written into an
object then objects will be created either: every minute, or after 100 Kafka
records have been received, whichever condition occurs first.

## Exactly once delivery

This connector can be used to provide exactly once delivery of Kafka records
into object storage. When used like this, data is copied without loss or
duplication.

Exactly once delivery requires that Kafka records are combined into objects in a
completely deterministic way. Which is to say that given a stream of Kafka
records to process, the connector will always group the same records into each
Cloud Object Storage object.

Using either the `cos.object.records` property or the
`cos.object.interval.seconds` property (or both together) will result in
deterministic processing of Kafka records into objects. However the
`cos.object.deadline.seconds` option cannot be used for exactly once delivery as
the grouping of Kafka records into objects is dependent on the speed at which
the system hosting the connector can process records.


## Provisioning an IBM Cloud Object Storage Service instance

To use this connector you must provision an instance of the [IBM Cloud Object Storage Service](https://cloud.ibm.com/catalog/services/cloud-object-storage). Once provisioned, navigate to the `Service Credentials` tab in your instance to retrieve the required configurations for the connector. You also need to create a Bucket.

## Running the connector

To run the connector, you must have:

* The JAR from building the connector
* A properties file containing the configuration for the connector
* Apache Kafka 1.1.0 or later, either standalone or included as part of an offering such as [IBM Event Streams](https://cloud.ibm.com/catalog/services/event-streams)

The connector can be run in a Kafka Connect worker in either standalone (single process) or distributed mode.

### Standalone Mode

You need two configuration files, one for the configuration that applies to all of the connectors such as the Kafka bootstrap servers, and another for the configuration specific to the IBM Cloud Object Storage sink connector such as the connection information for your Cloud Object Storage service. For the former, the Kafka distribution includes a file called connect-standalone.properties that you can use as a starting point. For the latter, you can use `config/cos-sink.properties` in this repository after replacing all placeholders.

To run the connector in standalone mode from the directory into which you installed Apache Kafka, you use a command like this:

```shell
bin/connect-standalone.sh connect-standalone.properties cos-sink.properties
```

### Distributed Mode

You need an instance of Kafka Connect running in distributed mode. To start the connector, you can use `config/cos-sink.json` in this repository after replacing all placeholders.

To run the connector in distributed mode, you use a command like this:

```shell
curl -X POST -H "Content-Type: application/json" http://localhost:8083/connectors \
  --data "@./config/cos-sink.json" 
```

