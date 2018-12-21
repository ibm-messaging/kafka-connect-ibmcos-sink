# kafka-connect-ibmcos-sink

Kafka Connect Sink Connector for [IBM Cloud Object
Storage](https://console.bluemix.net/catalog/services/cloud-object-storage).

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
                                        Must be one of: `cross-region`,
                                        `regional`, or `single-site`.
- `os.endpoint.visibility` _(required)_ - Specify `public` to connect to the
                                          Object Storage service over the public
                                          internet, or `private` to connect from
                                          a connector running inside the
                                          IBM Cloud network, for example from an
                                          IBM Cloud Kubernetes Service cluster.
- `os.service.crn`_(required)_ - CRN for the Object Storage service instance.

