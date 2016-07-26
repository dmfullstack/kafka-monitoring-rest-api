# kafka-monitoring-rest-api
A RESTful API for monitoring Kafka cluster

## Build
```
gradle build
```

## Run
```
gradle bootRun
```

## Test

Make a HTTP GET call to find offset info for each partition of a Kafka topic given a consumer group

<b>Sample HTTP Endpoint:</b>

http://localhost:8080/offsets?brokers=localhost:9092&topic=test&group=testgroup

<b>Sample Successful Response</b>

```json
{
  "0": {
    "latestOffset": 29,
    "lag": 2,
    "commitedOffset": 27
  },
  "1": {
    "latestOffset": 56,
    "lag": 0,
    "commitedOffset": 56
  }
}
```

Make a HTTP GET call to find about whether each of provided Kafka brokers is up and running

<b>Sample HTTP Endpoint:</b>

http://localhost:8080/checkBrokerStatus?brokers=localhost:9094,localhost:9093,localhost:9092

<b>Sample Successful Response</b>

```json
{
  "localhost:9092": true,
  "localhost:9094": true,
  "localhost:9093": true
}
```

## To Do's

- This project works with Kafka 0.9. Compatibility testing with Kafka 0.8 and 0.10 is pending.
- Add other useful monitoring metrics.

