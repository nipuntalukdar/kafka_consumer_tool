* kafka consumer tool *
=======================

Kafka consumer tool 
-------------------

This tool can be used to set offsets, get offsets and list consumers in a consumer group.
Right now it works for Kafka 0.9 onwards and with the new consumer. I am planning to add support for
all 0.8 version and old consumer as well.

Installation
------------
You need to have golang installed to install the tool. I haven't uploaded any prebuilt binary for
any platoform. 
Run the below command:

**$ go get github.com/nipuntalukdar/kafka_consumer_tool**

Running the tool
----------------
Example to set offsets of topic  (example input json topic_offsets.json file is included along with the source):

**$ kafka_consumer_tool -consumergroup  newgroup -inputjson topic_offsets.json  -brokers 127.0.0.1:9092  --command commitoffsets**

Detailed usage shown below:
```bash
Usage of kafka_consumer_tool:
  -brokers string
        Bootstrap brokers comma separated list (default "127.0.0.1:9092")
  -command string
        command to execute, valid commands are: commitoffsets, getoffsets, listconsumers (default "getoffsets")
  -consumergroup string
        consumer group name (default "testgroup")
  -inputjson string
        JSON file containing the input details, needed for getoffsets and commitoffsets (default "topic_offsets.json")
  -offsetRetentionTime int
        Offset retention time for commitoffsets in milliseconds (default 3155760000000)

The schema for Input json (needed for commitoffsets and getoffsets) is as shown below:

{
  "$schema": "http://json-schema.org/draft-04/schema#",
  "items": {
    "type": "object",
    "properties": {
      "offsets": {
        "items": {
          "items": {
            "type": "integer"
          },
          "type": "array",
          "maxItems": 2,
          "minItems": 2
        },
        "type": "array"
      },
      "topic": {
        "type": "string",
        "minLength": 1,
        "maxLength": 1024
      }
    },
    "additionalProperties": false,
    "required": [
      "topic",
      "offsets"
    ]
  },
  "type": "array"
}

```


