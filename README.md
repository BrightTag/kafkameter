# kafkameter - Kafka JMeter Extension

This extension provides two components:

* Kafka Producer Sampler: sends keyed messages to Kafka
* Load Generator Config Element: generates messages as synthetic load.

The purpose of the Load Generator Config Element is to dynamically create the messages for sending
to Kafka. You could alternatively use the CSV Data Set or another source for messages.

As each application has its own input message format and load distribution, the Load Generator
Config provides a pluggable framework for application-specific message generation. An example
implementation is included.

A sample JMeter Test Plan demonstrates wiring the Load Generator and Kafka Sampler together for a
complete test. A [Counter](http://jmeter.apache.org/usermanual/component_reference.html#Counter)
is used as the `kafka_key` to ensure the load is distributed across all available Kafka partitions.
This sample JMeter Test Plan can be found in `Tagserve-Kafka.jmx`.

## Install

Build the extension:

    mvn package

Install the extension into `$JMETER_HOME/lib/ext`:

    cp target/kafkameter-x.y.z.jar $JMETER_HOME/lib/ext

## Usage

### Kafka Producer Sampler

After installing `kafkameter`, add a Java Request Sampler and select the `KafkaProducerSampler`
class name. The following properties are required.

* **kafka_brokers**: comma-separated list of hosts in the format (hostname:port).
* **kafka_topic**: the topic in Kafka to which the message will be published.
* **kafka_key**: the partition key for the message.
* **kafka_message**: the message itself.

You may also override the following:

* **kafka_message_serializer**: the Kafka client `value.serializer` property.
* **kafka_key_serializer**: the Kafka client `key.serializer` property.
* **kafka_use_ssl**: set to 'true' if you want to use SSL to connect to kafka.
* **kafka_ssl_truststore**: the truststore certificate file (with path).
* **kafka_ssl_truststore_password**: the truststore certificate file password.
* **kafka_ssl_keystore**: the keystore certificate file (with path).
* **kafka_ssl_keystore_password**: the keystore certificate file password.
* **kafka_compression_type**: the compression type, for example gzip. Leave the field empty for no encryption.
* **kafka_partition**: the partition number to use. Use a fixed value or calculate it on a per thread basis. Example: `${__jexl(${__threadNum} % 10)}` This would spread the threads across 10 partitions.
 Works best if the thread numbers are a multiple of the total partitions. If you leave this field empty, the Kafka library will calculate a value based on your key.

### Load Generator Config

After installing `kafkameter`, the Load Generator will be available as a Config Element.

This component reads a Synthetic Load Description from a given file, uses a Synthetic Load Generator
plugin to generate a new message on each iteration, and makes this message available to other
elements under the given variable name. The Synthetic Load Description format will be specific
to each Synthetic Load Generator.

#### Simplest Possible Example

A dummy example is useful for demonstrating integration with the Load Generator framework in JMeter.

Create a file called `DummyGenerator.java`:

    import org.apache.commons.lang3.tuple.Pair;
    import co.signal.loadgen.SyntheticLoadGenerator;

    public class DummyGenerator implements SyntheticLoadGenerator {

      public DummyGenerator(String ignored) {}

      @Override
      public Pair<String, String> nextMessage() {
        return Pair.of("Hey! Dum-dum!" , "You give me gum-gum.");
      }
    }

Now compile this class:

    javac DummyGenerator.java -classpath target/kafkameter-x.y.z.jar

Package it into a jar:

    jar cvf DummyGenerator.jar *.class

Place the jar into JMeter's `lib/ext` directory:

    mv DummyGenerator.jar $JMETER_HOME/lib/ext/

Now you should see `DummyGenerator` as an option in the Load Generator's "Class Name" drop-down.

#### Realistic Example

This example will use one of [Signal's](signal.co) own domains: tag serving.

Let's say we have a collection of client websites identified by an alphanumeric `siteId`. Each site
contains a collection of pages identified by a numeric `pageId`. Any given request may match zero
or more pages. Each page contains a collection of tags identified by a numeric `tagId` which will be
served whenever the containing page matches. Each request occurs at a particular epoch `timestamp`.

Given this model, we will represent the domain-specific message in JSON as

    {
      "siteId": <string>,
      "timestamp": <long>,
      "pageIds": [<pageId>, ...],
      "tagIds": [<tagId>, ...]
    }

We refer to this as a `TagRequestMetrics` message.

The Load Generator will create `TagRequestMetrics` messages according to the distribution
specified by the example `Synthetic Tagserve Load Description`, described below.

##### Synthetic Tagserve Load Description

The Synthetic Tagserve Load Description has the following format:

    {
      <siteId>: {
        "weight": <double>,
        "pages": {
            <pageId>: {
                "weight": <double>,
                "tags": [<tagId>, ...]
            }
        }
      }
    }

The weights are used to determine the next `TagRequestMetrics` message for the `KafkaProducerSampler`.
The weight for a site represents the percentage of total traffic belonging to the site. However,
the weight for a page represents the probability that the page will be matched in any given request.

Said another way, because `TagRequestMetrics` represents a single site, the weights for the
different sites must sum to unity. However, a single request can match multiple pages independently,
so these weights are independent; i.e., they do not have to sum to unity.

##### Synthetic Tagserve Load Algorithm

For each iteration, generate a uniformly random variate between `(0, 1]`. The mapping below would
represent which site's load configuration to use based on the variate.

    site1: (0.0, 0.6]
    site2: (0.6, 1.0]

For each page within the load configuration, generate a uniformly random variate between `(0, 1]`.
Reject any pages with lesser weights. The tags included from each selected page are unioned.

##### Example

For example, given the following Synthetic Load Description

    {
       "site1": {
          "weight": 0.6,
          "pages": {
              "123": {
                 "weight": 0.7,
                 "tags": [123, 567]
              },
              "234": {
                  "weight": 0.1,
                  "tags": [123, 234, 345, 456]
              }
          },
       },
       "site2": {
          "weight": 0.4,
          "pages": {
              "123": {
                 "weight": 0.7,
                 "tags": [123, 234]
              }
          }
       }
    }

if the random variates were (0.4, 0.6, 0.05), then we would match the load config for
"site1" with pages [123, 234] and tags [123, 234, 345, 456, 567].
