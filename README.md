# Debesync Consumer

The Debesync Consumer is an Open source project built on top of Quarkus that provides a low latency streaming platform for reading the CDC events published by the [Debezium Source Connector](https://github.com/debezium/debezium) to various destinations such as Kafka, Kinesis etc.
This utilizes some functionalities of the Debezium to provide customization and tolerance in case of errors to ensure that there is no data loss.

Brief Summary of the application: https://crrajavignesh2.medium.com/debesync-a-consumer-application-for-implementing-data-streaming-with-debezium-437c04231532

## Basic Architecture
The Debesync Consumer is a consumer application built on top of the Kafka Sink connectors (kafka Connect) to run them in a standalone format and form a complete pipeline when utilized with the Debezium Source connector. 
This can be used to set up consumer application that can be used to replicate the data from the kafka topic to the desired target database with minimal code changes and provides additional features of durability, reliability and fault tolerance. Multiple instances of the connectors can be deployed listening to the same kafka topic to achieve higher throughput and performance.

## Common Use Cases

There are a large number of use cases for utilizing the debesync consumer, however, some of the most prominent ones are:

### Data Replication

This can be used to set up data replication between two different databases with options to include data transformations and predicates including calls to external services to customize the structure of data.
This can be used for both homogenous and heterogeneous data transfers and limited only by the availability of the sink connectors for the target database. Even then, kafka provides support for implementing our own custom connectors based on the use case.

### Schema Generation

In case of Relational Databases, the Debezium source Connector maintains a schema history topic or file. The debesync consumer provides the option to utilize that for the purpose of schema generation also.

### Cache Management

Utilizing the Kafka Sink Connectors created for the Redis application by [jcustenborder/kafka-connect-redis](https://github.com/jcustenborder/kafka-connect-redis) and Debezium, a data pipeline can be set up between a source database and Redis database such that the Redis cache is always in sync with the database. The pipeline itself takes care of all the operations like record generation/update and cache invalidation.

### Simplifying monolithic applications

As mentioned in [Debezium - Simplifying monolithic applications](https://github.com/debezium/debezium?tab=readme-ov-file#simplifying-monolithic-applications), this can be used to set up the pipeline to prevent "dual-writes" being done by the application using the data transformations and minimize the application performance.

### Data Integration

This can be used to set up Data Integration pipelines as mentioned in [Debezium - Data integration](https://github.com/debezium/debezium?tab=readme-ov-file#data-integration) using kafka connect and Kafka Transformations.

## Building Debesync

The following software is required to work with the Debezium codebase and build it locally:

* [Git](https://git-scm.com) 2.2.1 or later
* JDK 11 or later, e.g. [OpenJDK](http://openjdk.java.net/projects/jdk/)
* [Docker Engine](https://docs.docker.com/engine/install/) or [Docker Desktop](https://docs.docker.com/desktop/) 1.9 or later
* [Apache Maven](https://maven.apache.org/index.html) 3.8.1 or later  
  (or invoke the wrapper with `./mvnw` for Maven commands)

See the links above for installation instructions on your platform. You can verify the versions are installed and running:

    $ git --version
    $ javac -version
    $ mvn -version
    $ docker --version

### Building the code

First, Checkout the source code from the repository

    $ git clone https://github.com/Vicky-cmd/debesync.git
    $ cd debesync

The project can be built using the bellow command

    $ mvn clean install -DskipITs -DskipTests

Running the maven install or package command over the `pom.xml` in the root directory of the project installs and sets up all the necessary dependencies in your local system.

## Application Overview

The application consists of various components to enable plug and play type of configuration where the end user can configure only the dependencies needed by them.
The various components available are:

### debesync-api

This is the base dependency used by all the components and will contain the definitions for the various components like the emitters, engine etc. The various components defined are:

#### Emitters

These are the source of the events and are used to read/stream through the data stored in the event streaming platforms. The implementation for this can be customized based on the type of event streaming system, and would need to be specified in the configuration - `debezium.source.connector.class`. The classes for this must implement the interface `ConsumerSyncEngine.ChangeEmitter<R>`. The kafka based implementation would be found under the **debesync-server-kafka** module.

#### ChangeConnector

The implementation for this would be called for starting the data consumer application. This can be used provide additional functionalities or validations before the data load. The type of the Change connector would be determined based on the configuration `debezium.connector.type`. 
For e.g. in case of mysql, the connector can be used to also read through the schema history stream created by the debezium application and execute the necessary ddl statements in the target database.
The implementations for this must extend the `AbstractConnector` class or implement the `ConsumerSyncEngine.ChangeConnector<R>` class.

#### ErrorReporter

This is the implementation of the error reporting mechanism utilized in Kafka Connect to report the errors to a dlq target. The type of the error reporter will depend on the type of the emitter that has been configured. For e.g.: In case of Kafka Emitter, the error reporter will also be using a dlq with Kafka as the destination. The configuration for the error reporter are mentioned in detail in the below section.

### debesync-embedded

This module contains the implementation for the embedded engine that will take care of all the activities like setting up the kafka sink connector, retry mechanisms etc. The engine also contains the support for Single Message Transformations (SMTs) as defined by the Kafka Connect. All the Transformations provided by debezium are also supported here.
The debesync application also supports the custom transformation and predicates provided/supported by the debezium application with similar configuration and operations.

### debesync-server-core

This is the core module of the debesync application with the logic to start and manage the server. This is the main dependency that would need to be added by the user to their quarkus application to use the debesync server.

### debesync-server-kafka

This module contains the kafka implementation of the emitter. This should be used to configure the application to read through the cdc records in the kafka topics. This will also contain the implementation for the dlq records processing.

### debesync-mongo-connector

This module contains the implementation for configuring and using the mongo database as the target database/sink for the data pipeline. This also contains the additional transformations and post processors needed for working with Mongo as the target database.

### debesync-jdbc-base-connector

This is the base module that will be referred for the implementations for sql databases. This will have the definitions for some custom features for jdbc databases like reading the Schema History changes store managed by debezium and replicates the schema changes in the target database.

### debesync-mysql-connector

This module contains the implementation for configuring and using the mysql database as the target database/sink for the data pipeline. This also contains additional logic for implementing the schema loading logic while listening to the debezium schema history store.

### debesync-quarkus-bom

This module contains the bom file that can be used for dependency management of the debesync modules.

## Configuration

All the configuration related to the debesync consumer will be defined under the same prefix as debezium for maintaining the simplicity of the configurations. The config will be of the type `debezium.` (i.e. prefixed as `debezium.*`). The various sub configurations under this are mentioned in the below sections.

### Debezium Format Configuration

The debesync application supports the configuration of the message input format for the key and value of the kafka message separately. By default, JSON is used as the input format for the messages, but any arbitrary implementation of the `Converter` can be used.

| Property               | Default | Description                                                                                                                                                                                                                                                                                                                                                                                                                                |
|------------------------|---------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| debezium.format.key    | json    | The name of the inout format for the key. <br/> **Accepted Values**<br/>json<br/>jsonbytearray<br/>avro<br/><br/>The implementation for the key converter used can be overriden by configuring the `debezium.sink.converters.key.class` property. <br/>The different additional configurations for the key converter can be provided by configuring them under `debezium.sink.converters.key.properties.*` prefix.                         |
| debezium.format.value  | json    | The name of the inout format for the key. <br/> **Accepted Values**<br/>json<br/>jsonbytearray<br/>avro<br/>cloudevents<br/><br/>The implementation for the value converter used can be overriden by configuring the `debezium.sink.converters.value.class` property. <br/>The different additional configurations for the value converter can be provided by configuring them under `debezium.sink.converters.value.properties.*` prefix. |
| debezium.format.header | json    | The name of the inout format for the key. <br/> **Accepted Values**<br/>json<br/>jsonbytearray<br/><br/>The implementation for the header converter used can be overriden by configuring the `debezium.sink.converters.header.class` property. <br/>The different additional configurations for the header converter can be provided by configuring them under `debezium.sink.converters.header.properties.*` prefix.                      |

### Source Configuration

These will be configured as the source configuration under the prefix `debezium.source` and will be of the format `debezium.source.*`

| Option                | Default | Required | Description                                                                                                                                                                                                                                                                                                                                           |
|-----------------------|---------|----------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| connector.class       |         | true     | Specifies the implementation class for the Emitter. The emitters are the source of the consumer application and read the data from the event store like kafka. Sample value: `com.infotrends.in.debesync.kafka.emitters.KafkaChangeEmitter`                                                                                                           |
| kafka.consumer.*      |         | true     | This contains all of the configurations required to configure the Emitter which can be used to listen to the events from the kafka topic/event store. The various configurations found here would be used to configure the kafka consumer utilized in the emitter. <br/> This configuration would be used along with the kafka emitter implementation |
| kafka.poll.duration   | 1000    | false    | This configuration is used to set the duration for polling the kafka topic in milli seconds. By default, the value is set as 1 second (`1000`).                                                                                                                                                                                                       |
| kafka.commit.duration | 1000    | false    | The default duration for which the kafka consumer will attempt to commit the offsets to the kafka topic. By default, it is set as 1 second (`1000`).                                                                                                                                                                                                  |

### Sink Configuration

These will be configured as the sink configuration under the prefix `debezium.sink` and will be of the format `debezium.sink.*`

| Option                                        | Default                                                                                                                                                                                                                                                                                                                                                                                                                                                    | Required | Description                                                                                                                                                                                                                                                                                   |
|-----------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| name                                          |                                                                                                                                                                                                                                                                                                                                                                                                                                                            | true     | The name for the connector application.                                                                                                                                                                                                                                                       |  
| connector.class                               |                                                                                                                                                                                                                                                                                                                                                                                                                                                            | true     | The sink connector implementation to be used for the sink connector. This could be any class that implements the `org.apache.kafka.connect.sink.SinkConnector` class.                                                                                                                         |
| type                                          |                                                                                                                                                                                                                                                                                                                                                                                                                                                            | true     | This configuration denotes the type of the sync connector and is used to identify the configuration for kafka sink connector implementation.                                                                                                                                                  |
| {type}.*                                      |                                                                                                                                                                                                                                                                                                                                                                                                                                                            | true     | This configuration contains all the parameters/configuration needed for the implementation of the kafka sink connector that is getting used.                                                                                                                                                  |
| converters.key.class                          | Depends on the value of the debezium key format. The default values based on the type of config are: <br/>**json**: org.apache.kafka.connect.json.JsonConverter<br/>**json + apicurio url**: io.apicurio.registry.utils.converter.ExtJsonConverter<br/>**avro**: io.confluent.connect.avro.AvroConverter<br/>**avro + apicurio url**: io.apicurio.registry.utils.converter.AvroConverter<br/>**cloudevent**: io.debezium.converters.CloudEventsConverter   | false    | This is used to configure the instance of the key converters for converting the binary data that is getting read the emitter.                                                                                                                                                                 |
| converters.key.properties.*                   |                                                                                                                                                                                                                                                                                                                                                                                                                                                            | false    | This is used to configure the additional properties for the key converters                                                                                                                                                                                                                    |
| converters.value.class                        | Depends on the value of the debezium value format. The default values based on the type of config are: <br/>**json**: org.apache.kafka.connect.json.JsonConverter<br/>**json + apicurio url**: io.apicurio.registry.utils.converter.ExtJsonConverter<br/>**avro**: io.confluent.connect.avro.AvroConverter<br/>**avro + apicurio url**: io.apicurio.registry.utils.converter.AvroConverter<br/>**cloudevent**: io.debezium.converters.CloudEventsConverter | false    | This is used to configure the instance of the value converters for converting the binary data that is getting read the emitter.                                                                                                                                                               |
| converters.value.properties.*                 |                                                                                                                                                                                                                                                                                                                                                                                                                                                            | false    | This is used to configure the additional properties for the value converters                                                                                                                                                                                                                  |
| errors.deadletterqueue.topic.name             | ${topic}-dlq                                                                                                                                                                                                                                                                                                                                                                                                                                               | false    | This is the configuration for the topic name for the dead letter queues for the records that are getting errored out. It uses `${topic}-dlq` by default where the `${topic}` is replaced by the topic name for the errored out records                                                        |                                                                                                                                                                      |
| errors.deadletterqueue.context.headers.enable | false                                                                                                                                                                                                                                                                                                                                                                                                                                                      | false    | This is used to specify the error details and the context details like the partition key id etc are populated as the headers for the dlq record.                                                                                                                                              |
| errors.deadletterqueue.producer.*             |                                                                                                                                                                                                                                                                                                                                                                                                                                                            | false    | This configuration is used to configure the kafka producer used for pushing out the errored records to the dlq topic                                                                                                                                                                          |
| errors.retry.timeout                          | 0                                                                                                                                                                                                                                                                                                                                                                                                                                                          | false    | The maximum duration in milliseconds that a failed operation will be reattempted. The default is 0, which means no retries will be attempted. Use -1 for infinite retries.                                                                                                                    |
| errors.retry.delay.max.ms                     | 60000                                                                                                                                                                                                                                                                                                                                                                                                                                                      | false    | The maximum duration in milliseconds between consecutive retry attempts.                                                                                                                                                                                                                      |
| errors.tolerance                              | none                                                                                                                                                                                                                                                                                                                                                                                                                                                       | false    | Behavior for tolerating errors during connector operation. 'none' is the default value and signals that any error will result in an immediate connector task failure; 'all' changes the behavior to skip over problematic records.                                                            |
| transforms, transforms.*                      |                                                                                                                                                                                                                                                                                                                                                                                                                                                            | false    | The configurations that can be used to configure and setup Custom Single Message Transformation chains. More details are provided in the section for [Transformations and Predicates](#transformations-and-predicates)                                                                        |
| predicates, predicates.*                      |                                                                                                                                                                                                                                                                                                                                                                                                                                                            | false    | The configurations that can be used to configure the predicates that can used using the Transformations so that they are applied only over a subset of the messages being read. Further details are provided in the section [Transformations and Predicates](#transformations-and-predicates) |

### Change Connector Configuration

These will be configured as the change connector configuration under the prefix `debezium.connector` and will be of the format `debezium.connector.*`

| Option | Default  | Required | Description                                                                                                                                                                                                                                                                |
|--------|----------|----------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| type   | default  | false    | The type of the Change connector to be used by the debesync application. It refers to the classes that implement the `ConsumerSyncEngine.ChangeConnector<R>`. These implementation provide additional functionality based on the type of connector that is getting used.   |

### MySQL Change Connector

The MySQL implementation provides a Change connector with additional functionalities for taking care of the schema generation also. This will take care of creating and updating the tables in the target database. This can be used to restrict the operation to only specific source databases. 
The below configuration are specific for using the connector implementation for MySQL. For this, the `debezium.connector.type` is set as `mysql` along with the `debesync-mysql-connector` dependency provided in the classpath of the application.

| Option                                   | Default | Required                                                         | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
|------------------------------------------|---------|------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| operation.mode                           | never   | false                                                            | This is used to configure the type of operation for the mysql connector. <br/> This supports the following values: <br/> `initial`: When the connector starts it performs schema population operation based on the schema details being stored by the debezium schema history store (topic/file) before starting the kafka sink connector<br/> `schema_only`: The connector only performs the schema population operation. <br/> `never`: The connector does not perform any schema population operation and directly starts the sink connector. |
| sourceDatabases                          |         | false                                                            | The `,` seperated list of databases that are used to filter out the ddl statements stored in the schema history store/topic and execute only those statements that were extracted from the databases specified here.                                                                                                                                                                                                                                                                                                                             |
| enable.delete                            | false   | false                                                            | This is used to specify if the ddl queries for deleting the tables captured by the debezium in the schema history are to be executed in the target database.                                                                                                                                                                                                                                                                                                                                                                                     |
| schema.history.internal.type             |         | required <br/> if `operation.mode` set as initial or schema_only | This refers to the implementation of `com.infotrends.in.debesync.jdbc.storage.SchemaHistoryStorageReader`.  This instance is configured based on the source for reading the schema history topic created by debezium.<br/>For kafka the implementation to be used is : `com.infotrends.in.debesync.jdbc.storage.kafka.KafkaSchemaHistoryStorageReader`.                                                                                                                                                                                          |
| schema.history.internal.kafka.topic      |         | true                                                             | This is topic used by the debezium source connector to store the schema history.                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| schema.history.internal.kafka.consumer.* |         | false                                                            | This contains the configuration for the consumer to be used to override the default configurations for the kafka consumer to be used for listening to the schema history topic.                                                                                                                                                                                                                                                                                                                                                                  |

## Using the Debesync Consumer

To use the Debesync Engine module and server, add the `debesync-server-core` dependency/jar to the application. This should have all the dependencies needed to set up the debesync server and embedded engine. For maven, this involves adding the below dependency to the application pom file: 

    <dependency>
      <groupId>com.infotrends.in</groupId>
      <artifactId>debesync-server-core</artifactId>
      <version>${debesync.version}</version>
    </dependency>

Here, the `debesync.version` refers to the version of the debesync application to be used.

However, in order to run the desync application, the implementations for the emitter, kafka sink connector must be provided. 
For reading the change events from the kafka source, the `debesync-server-kafka` implementation for the emitter should be added under the dependencies for the application.

    <dependency>
      <groupId>com.infotrends.in</groupId>
      <artifactId>debesync-server-kafka</artifactId>
      <version>${debesync.version}</version>
    </dependency>

Likewise, the debesync implementations for the mongo and mysql sink connectors should be added to the dependency based on the type of sink:

For Mongo:

    <dependency>
      <groupId>com.infotrends.in</groupId>
      <artifactId>debesync-mongo-connector</artifactId>
      <version>${debesync.version}</version>
    </dependency>

For Mongo:

    <dependency>
      <groupId>com.infotrends.in</groupId>
      <artifactId>debesync-mysql-connector</artifactId>
      <version>${debesync.version}</version>
    </dependency>

**Note:** It is not mandatory to use these specific implementations for configuring the debesync application. We can use any of the kafka sink connector dependency with the required driver and client as necessary. However, the above-mentioned dependencies have some additional features like schema population and custom transformers for selecting the table/collection name from the topic as well as extracting the id.

The `debesync-quarkus-bom` can be configured under the dependency management section for managing all the debesync dependencies.

    <dependencyManagement>
      <dependencies>
        <dependency>
          <groupId>com.infotrends.in</groupId>
          <artifactId>debesync-quarkus-bom</artifactId>
          <version>${debesync.version}</version>
          <type>pom</type>
          <scope>import</scope>
        </dependency>
      </dependencies>
    </dependencyManagement>

## Transformations and Predicates

The debesync also supports the use of Single Message Transformations (SMTs) using [Transformations](https://kafka.apache.org/documentation/#connect_transforms) and [Predicates](https://kafka.apache.org/documentation/#connect_predicates) including the ones provided by [Debezium Source Connector](https://debezium.io/documentation/reference/stable/transformations/index.html).

All the transformations are to be configured under the `debezium.sink.*` prefix.

A transformation can be configured to make lightweight message-at-a-time modifications including chaining multiple transformation in a sequence.
The configuration for the transformation are:

* **transforms**: List of the aliases for the transformations based on the order in which they are to be executed.
* **transforms.${alias}.type**: The fully qualified class name to be used for the transformation.
* **transforms.${alias}.${transformationSpecificConfig}**: The specific configurations needed for the specified transformation. 
* **transforms.${alias}.predicate**: The alias name of the predicate to be used along with the transformation.
* **transforms.${alias}.negate**: The boolean configuration to be used to specify if we need to negate the results from predicate.

All the transformations can be configured to be used with a predicates so that they are applied only on specific messages based on the conditions specified in the predicate. As such, these could be used to filter out the messages based on parameters like topic name, only if a specific field is present etc.

The general configuration for ethe predicates are:

* **predicates**: The set of aliases of the predicates to be applied over the transformations.
* **predicates.${alias}.type**: The fully qualified class name to be used for the predicate.
* **predicates.${alias}.${predicateSpecificConfig}**: The specific configurations needed for the specific predicate.

### MongoDB

#### com.infotrends.in.debesync.mongo.namespace.mappings.TopicNameSpaceMapper

This is an implementation of the namespace mapper configured to be used by the mongo sink connector for extracting Database and the collection name for the CDC Event/Record based on the topic name.
This can be used in cases where the topic name with the events contains the database name and collection name seperated by a common delimiter.

For e.g: The configuration to be used to extract the database and collection name from the topic `mongo.infotrends.users` for target database `infotrends` and collection `users` is as shown below: 

    debezium.sink.mongo.namespace.mapper=com.infotrends.in.debesync.mongo.namespace.mappings.TopicNameSpaceMapper
    debezium.sink.mongo.namespace.mapper.topic.separator=\\.
    debezium.sink.mongo.namespace.mapper.location.database=1
    debezium.sink.mongo.namespace.mapper.location.collection=2

Here, the `topic.separator` is used to specify the delimiter. The `location.database` and `location.collection` properties are used for specifying the position of the database name and collection name in the Topic.

However, in cases where the `database` is configured for the sink configuration, that configuration is given precedence over the configuration for the database name to be used for the target database.

### MySQL

#### com.infotrends.in.debesync.mysql.transforms.ExtractTopicName

This is a custom transformation provided for extracting the target table name from the topic name of the CDC name. 

For e.g: The configuration to be used to extract the table name from the topic name for the topic `mysql.infotrends.users` for the table `users` is given below:

    debezium.sink.transforms.extractTopic.type=com.infotrends.in.debesync.mysql.transforms.ExtractTopicName
    debezium.sink.transforms.extractTopic.table.separator=\\.
    debezium.sink.transforms.extractTopic.table.location=2

Here, a transformation `extractTopic` is defined with the type `com.infotrends.in.debesync.mysql.transforms.ExtractTopicName`.
The configuration `table.separator` is used to define the seperator for separating the table name from the topic and `table.location` is used to specify the position of the table name in topic.


## Example Projects

The example implementation for the debesync consumer and sample configurations for configuring the consumer for the Consumer applications for various scenarios is provided under the folder [examples](/examples). It also considers a sample configuration file for configuring the corresponding debezium producer application.
