# Avro Util
Avro Util enables marshalling of Apache Avro object using widely supported data formats JSON as well as binary. The
util also supports compression such as Java deflate and GZIP. Other compression algorithms can be added if needed. 

### Usage
The util can be used either directly obtaining serializers and deserializers from the factory or via configuring  Http 
Message Converters within the Spring Framework.

#### AvroSerializeFactory
Configure a serializer/deserializer
```java
// Serialize AVRO object to JSON
final Serializer serializerJson = new AvroSerializerFactory().serializer(AvroFormat.JSON, CompressorType.NONE);

// Serialize AVRO object to Binary
final Serializer serializerBinary = new AvroSerializerFactory().serializer(AvroFormat.BINARY, CompressorType.NONE);

// Deserializer JSON String to Avro
final Deserializer deserializerJson = new AvroDeserializerFactory().deserializer(AvroFormat.JSON, CompressorType.NONE);

// Deserializer Binary String to Avro
final Deserializer deserializerBinary = new AvroDeserializerFactory().deserializer(AvroFormat.BINARY, CompressorType.NONE);

// Serialize AVRO object to JSON using Java deflater compression
final Serializer serializerJson = new AvroSerializerFactory().serializer(AvroFormat.JSON, CompressorType.DEFLATER);

// Serialize AVRO object to JSON using GZIP compression
final Serializer serializerJson = new AvroSerializerFactory().serializer(AvroFormat.JSON, CompressorType.GZIP);
```
Perform serialization/deserialization
```java
// Serialize AVRO object to JSON
final TestObjectParent testObject = new TestObjectParent();
byte[] result = serializerJson(testObject);
TestObjectParent resultObject = (TestObjectParent)deserializerJSON.deserialize(TestObjectParent.class, result);
```

### Useful information and resources
Further information about AVRO concepts can be found here:

* [Apache Avro Homepage.](https://avro.apache.org) The place where everything has started
* [Apache Avro Getting Started Page.](https://avro.apache.org/docs/current/gettingstartedjava.html) A short guide for getting started with Apache Avro using Java

