package ch.ichristen.avroUtil.serde;

import ch.ichristen.avroUtil.serde.compress.CompressorType;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.*;

import ch.ichristen.avroUtil.model.TestObjectParent;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import org.slf4j.LoggerFactory;

import static ch.ichristen.avroUtil.util.TestDataGenerator.buildTestObject;
import static ch.ichristen.avroUtil.util.TestDataGenerator.buildTestObjects;
import static org.junit.jupiter.api.Assertions.*;

@Slf4j
public class SerdeTest {

    final AvroSerializerFactory serializerFactory = new AvroSerializerFactory();
    final AvroDeserializerFactory deserializerFactory = new AvroDeserializerFactory();

    @BeforeAll
    public static void initializeTests() {
        Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        root.setLevel(Level.DEBUG);
    }

    //------------------------------------------------------------------
    // General Testing for serializer
    //------------------------------------------------------------------

    @Test
    public void testNullParameterForSerializer() throws SerializationException {
        assertNull(serializerFactory.serializer(AvroFormat.BINARY, CompressorType.NONE).serialize(null));
        assertNull(serializerFactory.serializer(AvroFormat.BINARY, CompressorType.DEFLATER).serialize(null));
        assertNull(serializerFactory.serializer(AvroFormat.BINARY, CompressorType.GZIP).serialize(null));
        assertNull(serializerFactory.serializer(AvroFormat.JSON, CompressorType.NONE).serialize(null));
        assertNull(serializerFactory.serializer(AvroFormat.JSON, CompressorType.DEFLATER).serialize(null));
        assertNull(serializerFactory.serializer(AvroFormat.JSON, CompressorType.GZIP).serialize(null));
    }

    @Test
    public void testUnsupportedParameterForSerializer() {
        Map map = new HashMap();
        assertThrows(SerializationException.class, () -> serializerFactory.serializer(AvroFormat.BINARY, CompressorType.NONE).serialize(map));
        assertThrows(SerializationException.class, () -> serializerFactory.serializer(AvroFormat.BINARY, CompressorType.DEFLATER).serialize(map));
        assertThrows(SerializationException.class, () -> serializerFactory.serializer(AvroFormat.BINARY, CompressorType.GZIP).serialize(map));
        assertThrows(SerializationException.class, () -> serializerFactory.serializer(AvroFormat.JSON, CompressorType.NONE).serialize(map));
        assertThrows(SerializationException.class, () -> serializerFactory.serializer(AvroFormat.JSON, CompressorType.DEFLATER).serialize(map));
        assertThrows(SerializationException.class, () -> serializerFactory.serializer(AvroFormat.JSON, CompressorType.GZIP).serialize(map));
    }

    //------------------------------------------------------------------
    // General Testing for deserializer
    //------------------------------------------------------------------

    @Test
    public void testUnsupportedClazzForDeserializer() {
        Map map = new HashMap();
        assertThrows(SerializationException.class, () -> deserializerFactory.deserializer(AvroFormat.BINARY, CompressorType.NONE).deserialize(map.getClass(), null));
        assertThrows(SerializationException.class, () -> deserializerFactory.deserializer(AvroFormat.BINARY, CompressorType.DEFLATER).deserialize(map.getClass(), null));
        assertThrows(SerializationException.class, () -> deserializerFactory.deserializer(AvroFormat.BINARY, CompressorType.GZIP).deserialize(map.getClass(), null));
        assertThrows(SerializationException.class, () -> deserializerFactory.deserializer(AvroFormat.JSON, CompressorType.NONE).deserialize(map.getClass(), null));
        assertThrows(SerializationException.class, () -> deserializerFactory.deserializer(AvroFormat.JSON, CompressorType.DEFLATER).deserialize(map.getClass(), null));
        assertThrows(SerializationException.class, () -> deserializerFactory.deserializer(AvroFormat.JSON, CompressorType.GZIP).deserialize(map.getClass(), null));
    }

    @Test
    public void testNullClazzForDeserializer() {
        assertThrows(SerializationException.class, () -> deserializerFactory.deserializer(AvroFormat.BINARY, CompressorType.NONE).deserialize(null, null));
        assertThrows(SerializationException.class, () -> deserializerFactory.deserializer(AvroFormat.BINARY, CompressorType.DEFLATER).deserialize(null, null));
        assertThrows(SerializationException.class, () -> deserializerFactory.deserializer(AvroFormat.BINARY, CompressorType.GZIP).deserialize(null, null));
        assertThrows(SerializationException.class, () -> deserializerFactory.deserializer(AvroFormat.JSON, CompressorType.NONE).deserialize(null, null));
        assertThrows(SerializationException.class, () -> deserializerFactory.deserializer(AvroFormat.JSON, CompressorType.DEFLATER).deserialize(null, null));
        assertThrows(SerializationException.class, () -> deserializerFactory.deserializer(AvroFormat.JSON, CompressorType.GZIP).deserialize(null, null));
    }

    @Test
    public void testSingelObjectWithNullDataForDeserializer() throws SerializationException {
        assertNull(deserializerFactory.deserializer(AvroFormat.BINARY, CompressorType.NONE).deserialize(TestObjectParent.class, null));
        assertNull(deserializerFactory.deserializer(AvroFormat.BINARY, CompressorType.DEFLATER).deserialize(TestObjectParent.class, null));
        assertNull(deserializerFactory.deserializer(AvroFormat.BINARY, CompressorType.GZIP).deserialize(TestObjectParent.class, null));
        assertNull(deserializerFactory.deserializer(AvroFormat.JSON, CompressorType.NONE).deserialize(TestObjectParent.class, null));
        assertNull(deserializerFactory.deserializer(AvroFormat.JSON, CompressorType.DEFLATER).deserialize(TestObjectParent.class, null));
        assertNull(deserializerFactory.deserializer(AvroFormat.JSON, CompressorType.GZIP).deserialize(TestObjectParent.class, null));
    }

    @Test
    public void testArrayWithNullDataForDeserializer() throws SerializationException {
        TestObjectParent[] test = new TestObjectParent[0];
        assertNull(deserializerFactory.deserializer(AvroFormat.BINARY, CompressorType.NONE).deserialize(test.getClass(), null));
        assertNull(deserializerFactory.deserializer(AvroFormat.BINARY, CompressorType.DEFLATER).deserialize(test.getClass(), null));
        assertNull(deserializerFactory.deserializer(AvroFormat.BINARY, CompressorType.GZIP).deserialize(test.getClass(), null));
        assertNull(deserializerFactory.deserializer(AvroFormat.JSON, CompressorType.NONE).deserialize(test.getClass(), null));
        assertNull(deserializerFactory.deserializer(AvroFormat.JSON, CompressorType.DEFLATER).deserialize(test.getClass(), null));
        assertNull(deserializerFactory.deserializer(AvroFormat.JSON, CompressorType.GZIP).deserialize(test.getClass(), null));
    }

    @Test
    public void testArrayWithEmptyDataForBinaryDeserializer() throws SerializationException {
        TestObjectParent[] test = new TestObjectParent[0];
        final byte[] binaryData = { };
        test = (TestObjectParent[])deserializerFactory.deserializer(AvroFormat.BINARY, CompressorType.NONE).deserialize(test.getClass(), binaryData);
        assertNotNull(test);
        assertEquals(0, test.length);
    }

    @Test
    public void testArrayWithEmptyArrayDataForJsonDeserializer() throws SerializationException {
        TestObjectParent[] test = new TestObjectParent[0];
        final byte[] jsonData = { '[', ']'};
        test = (TestObjectParent[])deserializerFactory.deserializer(AvroFormat.JSON, CompressorType.NONE).deserialize(test.getClass(), jsonData);
        assertNotNull(test);
        assertEquals(0, test.length);
    }

    @Test
    public void testArrayWithMalformedDataForJsonDeserializer() throws SerializationException {
        TestObjectParent[] test = new TestObjectParent[0];
        final byte[] jsonData = { '['};
        test = (TestObjectParent[])deserializerFactory.deserializer(AvroFormat.JSON, CompressorType.NONE).deserialize(test.getClass(), jsonData);
        assertNotNull(test);
        assertEquals(0, test.length);
    }

    @Test
    public void testArrayWithEmptyDataForJsonDeserializer() throws SerializationException {
        TestObjectParent[] test = new TestObjectParent[0];
        final byte[] jsonData = { };
        test = (TestObjectParent[])deserializerFactory.deserializer(AvroFormat.JSON, CompressorType.NONE).deserialize(test.getClass(), jsonData);
        assertNotNull(test);
        assertEquals(0, test.length);
    }

    //------------------------------------------------------------------
    // Testing one AVRO object per call -> single object
    // tests using 'standard' methods serialize and deserialize
    //------------------------------------------------------------------

    private void testSingleSerde(AvroFormat avroFormat, CompressorType compressorType) {

        Serializer ser = serializerFactory.serializer(avroFormat, compressorType);

        final TestObjectParent testObject = buildTestObject(0);
        byte[] result = null;
        try {
            result = ser.serialize(testObject);
            log.info("Result from serializer - Size: {} Binary Content: {}", result.length, result);
            log.debug("Result from serializer - Size: {} String Content: {}", result.length, new String(result));
        } catch (Exception e) {
            log.error("Exception during serialization of '" + testObject + "'", e);
        }

        TestObjectParent resultObject = null;
        Deserializer deser = deserializerFactory.deserializer(avroFormat, compressorType);
        if (result != null) {
            try {
                resultObject = (TestObjectParent)deser.deserialize(TestObjectParent.class, result);
                log.info("Result from deserializer {}", resultObject);
            } catch (Exception e) {
                log.error("Exception during deserialization of '" + result + "'", e);
            }
        }

        Assertions.assertEquals(testObject, resultObject);
    }

    @Test
    public void testSingleSpecificBinarySerde() {
        testSingleSerde(AvroFormat.BINARY, CompressorType.NONE);
    }

    @Test
    public void testSingleSpecificDeflaterBinarySerde() {
        testSingleSerde(AvroFormat.BINARY, CompressorType.DEFLATER);
    }

    @Test
    public void testSingleSpecificGZIPBinarySerde() {
        testSingleSerde(AvroFormat.BINARY, CompressorType.GZIP);
    }

    @Test
    public void testSingleSpecificJsonSerde() {
        testSingleSerde(AvroFormat.JSON, CompressorType.NONE);
    }

    @Test
    public void testSingleSpecificDeflaterJsonSerde() {
        testSingleSerde(AvroFormat.JSON, CompressorType.DEFLATER);
    }

    @Test
    public void testSingleSpecificGZIPJsonSerde() {
        testSingleSerde(AvroFormat.JSON, CompressorType.GZIP);
    }

    //------------------------------------------------------------------------------
    // Testing the use of a collection containing several AVRO objects
    // tests using standard method serialize and 'special' deserializeCollection
    //------------------------------------------------------------------------------

    private void testCollectionSerde(AvroFormat avroFormat, CompressorType compressorType) {

        Serializer ser = serializerFactory.serializer(avroFormat, compressorType);

        final ArrayList<TestObjectParent> testObjects = buildTestObjects(3);
//        testObjects.add(null);

        byte[] result = null;
        try {
            result = ser.serialize(testObjects.iterator());
            log.info("Result from serializer - Size: {} Binary Content: {}", result.length, result);
            log.debug("Result from serializer - Size: {} String Content: {}", result.length, new String(result));
        } catch (Exception e) {
            log.error("Exception during serialization of '" + testObjects + "'", e);
        }

        Collection<SpecificRecord> resultObjects = null;
        Deserializer deser = deserializerFactory.deserializer(avroFormat, compressorType);
        if (result != null) {
            try {
                resultObjects = deser.deserializeCollection(TestObjectParent.class, result);
                log.info("Result from deserializer {}", resultObjects);
            } catch (Exception e) {
                log.error("Exception during deserialization of '" + result + "'", e);
            }
        }

        Assertions.assertEquals(testObjects, resultObjects);
    }

    @Test
    public void testCollectionSpecificBinarySerde() {
        testCollectionSerde(AvroFormat.BINARY, CompressorType.NONE);
    }

    @Test
    public void testCollectionSpecificDeflaterBinarySerde() {
        testCollectionSerde(AvroFormat.BINARY, CompressorType.DEFLATER);
    }

    @Test
    public void testCollectionSpecificGZIPBinarySerde() {
        testCollectionSerde(AvroFormat.BINARY, CompressorType.GZIP);
    }

    @Test
    public void testCollectionSpecificJsonSerde() {
        testCollectionSerde(AvroFormat.JSON, CompressorType.NONE);
    }

    @Test
    public void testCollectionSpecificDeflaterJsonSerde() {
        testCollectionSerde(AvroFormat.JSON, CompressorType.DEFLATER);
    }

    @Test
    public void testCollectionSpecificGZIPJsonSerde() {
        testCollectionSerde(AvroFormat.JSON, CompressorType.GZIP);
    }

    //------------------------------------------------------------------------------
    // Testing the use of an array of AVRO objects
    // tests using standard methods serialize and deserialize
    //------------------------------------------------------------------------------

    private void testArraySerde(AvroFormat avroFormat, CompressorType compressorType) {

        Serializer ser = serializerFactory.serializer(avroFormat, compressorType);

        final TestObjectParent[] testObjects = buildTestObjects(3).toArray(new TestObjectParent[3]);

        byte[] result = null;
        try {
            // we only pass an array here !!
            result = ser.serialize(testObjects);
            log.info("Result from serializer - Size: {} Binary Content: {}", result.length, result);
            log.debug("Result from serializer - Size: {} String Content: {}", result.length, new String(result));
        } catch (Exception e) {
            log.error("Exception during serialization of '" + testObjects + "'", e);
        }

        TestObjectParent[] resultObjects = null;
        Deserializer deser = deserializerFactory.deserializer(avroFormat, compressorType);
        if (result != null) {
            try {
                resultObjects = (TestObjectParent[])deser.deserialize(TestObjectParent[].class, result);
                log.info("Result from deserializer {}", resultObjects);
            } catch (Exception e) {
                log.error("Exception during deserialization of '" + result + "'", e);
            }
        }

        Assertions.assertTrue(Arrays.equals(testObjects, resultObjects));
    }

    @Test
    public void testArraySpecificBinarySerde() {
        testArraySerde(AvroFormat.BINARY, CompressorType.NONE);
    }

    @Test
    public void testArraySpecificDeflaterBinarySerde() {
        testArraySerde(AvroFormat.BINARY, CompressorType.DEFLATER);
    }

    @Test
    public void testArraySpecificGZIPBinarySerde() {
        testArraySerde(AvroFormat.BINARY, CompressorType.GZIP);
    }

    @Test
    public void testArraySpecificJsonSerde() {
        testArraySerde(AvroFormat.JSON, CompressorType.NONE);
    }

    @Test
    public void testArraySpecificDeflaterJsonSerde() {
        testArraySerde(AvroFormat.JSON, CompressorType.DEFLATER);
    }

    @Test
    public void testArraySpecificGZIPJsonSerde() {
        testArraySerde(AvroFormat.JSON, CompressorType.GZIP);
    }

    //------------------------------------------------------------------------------
    // Testing the use of an iterator for a collection of AVRO objects
    // Result will be an array of AVRO objects
    // tests using standard methods serialize and deserialize
    //------------------------------------------------------------------------------

    private void testIteratorToArraySerde(AvroFormat avroFormat, CompressorType compressorType) {

        Serializer ser = serializerFactory.serializer(avroFormat, compressorType);

        final TestObjectParent[] testObjects = buildTestObjects(3).toArray(new TestObjectParent[3]);
        final Iterator testIterator = ((List)Arrays.asList(testObjects)).iterator();

        byte[] result = null;
        try {
            // we only pass an array here !!
            result = ser.serialize(testIterator);
            log.info("Result from serializer - Size: {} Binary Content: {}", result.length, result);
            log.debug("Result from serializer - Size: {} String Content: {}", result.length, new String(result));
        } catch (Exception e) {
            log.error("Exception during serialization of '" + testObjects + "'", e);
        }

        TestObjectParent[] resultObjects = null;
        Deserializer deser = deserializerFactory.deserializer(avroFormat, compressorType);
        if (result != null) {
            try {
                resultObjects = (TestObjectParent[])deser.deserialize(TestObjectParent[].class, result);
                log.info("Result from deserializer {}", resultObjects);
            } catch (Exception e) {
                log.error("Exception during deserialization of '" + result + "'", e);
            }
        }

        Assertions.assertTrue(Arrays.equals(testObjects, resultObjects));
    }

    @Test
    public void testIteratorToArraySpecificBinarySerde() {
        testIteratorToArraySerde(AvroFormat.BINARY, CompressorType.NONE);
    }

    @Test
    public void testIteratorToArraySpecificDeflaterBinarySerde() {
        testIteratorToArraySerde(AvroFormat.BINARY, CompressorType.DEFLATER);
    }

    @Test
    public void testIteratorToArraySpecificGZIPBinarySerde() {
        testIteratorToArraySerde(AvroFormat.BINARY, CompressorType.GZIP);
    }

    @Test
    public void testIteratorToArraySpecificJsonSerde() {
        testIteratorToArraySerde(AvroFormat.JSON, CompressorType.NONE);
    }

    @Test
    public void testIteratorToArraySpecificDeflaterJsonSerde() {
        testIteratorToArraySerde(AvroFormat.JSON, CompressorType.DEFLATER);
    }

    @Test
    public void testIteratorToArraySpecificGZIPJsonSerde() {
        testIteratorToArraySerde(AvroFormat.JSON, CompressorType.GZIP);
    }

}
