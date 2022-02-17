package ch.ichristen.avroUtil.serde;

import ch.ichristen.avroUtil.serde.compress.CompressorType;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.*;

import ch.ichristen.avroUtil.model.TestObjectParent;
import ch.ichristen.avroUtil.model.TestObjectChild;
import ch.ichristen.avroUtil.model.TestEnum;
import ch.ichristen.avroUtil.model.InnerClass;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import org.slf4j.LoggerFactory;

@Slf4j
public class BinarySerdeTest {

    final AvroSerializerFactory<TestObjectParent> serializerFactory = new AvroSerializerFactory<>();
    final AvroDeserializerFactory<TestObjectParent> deserializerFactory = new AvroDeserializerFactory<>();

    @BeforeAll
    public static void initializeTests() {
        Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        root.setLevel(Level.DEBUG);
    }

    @Test
    public void testSingleSpecificBinarySerde() {

        Serializer<TestObjectParent> ser = serializerFactory.serializer(AvroFormat.BINARY, CompressorType.NONE);

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
        Deserializer<TestObjectParent> deser = deserializerFactory.deserializer(AvroFormat.BINARY, CompressorType.NONE);
        if (result != null) {
            try {
                resultObject = deser.deserialize(TestObjectParent.class, result);
                log.info("Result from deserializer {}", resultObject);
            } catch (Exception e) {
                log.error("Exception during deserialization of '" + result + "'", e);
            }
        }

        Assertions.assertEquals(testObject, resultObject);
    }

    @Test
    public void testSingleSpecificDeflaterBinarySerde() {

        Serializer<TestObjectParent> ser = serializerFactory.serializer(AvroFormat.BINARY, CompressorType.DEFLATER);

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
        Deserializer<TestObjectParent> deser = deserializerFactory.deserializer(AvroFormat.BINARY, CompressorType.DEFLATER);
        if (result != null) {
            try {
                resultObject = deser.deserialize(TestObjectParent.class, result);
                log.info("Result from deserializer {}", resultObject);
            } catch (Exception e) {
                log.error("Exception during deserialization of '" + result + "'", e);
            }
        }

        Assertions.assertEquals(testObject, resultObject);
    }

    @Test
    public void testSingleSpecificGZIPBinarySerde() {

        Serializer<TestObjectParent> ser = serializerFactory.serializer(AvroFormat.BINARY, CompressorType.GZIP);

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
        Deserializer<TestObjectParent> deser = deserializerFactory.deserializer(AvroFormat.BINARY, CompressorType.GZIP);
        if (result != null) {
            try {
                resultObject = deser.deserialize(TestObjectParent.class, result);
                log.info("Result from deserializer {}", resultObject);
            } catch (Exception e) {
                log.error("Exception during deserialization of '" + result + "'", e);
            }
        }

        Assertions.assertEquals(testObject, resultObject);
    }

    @Test
    public void testCollectionSpecificBinarySerde() {

        Serializer<TestObjectParent> ser = serializerFactory.serializer(AvroFormat.BINARY, CompressorType.NONE);

        final ArrayList<TestObjectParent> testObjects = new ArrayList<>();
        testObjects.add(buildTestObject(0));
        testObjects.add(buildTestObject(1));
        testObjects.add(buildTestObject(2));

        byte[] result = null;
        try {
            result = ser.serialize(testObjects.iterator());
            log.info("Result from serializer - Size: {} Binary Content: {}", result.length, result);
            log.debug("Result from serializer - Size: {} String Content: {}", result.length, new String(result));
        } catch (Exception e) {
            log.error("Exception during serialization of '" + testObjects + "'", e);
        }

        Collection<TestObjectParent> resultObjects = null;
        Deserializer<TestObjectParent> deser = deserializerFactory.deserializer(AvroFormat.BINARY, CompressorType.NONE);
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
    public void testCollectionSpecificDeflaterBinarySerde() {

        Serializer<TestObjectParent> ser = serializerFactory.serializer(AvroFormat.BINARY, CompressorType.DEFLATER);

        final ArrayList<TestObjectParent> testObjects = new ArrayList<>();
        testObjects.add(buildTestObject(0));
        testObjects.add(buildTestObject(1));
        testObjects.add(buildTestObject(2));

        byte[] result = null;
        try {
            result = ser.serialize(testObjects.iterator());
            log.info("Result from serializer - Size: {} Binary Content: {}", result.length, result);
            log.debug("Result from serializer - Size: {} String Content: {}", result.length, new String(result));
        } catch (Exception e) {
            log.error("Exception during serialization of '" + testObjects + "'", e);
        }

        Collection<TestObjectParent> resultObjects = null;
        Deserializer<TestObjectParent> deser = deserializerFactory.deserializer(AvroFormat.BINARY, CompressorType.DEFLATER);
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
    public void testCollectionSpecificGZIPBinarySerde() {

        Serializer<TestObjectParent> ser = serializerFactory.serializer(AvroFormat.BINARY, CompressorType.GZIP);

        final ArrayList<TestObjectParent> testObjects = new ArrayList<>();
        testObjects.add(buildTestObject(0));
        testObjects.add(buildTestObject(1));
        testObjects.add(buildTestObject(2));

        byte[] result = null;
        try {
            result = ser.serialize(testObjects.iterator());
            log.info("Result from serializer - Size: {} Binary Content: {}", result.length, result);
            log.debug("Result from serializer - Size: {} String Content: {}", result.length, new String(result));
        } catch (Exception e) {
            log.error("Exception during serialization of '" + testObjects + "'", e);
        }

        Collection<TestObjectParent> resultObjects = null;
        Deserializer<TestObjectParent> deser = deserializerFactory.deserializer(AvroFormat.BINARY, CompressorType.GZIP);
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

    public TestObjectParent buildTestObject(int id) {
        Map<String,Float> testMap = new HashMap<>();
        testMap.putIfAbsent("test1-" + id, Float.valueOf("0"));
        testMap.putIfAbsent("test2-" + id, Float.valueOf("99.9"));
        List<InnerClass> testList = new ArrayList<>();
        testList.add(InnerClass.newBuilder().setTestName("Name 1 Inner Class").setTestId(77771+id).build());
        testList.add(InnerClass.newBuilder().setTestName("Name 2 Inner Class").setTestId(77772+id).build());
        testList.add(InnerClass.newBuilder().setTestName("Name 3 Inner Class").setTestId(77773+id).build());
        return TestObjectParent.newBuilder()
                .setTestPrimitives(TestObjectChild.newBuilder()
                        .setTestBoolean(true)
                        .setTestBytes(ByteBuffer.wrap("Test Byte Buffer".getBytes()))
                        .setTestDouble(Double.valueOf("99.9"))
                        .setTestFloat(Float.valueOf("99.991"))
                        .setTestLong(Long.valueOf("99999999"))
                        .setTestInt(Integer.valueOf("8889"))
                        .setTestString("Test this string")
                        .build())
                .setTestString("Outer Test String :"+id)
                .setTestEnum((id == 0) ? TestEnum.NO : (id == 1) ? TestEnum.YES : TestEnum.NONE)
                .setTestMap(testMap)
                .setTestList(testList)
                .build();

    }
}
