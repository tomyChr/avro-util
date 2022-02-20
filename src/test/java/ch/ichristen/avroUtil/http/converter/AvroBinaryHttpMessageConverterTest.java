package ch.ichristen.avroUtil.http.converter;

import ch.ichristen.avroUtil.model.TestObjectParent;
import ch.ichristen.avroUtil.serde.*;
import ch.ichristen.avroUtil.serde.compress.CompressorType;
import ch.ichristen.avroUtil.util.HttpInputMessageImpl;
import ch.ichristen.avroUtil.util.HttpOutputMessageImpl;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpInputMessage;
import org.springframework.http.HttpOutputMessage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import static ch.ichristen.avroUtil.util.TestDataGenerator.buildTestObject;
import static ch.ichristen.avroUtil.util.TestDataGenerator.buildTestObjects;

@Slf4j
class AvroBinaryHttpMessageConverterTest {

    final AvroSerializerFactory serializerFactory = new AvroSerializerFactory();
    final AvroDeserializerFactory deserializerFactory = new AvroDeserializerFactory();

    AbstractAvroHttpMessageConverter<Object> avroHttpMessageConverter;

    @BeforeEach
    void setUp() {
        avroHttpMessageConverter = new AvroBinaryHttpMessageConverter<>(CompressorType.NONE);
    }

    @Test
    void supports_single_avro_object() {
        Assertions.assertTrue(avroHttpMessageConverter.supports(TestObjectParent.class));
    }

    @Test
    void supports_multiple_avro_objects() {
        ArrayList<TestObjectParent> arrayList = buildTestObjects(3);
        TestObjectParent[] testObjectParents = (TestObjectParent[]) arrayList.toArray(new TestObjectParent[arrayList.size()]);
        Assertions.assertTrue(avroHttpMessageConverter.supports(testObjectParents.getClass()));
    }

    @Test
    void readInternalSingleObject() {
        Serializer ser = serializerFactory.serializer(AvroFormat.BINARY, CompressorType.NONE);

        final TestObjectParent testObject = buildTestObject(0);
        byte[] body = null;
        try {
            body = ser.serialize(testObject);
            log.info("Result from serializer - Size: {} Binary Content: {}", body.length, body);
            log.debug("Result from serializer - Size: {} String Content: {}", body.length, new String(body));
        } catch (Exception e) {
            log.error("Exception during serialization of '" + testObject + "'", e);
        }

        TestObjectParent resultObject = null;
        try {
            HttpInputMessage inputMessage = new HttpInputMessageImpl(null, body);
            resultObject = (TestObjectParent)avroHttpMessageConverter.readInternal(TestObjectParent.class, inputMessage);
        } catch (IOException e) {
            log.error("Exception during deserialization of '" + testObject + "'", e);
        }
        Assertions.assertEquals(testObject, resultObject);
    }

    @Test
    void readInternalArrayObject() {
        Serializer ser = serializerFactory.serializer(AvroFormat.BINARY, CompressorType.NONE);

        final TestObjectParent[] testObjects = buildTestObjects(3).toArray(new TestObjectParent[3]);
        byte[] body = null;
        try {
            body = ser.serialize(testObjects);
            log.info("Result from serializer - Size: {} Binary Content: {}", body.length, body);
            log.debug("Result from serializer - Size: {} String Content: {}", body.length, new String(body));
        } catch (Exception e) {
            log.error("Exception during serialization of '" + testObjects + "'", e);
        }

        TestObjectParent[] resultObjects = new TestObjectParent[0];
        try {
            HttpInputMessage inputMessage = new HttpInputMessageImpl(null, body);
            resultObjects = (TestObjectParent[])avroHttpMessageConverter.readInternal(resultObjects.getClass(), inputMessage);
        } catch (IOException e) {
            log.error("Exception during deserialization of '" + testObjects + "'", e);
        }
        Assertions.assertTrue(Arrays.equals(testObjects, resultObjects));
    }

    @Test
    void writeInternalSingleObject() {

        final TestObjectParent testObject = buildTestObject(0);

        HttpOutputMessage outputMessage = new HttpOutputMessageImpl(null);
        try {
            avroHttpMessageConverter.writeInternal(testObject, outputMessage);
        } catch (IOException e) {
            log.error("Exception during deserialization of '" + testObject + "'", e);
        }

        TestObjectParent resultObject = null;
        try {
            byte[] body = ((HttpOutputMessageImpl)outputMessage).getByteArray();
            Deserializer desr = deserializerFactory.deserializer(AvroFormat.BINARY, CompressorType.NONE);
            resultObject = (TestObjectParent)desr.deserialize(TestObjectParent.class, body);
       } catch (Exception e) {
            log.error("Exception during serialization of '" + testObject + "'", e);
        }

        Assertions.assertEquals(testObject, resultObject);

    }
}