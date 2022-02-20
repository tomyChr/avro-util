package ch.ichristen.avroUtil.util;

import ch.ichristen.avroUtil.model.InnerClass;
import ch.ichristen.avroUtil.model.TestEnum;
import ch.ichristen.avroUtil.model.TestObjectChild;
import ch.ichristen.avroUtil.model.TestObjectParent;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestDataGenerator {

    public static TestObjectParent buildTestObject(int id) {
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

    public static ArrayList<TestObjectParent> buildTestObjects(int n) {
        final ArrayList<TestObjectParent> testObjects = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            testObjects.add(buildTestObject(i));
        }
        return testObjects;
    }

}
