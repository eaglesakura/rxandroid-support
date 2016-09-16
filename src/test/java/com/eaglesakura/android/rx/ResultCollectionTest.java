package com.eaglesakura.android.rx;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ResultCollectionTest {

    void stringMethod(String value) {
        assertEquals(value, "test");
    }

    void intMethod(Integer value) {
        assertEquals(value, Integer.valueOf(123));
    }

    void arrayMethod(List value) {
        assertNotNull(value);
    }

    @Test
    public void 特定型でputとgetができる() {
        ResultCollection collection = new ResultCollection().put("test").put(123);

        intMethod(collection.get(Integer.class));
        stringMethod(collection.get(String.class));
    }

    @Test
    public void 自動の型マッチが行える() {
        ResultCollection collection = new ResultCollection().put(new ArrayList<>()).put("test");

        arrayMethod(collection.as(ArrayList.class));
        stringMethod(collection.as(String.class));
    }
}