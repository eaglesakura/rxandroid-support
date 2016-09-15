package com.eaglesakura.android.rx;

import java.util.HashMap;
import java.util.Map;

/**
 * 複数アイテムを返却するためのUtil
 *
 */
public class ResultCollection {
    Map<Object, Object> mObjects = new HashMap<>();

    public ResultCollection put(Object key, Object value) {
        synchronized (mObjects) {
            if (mObjects.containsKey(key)) {
                throw new Error();
            }
            mObjects.put(key, value);
        }
        return this;
    }

    public <T> T get(Object key) {
        synchronized (mObjects) {
            if (mObjects.containsKey(key)) {
                return (T) mObjects.get(key);
            }
            throw new Error();
        }
    }
}
