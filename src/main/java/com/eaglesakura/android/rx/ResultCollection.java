package com.eaglesakura.android.rx;

import android.support.annotation.NonNull;

import java.util.HashMap;
import java.util.Map;

/**
 * 複数アイテムを返却するためのUtil
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

    /**
     * valueの型(.class)をキーにして集合に追加する。
     * 返却がすべて異なる型である場合のみ有効
     */
    public ResultCollection put(@NonNull Object value) {
        return put(value.getClass(), value);
    }

    public <T> T get(Object key) {
        synchronized (mObjects) {
            if (mObjects.containsKey(key)) {
                return (T) mObjects.get(key);
            }
            throw new Error();
        }
    }

    /**
     * 自動で結果を検索して返す。
     * これはputされている値がすべて違う型である場合のみ正しく動作する。
     */
    public <T> T as(Class<T> clazz) {
        synchronized (mObjects) {
            for (Object value : mObjects.values()) {
                if (value == null) {
                    continue;
                }


                try {
                    if (value.getClass().asSubclass(clazz) != null) {
                        return (T) value;
                    }
                } catch (ClassCastException e) {

                }
            }

            throw new Error();
        }
    }
}
