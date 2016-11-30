package com.facebook.presto.utils;

import java.util.Arrays;

/**
 * asserts.
 *
 * @author zhaoshb
 * @since 1.0
 */
public class Objects {

    public static int hash(Object... values) {
        return Arrays.hashCode(values);
    }

    public static <T> T requireNonNull(T obj) {
        if (obj == null)
            throw new NullPointerException();
        return obj;
    }

    public static <T> T requireNonNull(T obj, String message) {
        if (obj == null)
            throw new NullPointerException(message);
        return obj;
    }

    public static boolean equals(Object a, Object b) {
        return (a == b) || (a != null && a.equals(b));
    }
}
