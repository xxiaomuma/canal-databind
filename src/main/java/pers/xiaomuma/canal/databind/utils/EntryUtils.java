package pers.xiaomuma.canal.databind.utils;

import org.apache.commons.lang3.reflect.FieldUtils;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class EntryUtils {

    private static Map<Class<?>, Map<String, String>> cache = new ConcurrentHashMap<>();

    public static Map<String, String> getFieldName(Class<?> clazz) {
        Map<String, String> cacheMap = cache.get(clazz);
        if (cacheMap != null) {
            return cacheMap;
        }
        List<Field> fields = FieldUtils.getAllFieldsList(clazz);
        cacheMap = fields.stream().filter(field -> !Modifier.isStatic(field.getModifiers()))
                .collect(Collectors.toMap(Field::getName, Field::getName));
        cache.putIfAbsent(clazz, cacheMap);
        return cacheMap;
    }
}
