package pers.xiaomuma.canal.databind.utils;


import pers.xiaomuma.canal.databind.EntryHandler;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


public class GenericUtils {

    private static Map<Class<? extends EntryHandler>, Class> cache = new ConcurrentHashMap<>();

    @SuppressWarnings("unchecked")
    public static <T> Class<T> getTableClass(EntryHandler<T> entryHandler) {
        Class<? extends EntryHandler> handlerClass = entryHandler.getClass();
        Class<T> tableClazz = cache.get(handlerClass);
        if (tableClazz == null) {
            Type[] interfacesTypes = handlerClass.getGenericInterfaces();
            for (Type type : interfacesTypes) {
                Class<T> clazz = (Class<T>) ((ParameterizedType) type).getRawType();
                if (clazz.equals(EntryHandler.class)) {
                    tableClazz = (Class<T>) ((ParameterizedType) type).getActualTypeArguments()[0];
                    cache.putIfAbsent(handlerClass, tableClazz);
                    return tableClazz;
                }
            }
        }
        return tableClazz;

    }
}
