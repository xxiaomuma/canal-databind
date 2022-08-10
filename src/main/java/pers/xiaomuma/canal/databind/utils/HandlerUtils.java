package pers.xiaomuma.canal.databind.utils;


import pers.xiaomuma.canal.databind.EntryHandler;
import pers.xiaomuma.canal.databind.IgnoreEntryHandler;
import pers.xiaomuma.canal.databind.annotation.CanalTableName;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


public class HandlerUtils {

    public static final EntryHandler<Map<String, String>> ignoreHandler = new IgnoreEntryHandler();

    public static <T> Map<String, EntryHandler<T>> getTableHandlerMap(List<? extends EntryHandler<T>> entryHandlers) {
        Map<String, EntryHandler<T>> handlerMap = new ConcurrentHashMap<>();
        if (entryHandlers != null && !entryHandlers.isEmpty()) {
            for (EntryHandler<T> handler : entryHandlers) {
                String canalTableName = getCanalTableName(handler);
                if (canalTableName != null) {
                    handlerMap.putIfAbsent(canalTableName, handler);
                }
            }
        }
        return handlerMap;
    }

    public static <T> String getCanalTableName(EntryHandler<T> entryHandler) {
        CanalTableName canalTable = entryHandler.getClass().getAnnotation(CanalTableName.class);
        if (canalTable != null) {
            return buildKey(canalTable.database(), canalTable.table());
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    public static <T> EntryHandler<T> getEntryHandler(Map<String, EntryHandler<T>> handlerMap, String database, String table) {
        String canalTableName = buildKey(database, table);
        EntryHandler<T> entryHandler = handlerMap.get(canalTableName);
        if (entryHandler == null) {
            return (EntryHandler<T>) ignoreHandler;
        }
        return entryHandler;
    }

    private static String buildKey(String database, String table) {
        String key = database + "_" + table;
        return key.toLowerCase();
    }

}
