package pers.xiaomuma.canal.databind.utils;


import pers.xiaomuma.canal.databind.EntryHandler;
import pers.xiaomuma.canal.databind.IgnoreEntryHandler;
import pers.xiaomuma.canal.databind.annotation.CanalTableName;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


public class HandlerUtils {

    public static final EntryHandler<Map<String, String>> ignoreHandler = new IgnoreEntryHandler();

    @SuppressWarnings("unchecked")
    public static Map<String, EntryHandler> getTableHandlerMap(List<? extends EntryHandler> entryHandlers) {
        Map<String, EntryHandler> handlerMap = new ConcurrentHashMap<>();
        if (entryHandlers != null && !entryHandlers.isEmpty()) {
            for (EntryHandler handler : entryHandlers) {
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

    public static EntryHandler getEntryHandler(Map<String, EntryHandler> handlerMap, String database, String table) {
        String canalTableName = buildKey(database, table);
        EntryHandler entryHandler = handlerMap.get(canalTableName);
        if (entryHandler == null) {
            return ignoreHandler;
        }
        return entryHandler;
    }

    private static String buildKey(String database, String table) {
        String key = database + "_" + table;
        return key.toLowerCase();
    }

}
