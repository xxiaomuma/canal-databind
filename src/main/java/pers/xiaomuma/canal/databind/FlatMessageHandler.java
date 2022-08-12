package pers.xiaomuma.canal.databind;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.FlatMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pers.xiaomuma.canal.databind.utils.HandlerUtils;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FlatMessageHandler implements MessageHandler<FlatMessage> {

    private final Logger logger = LoggerFactory.getLogger(FlatMessageHandler.class);
    private Map<String, EntryHandler> entryHandlerMap;
    private RowDataHandler<List<Map<String, String>>> rowDataHandler;

    public FlatMessageHandler(RowDataHandler<List<Map<String, String>>> rowDataHandler,
                              List<? extends EntryHandler> entryHandlers) {
        this.rowDataHandler = rowDataHandler;
        this.entryHandlerMap = HandlerUtils.getTableHandlerMap(entryHandlers);
    }

    @Override
    public void handleMessage(FlatMessage message) {
        CanalEntry.EventType eventType = CanalEntry.EventType.valueOf(message.getType());
        String table = message.getTable();
        String database = message.getDatabase();
        List<Map<String, String>> data = message.getData();

        switch (eventType) {
            case INSERT:
            case DELETE:
                this.handleMessage(data, eventType, database, table);
                break;
            case UPDATE:
                List<Map<String, String>> old = message.getOld();
                this.handleMessage(data, old, eventType, database, table);
                break;
            default:
                logger.info("未支持事件处理:message:{}", message);
        }
    }

    private void handleMessage(List<Map<String, String>> data, CanalEntry.EventType eventType, String database, String table) {
        if (data == null || data.isEmpty()) {
            return;
        }
        for (Map<String, String> currentData : data) {
            List<Map<String, String>> values = Stream.of(currentData).collect(Collectors.toList());
            EntryHandler<?> entryHandler = HandlerUtils.getEntryHandler(entryHandlerMap, database, table);
            rowDataHandler.handlerRowData(values, entryHandler, eventType);
        }
    }

    private void handleMessage(List<Map<String, String>> data, List<Map<String, String>> old, CanalEntry.EventType eventType, String database, String table) {
        if (data == null || data.isEmpty()) {
            return;
        }
        for (int i = 0; i < data.size(); i++) {
            Map<String, String> oldMap = old.get(i);
            Map<String, String> dataMap = data.get(i);
            List<Map<String, String>> values = Stream.of(dataMap, oldMap).collect(Collectors.toList());
            EntryHandler<?> entryHandler = HandlerUtils.getEntryHandler(entryHandlerMap, database, table);
            rowDataHandler.handlerRowData(values, entryHandler, eventType);
        }
    }
}
