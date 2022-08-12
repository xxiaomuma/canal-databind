package pers.xiaomuma.canal.databind;

import com.alibaba.otter.canal.protocol.CanalEntry;
import pers.xiaomuma.canal.databind.factory.IColumnFactory;
import java.util.List;
import java.util.Map;

public class DefaultRowDataHandler implements RowDataHandler<List<Map<String, String>>>  {

    private IColumnFactory<Map<String, String>> columnFactory;

    public DefaultRowDataHandler(IColumnFactory<Map<String, String>> columnFactory) {
        this.columnFactory = columnFactory;
    }

    @Override
    public <R> void handlerRowData(List<Map<String, String>> list, EntryHandler<R> handler, CanalEntry.EventType eventType) {
        switch (eventType) {
            case INSERT:
                R entry = columnFactory.newInstance(handler, list.iterator().next());
                handler.insert(entry);
                break;
            case UPDATE:
                R before = columnFactory.newInstance(handler, list.get(1));
                R after = columnFactory.newInstance(handler, list.get(0));
                handler.update(before, after);
                break;
            case DELETE:
                R o = columnFactory.newInstance(handler, list.iterator().next());
                handler.delete(o);
                break;
            default:
                break;
        }
    }
}
