package pers.xiaomuma.canal.databind;


import com.alibaba.otter.canal.protocol.CanalEntry;

@FunctionalInterface
public interface RowDataHandler<T> {


    <R> void handlerRowData(T rowData, EntryHandler<R> handler, CanalEntry.EventType eventType);
}
