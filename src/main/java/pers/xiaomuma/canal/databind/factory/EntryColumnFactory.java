package pers.xiaomuma.canal.databind.factory;


import com.alibaba.otter.canal.protocol.CanalEntry;
import org.apache.commons.lang3.StringUtils;
import pers.xiaomuma.canal.databind.DefaultRowDataHandler;
import pers.xiaomuma.canal.databind.EntryHandler;
import pers.xiaomuma.canal.databind.exception.CanalBizException;
import pers.xiaomuma.canal.databind.utils.*;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;


public class EntryColumnFactory implements IColumnFactory<List<CanalEntry.Column>> {


    @SuppressWarnings("unchecked")
    @Override
    public <R> R newInstance(EntryHandler<R> handler, List<CanalEntry.Column> columns) throws CanalBizException {
        if (handler instanceof DefaultRowDataHandler) {
            Map<String, String> map = columns.stream().collect(Collectors.toMap(CanalEntry.Column::getName, CanalEntry.Column::getValue));
            return (R) map;
        }
        Class<R> tableClass = GenericUtils.getTableClass(handler);
        if (tableClass != null) {
            return newInstance(tableClass, columns);
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <R> R newInstance(EntryHandler<R> handler, List<CanalEntry.Column> columns, Set<String> updateColumn) throws CanalBizException {
        if (handler instanceof DefaultRowDataHandler) {
            Map<String, String> map = columns.stream().collect(Collectors.toMap(CanalEntry.Column::getName, CanalEntry.Column::getValue));
            return (R) map;
        }
        try {
            Class<R> tableClass = GenericUtils.getTableClass(handler);
            if (tableClass != null) {
                R object = tableClass.newInstance();
                Map<String, String> columnNames = EntryUtils.getFieldName(object.getClass());
                for (CanalEntry.Column column : columns) {
                    String name = StringConvertUtils.humpToLine(column.getName());
                    if (updateColumn.contains(name)) {
                        String fieldName = columnNames.get(StringConvertUtils.lineToHump(name));
                        if (StringUtils.isNotEmpty(fieldName)) {
                            FieldUtils.setFieldValue(object, fieldName, column.getValue());
                        }
                    }
                }
                return object;
            }
            return null;
        } catch (Exception e) {
            throw new CanalBizException(e.getMessage());
        }
    }

    @Override
    public <R> R newInstance(Class<R> clazz, List<CanalEntry.Column> columns) throws CanalBizException {
        try {
            R object = clazz.newInstance();
            Map<String, String> columnNames = EntryUtils.getFieldName(object.getClass());
            for (CanalEntry.Column column : columns) {
                String name = StringConvertUtils.lineToHump(column.getName());
                String fieldName = columnNames.get(name);
                if (StringUtils.isNotEmpty(fieldName)) {
                    FieldUtils.setFieldValue(object, fieldName, column.getValue());
                }
            }
            return object;
        } catch (Exception e) {
            throw new CanalBizException(e.getMessage());
        }
    }
}
