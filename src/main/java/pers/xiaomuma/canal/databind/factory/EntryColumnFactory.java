package pers.xiaomuma.canal.databind.factory;


import org.apache.commons.lang3.StringUtils;
import pers.xiaomuma.canal.databind.DefaultRowDataHandler;
import pers.xiaomuma.canal.databind.EntryHandler;
import pers.xiaomuma.canal.databind.exception.CanalBizException;
import pers.xiaomuma.canal.databind.utils.EntryUtils;
import pers.xiaomuma.canal.databind.utils.FieldUtils;
import pers.xiaomuma.canal.databind.utils.GenericUtils;
import pers.xiaomuma.canal.databind.utils.StringConvertUtils;
import java.util.Map;
import java.util.Set;


public class EntryColumnFactory implements IColumnFactory<Map<String, String>> {


    @Override
    @SuppressWarnings("unchecked")
    public <R> R newInstance(EntryHandler<R> handler, Map<String, String> target) throws CanalBizException {
        if (handler instanceof DefaultRowDataHandler) {
            return (R) target;
        }
        Class<R> tableClass = GenericUtils.getTableClass(handler);
        if (tableClass != null) {
            return newInstance(tableClass, target);
        }
        return null;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <R> R newInstance(EntryHandler<R> handler, Map<String, String> target, Set<String> updateColumn) throws CanalBizException {
        if (handler instanceof DefaultRowDataHandler) {
            return (R) target;
        }
        try {
            Class<R> tableClass = GenericUtils.getTableClass(handler);
            if (tableClass != null) {
                R object = tableClass.newInstance();
                Map<String, String> columnNames = EntryUtils.getFieldName(object.getClass());
                for (Map.Entry<String, String> entry : target.entrySet()) {
                    String name = StringConvertUtils.lineToHump(entry.getKey());
                    if (updateColumn.contains(name)) {
                        String fieldName = columnNames.get(StringConvertUtils.lineToHump(name));
                        if (StringUtils.isNotEmpty(fieldName)) {
                            FieldUtils.setFieldValue(object, fieldName, entry.getValue());
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
    public <R> R newInstance(Class<R> clazz, Map<String, String> target) throws CanalBizException {
        try {
            R object = clazz.newInstance();
            Map<String, String> columnNames = EntryUtils.getFieldName(object.getClass());
            for (Map.Entry<String, String> entry : target.entrySet()) {
                String name = StringConvertUtils.lineToHump(entry.getKey());
                String fieldName = columnNames.get(name);
                if (StringUtils.isNotEmpty(fieldName)) {
                    FieldUtils.setFieldValue(object, fieldName, entry.getValue());
                }
            }
            return object;
        } catch (Exception e) {
            throw new CanalBizException(e.getMessage());
        }
    }






    /*@SuppressWarnings("unchecked")
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
    }*/
}
