package pers.xiaomuma.canal.databind.utils;

import java.lang.reflect.Field;

public class FieldUtils {

    public static void setFieldValue(Object object, String fieldName, String value) throws NoSuchFieldException, IllegalAccessException {
        Field field;
        try{
            field = object.getClass().getDeclaredField(fieldName);
        }catch (NoSuchFieldException e){
            field = object.getClass().getSuperclass().getDeclaredField(fieldName);
        }
        field.setAccessible(true);
        Class<?> type = field.getType();
        Object result = StringConvertUtils.convertType(type, value);
        field.set(object,result);
    }
}
