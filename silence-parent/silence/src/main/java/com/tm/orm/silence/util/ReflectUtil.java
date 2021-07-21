package com.tm.orm.silence.util;


import com.tm.orm.silence.exception.SqlException;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author yudm
 * @Date 2021/7/21 10:29
 * @Desc 反射工具类
 */
public class ReflectUtil {
    /**
     * @Param [obj 目标对象, field 字段, value 字段值]
     * @Desc 给某个对象的某个字段设置值
     **/
    public static void setFieldValue(Object obj, Field field, Object value) {
        boolean flag = field.isAccessible();
        field.setAccessible(true);
        try {
            field.set(obj, value);
        } catch (IllegalAccessException e) {
            throw new SqlException(e);
        } finally {
            field.setAccessible(flag);
        }
    }

    /**
     * @Param [obj 目标对象, field 字段]
     * @Desc 获取字段的值
     **/
    public static Object getFieldValue(Object obj, Field field) {
        boolean accessible = field.isAccessible();
        field.setAccessible(true);
        try {
            return field.get(obj);
        } catch (IllegalAccessException e) {
            throw new SqlException(e);
        } finally {
            field.setAccessible(accessible);
        }
    }

    /**
     * @params [clazz 需要返回对象的字节码]
     * @desc 一次性获取clazz的所有字段并转化成name->field的map
     */
    public static Map<String, Field> getFieldMap(Class<?> clazz) {
        Field[] fields = clazz.getDeclaredFields();
        Map<String, Field> fieldMap = new HashMap<>();
        for (Field field : fields) {
            fieldMap.put(field.getName(), field);
        }
        return fieldMap;
    }

    /**
     * @params [clazz 实体对象的字节码]
     * @desc 获取主键对应的字段
     */
    public static Field getIdField(Class<?> clazz) {
        Field[] fields = clazz.getDeclaredFields();
        Field idField = null;
        for (Field field : fields) {
            if (!Modifier.isStatic(field.getModifiers())) {
                idField = field;
                break;
            }
        }
        if (null == idField) {
            throw new SqlException("can not find the field of primary key");
        }
        return idField;
    }

    /**
     * @params [fields 字段列表]
     * @desc 获取字段名
     */
    public static List<String> getFieldNames(List<Field> fields) {
        List<String> names = new ArrayList<>();
        for (Field field : fields) {
            names.add(field.getName());
        }
        return names;
    }

    /**
     * @params [data 入参对象,fields 字段列表]
     * @desc 获取字段的值
     */
    public static List<Object> getValues(Object entity, List<Field> fields) {
        if (null == entity) {
            throw new SqlException("entity can not be null");
        }
        List<Object> columns = new ArrayList<>();
        for (Field field : fields) {
            columns.add(getFieldValue(entity, field));
        }
        return columns;
    }

    /**
     * @params [obj 入参]
     * @desc 将入参转化成map
     */
    @SuppressWarnings("unchecked")
    public static Map<String, Object> toMap(Object obj) {
        Map<String, Object> param = new HashMap<>();
        if (null == obj) {
            return param;
        }
        if (obj instanceof Map) {
            return (Map<String, Object>) obj;
        }

        Field[] fields = obj.getClass().getDeclaredFields();
        for (Field field : fields) {
            param.put(field.getName(), getFieldValue(obj, field));
        }
        return param;
    }
}
