package com.tm.orm.silence.util;

import com.tm.orm.silence.exception.SqlException;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.*;

/**
 * @author yudm
 * @date 2021/7/21 10:29
 * @desc 反射工具类
 */
public class ReflectUtil {
    /**
     * @params [obj 目标对象, field 字段, value 字段值]
     * @desc 给某个对象的某个字段设置值
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
     * @params [obj 目标对象, field 字段]
     * @desc 获取字段的值
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
     * @params [obj 入参, keys sql语句中的参数名]
     * @desc 将入参根据参数名转化成map
     **/
    public static Map<String, Object> toMap(Object obj, List<String> keys) {
        if (null == obj && !keys.isEmpty()) {
            throw new SqlException("param can not be null");
        } else if (keys.isEmpty()) {
            throw new SqlException("this sql does not require parameters,I suggest you use simple sql instead");
        }
        //获取真正的key
        List<String> realKeys = new ArrayList<>();
        keys.forEach(s -> realKeys.add(StringUtil.replaceAll(s, StringUtil.PARAM_LABEL, "")));
        //逐层解析一个对象成为map
        return doToMap(obj, realKeys);
    }

    /**
     * @params [param , key]
     * @desc 通过key从param中获取值，key可以是多级的
     **/
    public static Object getValueFromMap(Map<?, ?> param, String key) {
        //key只有一级
        if (!key.contains(".")) {
            Object obj = param.get(key);
            if (null == obj && !param.containsKey(key)) {
                throw new SqlException("there is no param named:" + key);
            }
            return obj;
        }
        //key有多级
        int firstIndex = key.indexOf(".");
        //获取一级key
        String head = key.substring(0, firstIndex);
        Object value = param.get(head);
        if (null == value && !param.containsKey(head)) {
            throw new SqlException("there is no param named:" + head);
        } else if (!(value instanceof Map<?, ?>)) {
            throw new SqlException("there is no param named:" + key);
        }
        return getValueFromMap((Map<?, ?>) value, key.substring(firstIndex + 1, key.length() - 1));
    }

    /**
     * @params [obj 入参, keys sql语句中的参数名]
     * @desc 执行将入参根据参数名转化成map
     **/
    private static Map<String, Object> doToMap(Object obj, List<String> keys) {
        Map<String, Object> param = new HashMap<>();
        //先解析第一层
        if (obj instanceof Map<?, ?>) {
            //强制转化会有安全风险，因此采用遍历的形式
            ((Map<?, ?>) obj).forEach((k, v) -> {
                String realKey;
                if (k instanceof String) {
                    realKey = (String) k;
                } else {
                    realKey = String.valueOf(k);
                }
                param.put(realKey, v);
            });
        } else {
            Field[] fields = obj.getClass().getDeclaredFields();
            for (Field field : fields) {
                param.put(field.getName(), getFieldValue(obj, field));
            }
        }
        //将key按照父级key聚合
        Map<String, List<String>> keyMap = new HashMap<>();
        for (String key : keys) {
            if (!key.contains(".")) {
                continue;
            }
            int firstIndex = key.indexOf(".");
            //子对象名
            String head = key.substring(0, firstIndex);
            //子对象中字段名
            String left = key.substring(firstIndex + 1, key.length() - 1);
            List<String> lefts = keyMap.get(head);
            if (null == lefts) {
                lefts = new ArrayList<>();
                lefts.add(left);
                keyMap.put(head, lefts);
            } else {
                lefts.add(left);
            }
        }
        //解析子对象为map
        keyMap.forEach((k, v) -> {
            Object child = param.get(k);
            if (null == child && !v.isEmpty()) {
                throw new SqlException("there is no param named " + v);
            } else if (null != child && !v.isEmpty()) {
                param.put(k, doToMap(child, v));
            }
        });
        return param;
    }

}
