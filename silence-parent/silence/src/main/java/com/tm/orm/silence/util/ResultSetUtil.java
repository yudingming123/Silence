package com.tm.orm.silence.util;

import com.tm.orm.silence.exception.SqlException;

import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author yudm
 * @date 2021/7/21 10:36
 * @desc 结果集工具类
 */
public class ResultSetUtil {
    /**
     * @params [rs 查询结果集, clazz 需要返回对象的字节码]
     * @desc 映射结果集到一个对象，如果结果集中有多行数据则抛出异常
     **/
    public static <T> T mappingOne(ResultSet rs, Class<T> clazz) throws Exception {
        if (null == rs) {
            return null;
        }
        T t = null;
        //跳过表头
        if (rs.next()) {
            //存放属于子对象的数据
            Map<String, Object> anyChild = new HashMap<>();
            Map<String, Field> fieldMap = ReflectUtil.getFieldMap(clazz);
            //结果集中的元数据
            ResultSetMetaData md = rs.getMetaData();
            t = mappingLine(rs, md, fieldMap, anyChild, clazz);
        }
        if (rs.next()) {
            throw new SqlException("too many result");
        }
        return t;
    }

    /**
     * @params [rs 查询结果集, clazz 需要返回对象的字节码]
     * @desc 映射结果集到对象列表
     */
    public static <T> List<T> mappingAll(ResultSet rs, Class<T> clazz) throws Exception {
        List<T> list = new ArrayList<>();
        if (null == rs) {
            return list;
        }
        Map<String, Field> fieldMap = ReflectUtil.getFieldMap(clazz);
        //结果集中的元数据
        ResultSetMetaData md = rs.getMetaData();
        //存放属于子对象的数据
        Map<String, Object> anyChild = new HashMap<>();
        //跳过表头
        while (rs.next()) {
            //映射一行到一个对象中
            list.add(mappingLine(rs, md, fieldMap, anyChild, clazz));
            anyChild.clear();
        }
        return list;
    }

    /**
     * @params [rs 结果集, md 结果集元数据, fieldMap 字段集, anyChild 存放属于子对象的数据, clazz 需要返回对象的类型]
     * @desc 解析映射一行数据到一个对象中
     **/
    public static <T> T mappingLine(ResultSet rs, ResultSetMetaData md, Map<String, Field> fieldMap, Map<String, Object> anyChild, Class<T> clazz) throws Exception {
        T t = clazz.newInstance();
        for (int i = 1; i <= md.getColumnCount(); ++i) {
            String name = StringUtil.toCamel(md.getColumnName(i));
            Field field = fieldMap.get(name);
            if (null != field) {
                ReflectUtil.setFieldValue(t, field, rs.getObject(i));
            } else if (name.contains("__")) {//属于子对象的数据
                anyChild.put(name, rs.getObject(i));
            }
        }
        //映射子对象
        mappingChild(t, fieldMap, anyChild);
        return t;
    }

    /**
     * @params [parent 父对象, parentFieldMap 父对象的字段集, anyChild 存放属于子对象的数据]
     * @desc 递归映射子对象
     **/
    public static void mappingChild(Object parent, Map<String, Field> parentFieldMap, Map<String, Object> anyChild) throws Exception {
        //存放 子对象名->(子对象中字段名->值)
        Map<String, Map<String, Object>> childMap = new HashMap<>();
        anyChild.forEach((k, v) -> {
            int firstIndex = k.indexOf("__");
            //子对象名
            String head = k.substring(0, firstIndex);
            //子对象中字段名
            String name = k.substring(firstIndex + 2, k.length() - 1);
            if (childMap.containsKey(head)) {
                childMap.get(head).put(name, v);
            } else {
                Map<String, Object> map = new HashMap<>();
                map.put(name, v);
                childMap.put(head, map);
            }
        });
        //逐个映射所有的子对象
        Map<String, Object> childAnyMap = new HashMap<>();
        for (Map.Entry<String, Map<String, Object>> childEntry : childMap.entrySet()) {
            Field parentField = parentFieldMap.get(childEntry.getKey());
            Class<?> clazz = parentField.getType();
            Object child = clazz.newInstance();
            Map<String, Field> childFieldMap = ReflectUtil.getFieldMap(clazz);
            //开始映射子对象
            for (Map.Entry<String, Object> entry : childEntry.getValue().entrySet()) {
                String name = entry.getKey();
                Object value = entry.getValue();
                Field childField = childFieldMap.get(name);
                if (null != childField) {
                    ReflectUtil.setFieldValue(child, childField, value);
                } else if (entry.getKey().contains("__")) {//属于子对象的子对象的数据
                    childAnyMap.put(name, value);
                }
            }
            //递归映射子对象的子对象
            mappingChild(child, childFieldMap, childAnyMap);
            ReflectUtil.setFieldValue(parent, parentField, child);
            childAnyMap.clear();
        }
    }

    /**
     * @params [rs 结果集, entity 插入的实体对象]
     * @desc 将插入后的id回显到实体对象中
     */
    public static void echoId(ResultSet rs, Object entity) throws Exception {
        if (rs.next()) {
            //返回的主键值只会有一个，即使表中是复合主键也只会返回第一个主键的值
            ReflectUtil.setFieldValue(entity, ReflectUtil.getIdField(entity.getClass()), rs.getObject(1));
        }
    }

    /**
     * @params [rs 结果集, entities 批量插入的对象列表]
     * @desc 将插入后的id回显到实体对象列表中
     */
    public static void echoIdList(ResultSet rs, List<Object> entities) throws Exception {
        Field idField = ReflectUtil.getIdField(entities.get(0).getClass());
        for (int i = 0; i < entities.size() && rs.next(); ++i) {
            //返回的主键值只会有一个，即使表中是复合主键也只会返回第一个主键的值
            ReflectUtil.setFieldValue(entities.get(i), idField, rs.getObject(1));
        }
    }

    /**
     * @params [st, rs]
     * @desc 释放资源
     */
    public static void releaseRs(ResultSet rs) {
        try {
            if (null != rs) {
                rs.close();
            }
        } catch (SQLException e) {
            throw new SqlException(e);
        }
    }
}
