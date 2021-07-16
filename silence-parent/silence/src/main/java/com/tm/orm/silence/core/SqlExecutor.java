package com.tm.orm.silence.core;

import com.google.common.base.CaseFormat;
import com.tm.orm.silence.exception.SqlException;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import javax.sql.DataSource;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.sql.*;
import java.util.*;

/**
 * @author yudm
 * @date 2021/5/13 17:40
 * @desc 执行sql的类
 */
@Component
public class SqlExecutor {
    @Resource
    private DataSource dataSource;
    //存放参数列表
    private final ThreadLocal<List<Object>> valuesThreadLocal = new ThreadLocal<>();
    //sql语句构建器
    private final SqlBuilder sqlBuilder = new SqlBuilder(valuesThreadLocal);

    @PostConstruct
    public void init() {
        if (null == dataSource) {
            throw new SqlException("Datasource is not initialized");
        }
    }

    /**
     * @params [entity 实体对象, selective 是否过滤掉null, echoId 是否回显主键值]
     * @desc 插入单条数据
     */
    public <T> int insert(T entity, boolean selective, boolean echoId) {
        return doInsert(sqlBuilder.buildInsertSql(entity, selective), entity, echoId);
    }

    /**
     * @params [entities 实体对象列表, selective 是否过滤掉null, echoId 是否回显主键值]
     * @desc 批量插入
     */
    public <T> int insertList(List<T> entities, boolean selective, boolean echoId) {
        return doInsertList(sqlBuilder.buildInsertListSql(entities, selective), entities, echoId);
    }

    /**
     * @params [entity 实体对象, selective 是否过滤掉null]
     * @desc 根据主键更新
     */
    public int updateById(Object entity, boolean selective) {
        return doExecute(sqlBuilder.buildUpdateByIdSql(entity, selective));
    }

    /**
     * @params [entities 实体对象]
     * @desc 根据主键删除
     */
    public int deleteById(Object entity) {
        return doExecute(sqlBuilder.buildDeleteByIdSql(entity));
    }

    /**
     * @params [sql 简单增删改sql语句，可含有占位符，但不能含有动态语句, data 占位符对应的参数列表]
     * @desc 执行简单增删改
     */
    public int simpleExecute(String sql, Object... data) {
        valuesThreadLocal.set(Arrays.asList(data));
        return doExecute(sql);
    }

    /**
     * @params [sql 复杂增删改sql语句，含有动态语句, data 参数]
     * @desc 执行带有动态语句的复杂增删改
     */
    public int execute(String sql, Object data) {
        return doExecute(sqlBuilder.build(sql, data));
    }

    /**
     * @Param [clazz 实体类对应字节码, id 主键值]
     * @Desc 通过主键查询
     **/
    public <T> List<T> selectById(Class<T> clazz, Object id) {
        return doQuery(sqlBuilder.buildSelectByIdSql(clazz, id), clazz);
    }

    /**
     * @params [clazz 需要返回的对象类型, sql 简单查询sql语句，可含有占位符，但不能含有动态语句, data 占位符对应的参数列表]
     * @desc 执行简单查询
     */
    public <T> List<T> simpleQuery(Class<T> clazz, String sql, Object... data) {
        valuesThreadLocal.set(Arrays.asList(data));
        return doQuery(sql, clazz);
    }

    /**
     * @params [clazz 需要返回的对象字节码, page 分页对象, sql 简单查询sql语句, data 占位符对应的参数列表]
     * @desc 通过简单sql语句分页查询
     */
    public <T> Page<T> simplePage(Class<T> clazz, Page<T> page, String sql, Object... data) {
        if (page.isSearchTotal()) {
            Integer count = doSelectOne(simpleQuery(Integer.class, "select count(*) from (" + sql + ")", data));
            page.setTotal(null == count ? 0 : count);
        }
        page.setList(simpleQuery(clazz, sql + " limit " + (page.getPageNum() - 1) + "," + page.getPageSize(), data));
        return page;
    }

    /**
     * @params [clazz 需要返回的对象类型,sql 复杂查询sql语句，含有动态语句, data 参数]
     * @desc 执行带有动态语句的复杂增删改
     */
    public <T> List<T> query(Class<T> clazz, String sql, Object data) {
        return doQuery(sqlBuilder.build(sql, data), clazz);
    }

    /**
     * @params [clazz 需要返回的对象字节码, page 分页对象, sql 复杂查询sql语句，含有动态语句, data 占位符对应的参数列表]
     * @desc 通过带有动态语句的sql分页查询
     */
    public <T> Page<T> page(Class<T> clazz, Page<T> page, String sql, Object data) {
        if (page.isSearchTotal()) {
            Integer count = doSelectOne(query(Integer.class, "select count(*) from (" + sql + ")", data));
            page.setTotal(null == count ? 0 : count);
        }
        page.setList(query(clazz, sql + " limit " + (page.getPageNum() - 1) + "," + page.getPageSize(), data));
        return page;
    }

    /**
     * @params [sql sql语句, obj 参数, echoId 是否回显主键]
     * @desc 真正执行单条插入
     */
    private int doInsert(String sql, Object obj, boolean echoId) {
        ResultSet rs = null;
        PreparedStatement pst = null;
        try (Connection cn = getCn()) {
            //回显主键的值
            if (echoId) {
                pst = cn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
                fillPst(pst, valuesThreadLocal.get());
                int rows = pst.executeUpdate();
                rs = pst.getGeneratedKeys();
                echoId(rs, obj);
                return rows;
            }
            //不回显主键的值
            pst = cn.prepareStatement(sql);
            fillPst(pst, valuesThreadLocal.get());
            return pst.executeUpdate();
        } catch (Exception e) {
            throw new SqlException(e);
        } finally {
            releaseRs(pst, rs);
        }
    }

    /**
     * @params [sql sql语句, objs 参数列表, echoId 是否回显主键]
     * @desc 真正执行批量插入
     */
    private <T> int doInsertList(String sql, List<T> objs, boolean echoId) {
        ResultSet rs = null;
        PreparedStatement pst = null;
        try (Connection cn = getCn()) {
            if (echoId) {
                pst = cn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
                fillPstList(pst, valuesThreadLocal.get());
                int rows = pst.executeBatch().length;
                rs = pst.getGeneratedKeys();
                echoIdList(rs, objs);
                return rows;
            }
            pst = cn.prepareStatement(sql);
            fillPstList(pst, valuesThreadLocal.get());
            return pst.executeUpdate();
        } catch (Exception e) {
            throw new SqlException(e);
        } finally {
            releaseRs(pst, rs);
        }
    }

    /**
     * @params [sql sql语句]
     * @desc 真正执行增删改
     */
    private int doExecute(String sql) {
        try (Connection cn = getCn(); PreparedStatement pst = cn.prepareStatement(sql)) {
            fillPst(pst, valuesThreadLocal.get());
            return pst.executeUpdate();
        } catch (SQLException e) {
            throw new SqlException(e);
        } finally {
            valuesThreadLocal.remove();
        }
    }

    /**
     * @params [sql sql语句, clazz 需要返回的类型]
     * @desc 真正执行查询
     */
    private <T> List<T> doQuery(String sql, Class<T> clazz) {
        ResultSet rs = null;
        try (Connection cn = getCn(); PreparedStatement pst = cn.prepareStatement(sql)) {
            fillPst(pst, valuesThreadLocal.get());
            rs = pst.executeQuery();
            return mappingResultSet(rs, clazz);
        } catch (Exception e) {
            throw new SqlException(e);
        } finally {
            releaseRs(null, rs);
        }
    }

    /**
     * @params [list 查询结果]
     * @desc 查询一个的执行者
     */
    public <T> T doSelectOne(List<T> list) {
        if (list.size() == 0) {
            return null;
        } else if (list.size() == 1) {
            return list.get(0);
        } else {
            throw new SqlException("too many result");
        }
    }

    /**
     * @desc 从数据库连接池中获取链接，并由spring来管理事务
     */
    private Connection getCn() throws SQLException {
        Connection cn = DataSourceUtils.getConnection(dataSource);
        if (!DataSourceUtils.isConnectionTransactional(cn, dataSource)) {
            cn.setAutoCommit(true);
        }
        return cn;
    }

    /**
     * @params [pst PreparedStatement, values 参数列表]
     * @desc 向批量占位符中填充值
     */
    private void fillPstList(PreparedStatement pst, List<Object> values) throws SQLException {
        for (Object value : values) {
            fillPst(pst, (List<Object>) value);
            pst.addBatch();
        }
    }

    /**
     * @params [pst PreparedStatement, values 参数列表]
     * @desc 向占位符中填充值
     */
    private void fillPst(PreparedStatement pst, List<Object> values) throws SQLException {
        for (int i = 0; i < values.size(); ++i) {
            pst.setObject(i + 1, values.get(i));
        }
    }

    /**
     * @params [rs 结果集, entity 插入的实体对象]
     * @desc 将插入后的id回显到实体对象中
     */
    private void echoId(ResultSet rs, Object entity) throws Exception {
        Field idField = getIdField(entity.getClass());
        rs.next();
        boolean accessible = idField.isAccessible();
        idField.setAccessible(true);
        //返回的主键值只会有一个，即使表中是复合主键也只会返回第一个主键的值
        idField.set(entity, rs.getObject(1));
        idField.setAccessible(accessible);
    }

    /**
     * @params [rs 结果集, entities 批量插入的对象列表]
     * @desc 将插入后的id回显到实体对象列表中
     */
    private <T> void echoIdList(ResultSet rs, List<T> entities) throws Exception {
        Field idField = getIdField(entities.get(0).getClass());
        rs.next();
        for (int i = 0; rs.next(); ++i) {
            boolean accessible = idField.isAccessible();
            idField.setAccessible(true);
            //返回的主键值只会有一个，即使表中是复合主键也只会返回第一个主键的值
            idField.set(entities.get(i), rs.getObject(1));
            idField.setAccessible(accessible);
        }
    }

    /**
     * @params [clazz 实体对象的字节码]
     * @desc 获取主键对应的字段
     */
    private Field getIdField(Class<?> clazz) {
        Field[] fields = clazz.getDeclaredFields();
        Field idField = null;
        for (Field field : fields) {
            if (!Modifier.isStatic(field.getModifiers())) {
                idField = field;
                break;
            }
        }
        if (null == idField) {
            throw new SqlException("can not find the field of primary key ");
        }
        return idField;
    }

    /**
     * @params [rs 查询结果集, clazz 需要返回对象的字节码]
     * @desc 将结果集映射到对象中
     */
    private <T> List<T> mappingResultSet(ResultSet rs, Class<T> clazz) throws Exception {
        List<T> list = new ArrayList<>();
        if (null == rs) {
            return list;
        }
        Map<String, Field> fieldMap = getFieldMap(clazz);
        ResultSetMetaData md = rs.getMetaData();
        int count = md.getColumnCount();
        //存放属于子对象的数据
        Map<String, Object> anyChild = new HashMap<>();
        //跳过表头
        while (rs.next()) {
            //映射一行到一个对象中
            T t = clazz.newInstance();
            for (int i = 1; i <= count; ++i) {
                String name = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, md.getColumnName(i));
                Field field = fieldMap.get(name);
                if (null != field) {
                    setFieldValue(t, field, rs.getObject(i));
                } else if (name.contains("__")) {//属于子对象的数据
                    anyChild.put(name, rs.getObject(i));
                }
            }
            //映射子对象
            mappingChild(t, fieldMap, anyChild);
            list.add(t);
            anyChild.clear();
        }
        return list;
    }

    private void mappingChild(Object parent, Map<String, Field> parentFieldMap, Map<String, Object> anyChild) throws Exception {
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
            Map<String, Field> childFieldMap = getFieldMap(clazz);
            //开始映射子对象
            for (Map.Entry<String, Object> entry : childEntry.getValue().entrySet()) {
                String name = entry.getKey();
                Object value = entry.getValue();
                Field childField = childFieldMap.get(name);
                if (null != childField) {
                    setFieldValue(child, childField, value);
                } else if (entry.getKey().contains("__")) {//属于子对象的子对象的数据
                    childAnyMap.put(name, value);
                }
            }
            //递归映射子对象的子对象
            mappingChild(child, childFieldMap, childAnyMap);
            setFieldValue(parent, parentField, child);
            childAnyMap.clear();
        }
    }

    private void setFieldValue(Object obj, Field field, Object value) throws Exception {
        boolean flag = field.isAccessible();
        field.setAccessible(true);
        field.set(obj, value);
        field.setAccessible(flag);
    }

    /**
     * @params [clazz 需要返回对象的字节码]
     * @desc 一次性获取clazz的所有字段并转化成name->field的map
     */
    private <T> Map<String, Field> getFieldMap(Class<T> clazz) {
        Field[] fields = clazz.getDeclaredFields();
        Map<String, Field> fieldMap = new HashMap<>();
        for (Field field : fields) {
            fieldMap.put(field.getName(), field);
        }
        return fieldMap;
    }

    /**
     * @params [st, rs]
     * @desc 释放资源
     */
    private void releaseRs(Statement st, ResultSet rs) {
        valuesThreadLocal.remove();
        try {
            if (null != st) {
                st.close();
            }
            if (null != rs) {
                rs.close();
            }
        } catch (SQLException e) {
            throw new SqlException(e);
        }
    }

}
