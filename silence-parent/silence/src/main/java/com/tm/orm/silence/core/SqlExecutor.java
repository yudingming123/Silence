package com.tm.orm.silence.core;

import com.google.common.base.CaseFormat;
import com.tm.orm.silence.exception.SqlException;
import com.tm.orm.silence.function.BiThrowConsumer;
import com.tm.orm.silence.function.ThrowFunction;
import com.tm.orm.silence.function.ThrowConsumer;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import javax.sql.DataSource;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.sql.*;
import java.util.*;
import java.util.function.BiFunction;

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
     * @params [entity 实体对象]
     * @desc 插入单条数据，null会被过滤掉
     */
    public <T> int insert(T entity) {
        return doUpdate(sqlBuilder.buildInsertSql(entity), this::fillPst);
    }

    /**
     * @params [entity 实体对象]
     * @desc 插入单条数据，null会被过滤掉，并且回显主键
     */
    public <T> int insertAndEchoId(T entity) {
        return doUpdateAndEchoId(sqlBuilder.buildInsertSql(entity), this::fillPst, r -> echoId(r, entity));
    }

    /**
     * @params [entities 实体对象列表]
     * @desc 批量插入，null会被过滤掉
     */
    public <T> int insertList(List<T> entities) {
        return doUpdate(sqlBuilder.buildInsertListSql(entities), this::fillPstList);
    }

    /**
     * @params [entities 实体对象列表]
     * @desc 批量插入，null会被过滤掉，并且回显主键
     */
    public <T> int insertListAndEchoId(List<T> entities) {
        return doUpdateAndEchoId(sqlBuilder.buildInsertListSql(entities), this::fillPstList, r -> echoIdList(r, entities));
    }

    /**
     * @params [entity 实体对象, selective 是否过滤掉null]
     * @desc 根据主键更新
     */
    public int updateById(Object entity) { return doUpdate(sqlBuilder.buildUpdateByIdSql(entity), this::fillPst);}

    /**
     * @params [entities 实体对象]
     * @desc 根据主键删除
     */
    public int deleteById(Object entity) {
        return doUpdate(sqlBuilder.buildDeleteByIdSql(entity), this::fillPst);
    }

    /**
     * @params [sql 简单增删改sql语句，可含有占位符，但不能含有动态语句, data 占位符对应的参数列表]
     * @desc 执行简单增删改
     */
    public int simpleUpdate(String sql, Object... data) {
        valuesThreadLocal.set(Arrays.asList(data));
        return doUpdate(sql, this::fillPst);
    }

    /**
     * @params [sql 复杂增删改sql语句，含有动态语句, data 参数]
     * @desc 执行带有动态语句的复杂增删改
     */
    public int update(String sql, Object data) {
        return doUpdate(sqlBuilder.build(sql, data), this::fillPst);
    }

    /**
     * @Param [clazz 实体类对应字节码, id 主键值]
     * @Desc 通过主键查询
     **/
    public <T> T selectById(Class<T> clazz, Object id) {
        return doQuery(sqlBuilder.buildSelectByIdSql(clazz, id), r -> mappingOne(r, clazz));
    }

    /**
     * @params [clazz 需要返回的对象类型, sql 简单查询sql语句，可含有占位符，但不能含有动态语句, data 占位符对应的参数列表]
     * @desc 执行简单查询
     */
    public <T> T simpleQueryOne(Class<T> clazz, String sql, Object... data) {
        valuesThreadLocal.set(Arrays.asList(data));
        return doQuery(sql, r -> mappingOne(r, clazz));
    }

    /**
     * @params [clazz 需要返回的对象类型, sql 简单查询sql语句，可含有占位符，但不能含有动态语句, data 占位符对应的参数列表]
     * @desc 执行简单查询
     */
    public <T> List<T> simpleQueryList(Class<T> clazz, String sql, Object... data) {
        valuesThreadLocal.set(Arrays.asList(data));
        return doQuery(sql, r -> mappingAll(r, clazz));
    }

    /**
     * @params [clazz 需要返回的对象类型,sql 复杂查询sql语句，含有动态语句, data 参数]
     * @desc 通过动态语句查询一个
     */
    public <T> T queryOne(Class<T> clazz, String sql, Object data) {
        return doQuery(sqlBuilder.build(sql, data), r -> mappingOne(r, clazz));
    }

    /**
     * @params [clazz 需要返回的对象类型,sql 复杂查询sql语句，含有动态语句, data 参数]
     * @desc 通过动态语句查询多个
     */
    public <T> List<T> queryList(Class<T> clazz, String sql, Object data) {
        return doQuery(sqlBuilder.build(sql, data), r -> mappingAll(r, clazz));
    }

    /**
     * @params [clazz 需要返回的对象字节码, page 分页对象, sql 简单查询sql语句, data 占位符对应的参数列表]
     * @desc 通过简单sql语句分页查询
     */
    public <T> Page<T> simplePage(Class<T> clazz, Page<T> page, String sql, Object... data) {
        return doPage(clazz, page, sql, (c, s) -> simpleQueryOne(c, s, data), (c, s) -> simpleQueryList(c, s, data));
    }

    /**
     * @params [clazz 需要返回的对象字节码, page 分页对象, sql 复杂查询sql语句，含有动态语句, data 占位符对应的参数列表]
     * @desc 通过带有动态语句的sql分页查询
     */
    public <T> Page<T> page(Class<T> clazz, Page<T> page, String sql, Object data) {
        return doPage(clazz, page, sql, (c, s) -> queryOne(c, s, data), (c, s) -> queryList(c, s, data));
    }

    /**
     * @Param [sql sql语句, fillPstConsumer 填充占位符的函数]
     * @Desc 执行增删改
     **/
    private int doUpdate(String sql, BiThrowConsumer<PreparedStatement, List<Object>> fillPstConsumer) {
        try (Connection con = DataSourceUtils.getConnection(dataSource); PreparedStatement pst = con.prepareStatement(sql)) {
            //填充占位符
            fillPstConsumer.accept(pst, valuesThreadLocal.get());
            return pst.executeUpdate();
        } catch (SQLException e) {
            throw new SqlException(e);
        } finally {
            valuesThreadLocal.remove();
        }
    }

    /**
     * @Param [sql sql语句, fillPstConsumer 填充占位符的函数, echoIdConsumer 回显主键值的函数]
     * @Desc 执行增删改，并回显主键值
     **/
    private int doUpdateAndEchoId(String sql, BiThrowConsumer<PreparedStatement, List<Object>> fillPstConsumer, ThrowConsumer<ResultSet> echoIdConsumer) {
        ResultSet rs = null;
        try (Connection con = DataSourceUtils.getConnection(dataSource); PreparedStatement pst = con.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
            //填充占位符
            fillPstConsumer.accept(pst, valuesThreadLocal.get());
            int rows = pst.executeBatch().length;
            rs = pst.getGeneratedKeys();
            //回显主键
            echoIdConsumer.accept(rs);
            return rows;
        } catch (Exception e) {
            throw new SqlException(e);
        } finally {
            valuesThreadLocal.remove();
            releaseRs(rs);
        }
    }

    /**
     * @params [sql sql语句, mappingFunc 映射结果集的函数]
     * @desc 执行查询
     */
    @SuppressWarnings("unchecked")
    private <T> T doQuery(String sql, ThrowFunction<ResultSet> mappingFunc) {
        ResultSet rs = null;
        try (Connection cn = DataSourceUtils.getConnection(dataSource); PreparedStatement pst = cn.prepareStatement(sql)) {
            fillPst(pst, valuesThreadLocal.get());
            rs = pst.executeQuery();
            return mappingFunc.apply(rs);
        } catch (Exception e) {
            throw new SqlException(e);
        } finally {
            valuesThreadLocal.remove();
            releaseRs(rs);
        }
    }

    private <T> Page<T> doPage(Class<T> clazz, Page<T> page, String sql, BiFunction<Class<Integer>, String, Integer> countFunc, BiFunction<Class<T>, String, List<T>> listFunc) {
        if (page.isSearchTotal()) {
            Integer count = countFunc.apply(Integer.class, "select count(*) from (" + sql + ")");
            page.setTotal(null == count ? 0 : count);
        }
        page.setList(listFunc.apply(clazz, sql + " limit " + (page.getPageNum() - 1) + "," + page.getPageSize()));
        return page;
    }

    /**
     * @params [pst PreparedStatement, values 参数列表]
     * @desc 向批量占位符中填充值
     */
    @SuppressWarnings("unchecked")
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
        //返回的主键值只会有一个，即使表中是复合主键也只会返回第一个主键的值
        setFieldValue(entity, idField, rs.getObject(1));
    }

    /**
     * @params [rs 结果集, entities 批量插入的对象列表]
     * @desc 将插入后的id回显到实体对象列表中
     */
    private <T> void echoIdList(ResultSet rs, List<T> entities) throws Exception {
        Field idField = getIdField(entities.get(0).getClass());
        rs.next();
        for (int i = 0; i < entities.size() && rs.next(); ++i) {
            //返回的主键值只会有一个，即使表中是复合主键也只会返回第一个主键的值
            setFieldValue(entities.get(i), idField, rs.getObject(1));
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

    private <T> T mappingOne(ResultSet rs, Class<T> clazz) throws Exception {
        if (null == rs) {
            return null;
        }
        //跳过表头
        rs.next();
        //存放属于子对象的数据
        Map<String, Object> anyChild = new HashMap<>();
        Map<String, Field> fieldMap = getFieldMap(clazz);
        ResultSetMetaData md = rs.getMetaData();
        T t = mappingLine(rs, md, fieldMap, anyChild, clazz);
        if (rs.next()) {
            throw new SqlException("too many result");
        }
        return t;
    }

    /**
     * @params [rs 查询结果集, clazz 需要返回对象的字节码]
     * @desc 将结果集映射到对象中
     */
    private <T> List<T> mappingAll(ResultSet rs, Class<T> clazz) throws Exception {
        List<T> list = new ArrayList<>();
        if (null == rs) {
            return list;
        }
        Map<String, Field> fieldMap = getFieldMap(clazz);
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

    private <T> T mappingLine(ResultSet rs, ResultSetMetaData md, Map<String, Field> fieldMap, Map<String, Object> anyChild, Class<T> clazz) throws Exception {
        T t = clazz.newInstance();
        for (int i = 1; i <= md.getColumnCount(); ++i) {
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
        return t;
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
    private Map<String, Field> getFieldMap(Class<?> clazz) {
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
    private void releaseRs(ResultSet rs) {
        try {
            if (null != rs) {
                rs.close();
            }
        } catch (SQLException e) {
            throw new SqlException(e);
        }
    }

}
