package com.tm.orm.silence.core;

import com.tm.orm.silence.exception.SqlException;
import com.tm.orm.silence.function.BiThrowConsumer;
import com.tm.orm.silence.function.ThrowFunction;
import com.tm.orm.silence.function.ThrowConsumer;
import com.tm.orm.silence.util.ResultSetUtil;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import javax.sql.DataSource;
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
    public int insert(Object entity) {
        return doUpdate(sqlBuilder.buildInsertSql(entity), this::fillPst);
    }

    /**
     * @params [entity 实体对象]
     * @desc 插入单条数据，null会被过滤掉，并且回显主键
     */
    public int insertAndEchoId(Object entity) {
        return doUpdateAndEchoId(sqlBuilder.buildInsertSql(entity), this::fillPst, r -> ResultSetUtil.echoId(r, entity));
    }

    /**
     * @params [entities 实体对象列表]
     * @desc 批量插入，null会被过滤掉
     */
    public int insertList(List<?> entities) {
        return doUpdate(sqlBuilder.buildInsertListSql(entities), this::fillPstList);
    }

    /**
     * @params [entities 实体对象列表]
     * @desc 批量插入，null会被过滤掉，并且回显主键
     */
    public int insertListAndEchoId(List<Object> entities) {
        return doUpdateAndEchoId(sqlBuilder.buildInsertListSql(entities), this::fillPstList, r -> ResultSetUtil.echoIdList(r, entities));
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
        return doQuery(sqlBuilder.buildSelectByIdSql(clazz, id), r -> ResultSetUtil.mappingOne(r, clazz));
    }

    /**
     * @params [clazz 需要返回的对象类型, sql 简单查询sql语句，可含有占位符，但不能含有动态语句, data 占位符对应的参数列表]
     * @desc 执行简单查询
     */
    public <T> T simpleQueryOne(Class<T> clazz, String sql, Object... data) {
        valuesThreadLocal.set(Arrays.asList(data));
        return doQuery(sql, r -> ResultSetUtil.mappingOne(r, clazz));
    }

    /**
     * @params [clazz 需要返回的对象类型, sql 简单查询sql语句，可含有占位符，但不能含有动态语句, data 占位符对应的参数列表]
     * @desc 执行简单查询
     */
    public <T> List<T> simpleQueryList(Class<T> clazz, String sql, Object... data) {
        valuesThreadLocal.set(Arrays.asList(data));
        return doQuery(sql, r -> ResultSetUtil.mappingAll(r, clazz));
    }

    /**
     * @params [clazz 需要返回的对象类型,sql 复杂查询sql语句，含有动态语句, data 参数]
     * @desc 通过动态语句查询一个
     */
    public <T> T queryOne(Class<T> clazz, String sql, Object data) {
        return doQuery(sqlBuilder.build(sql, data), r -> ResultSetUtil.mappingOne(r, clazz));
    }

    /**
     * @params [clazz 需要返回的对象类型,sql 复杂查询sql语句，含有动态语句, data 参数]
     * @desc 通过动态语句查询多个
     */
    public <T> List<T> queryList(Class<T> clazz, String sql, Object data) {
        return doQuery(sqlBuilder.build(sql, data), r -> ResultSetUtil.mappingAll(r, clazz));
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
            ResultSetUtil.releaseRs(rs);
        }
    }

    /**
     * @params [sql sql语句, mappingFunc 映射结果集的函数]
     * @desc 执行查询
     */
    private <R> R doQuery(String sql, ThrowFunction<ResultSet, R> mappingFunc) {
        ResultSet rs = null;
        try (Connection cn = DataSourceUtils.getConnection(dataSource); PreparedStatement pst = cn.prepareStatement(sql)) {
            fillPst(pst, valuesThreadLocal.get());
            rs = pst.executeQuery();
            return mappingFunc.apply(rs);
        } catch (Exception e) {
            throw new SqlException(e);
        } finally {
            valuesThreadLocal.remove();
            ResultSetUtil.releaseRs(rs);
        }
    }

    /**
     * @Param [clazz 需要返回的类型, page 分页对象, sql sql语句, countFunc 查询数量的函数, listFunc 查询列表的函数]
     * @Desc 执行分页查询
     **/
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
    private void fillPstList(PreparedStatement pst, List<?> values) throws SQLException {
        for (Object value : values) {
            fillPst(pst, (List<Object>) value);
            pst.addBatch();
        }
    }

    /**
     * @params [pst PreparedStatement, values 参数列表]
     * @desc 向占位符中填充值
     */
    private void fillPst(PreparedStatement pst, List<?> values) throws SQLException {
        for (int i = 0; i < values.size(); ++i) {
            pst.setObject(i + 1, values.get(i));
        }
    }

}
