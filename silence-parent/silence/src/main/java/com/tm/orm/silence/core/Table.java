package com.tm.orm.silence.core;


import com.google.common.base.CaseFormat;
import com.tm.orm.silence.exception.SqlException;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.List;

/**
 * @author yudm
 * @date 2020/9/19 17:32
 * @desc 用于操作数据库的类，封装有一些简单通用的CURD操作，同时也可以执行动态的SQL语句。
 */
@Component
public class Table {
    //sql语句执行器
    private static SqlExecutor sqlExecutor;

    @Resource
    public void setSqlExecutor(SqlExecutor sqlExecutor) {
        Table.sqlExecutor = sqlExecutor;
    }

    /**
     * @params [entity 实体对象, selective 是否过滤掉null, echoId 是否回显主键值]
     * @desc 单条插入
     */
    public static int insert(Object entity, boolean selective, boolean echoId) {
        return sqlExecutor.insert(entity, selective, echoId);
    }

    /**
     * @params [entities 实体对象列表, selective 是否过滤掉null, echoId 是否回显主键值]
     * @desc 批量插入
     */
    public static <T> int insertList(List<T> entities, boolean selective, boolean echoId) {
        return sqlExecutor.insertList(entities, selective, echoId);
    }

    /**
     * @params [entity 实体对象, selective 是否过滤掉null]
     * @desc 根据主键更新
     */
    public static int updateById(Object entity, boolean selective) {
        return sqlExecutor.updateById(entity, selective);
    }

    /**
     * @params [entities 实体对象]
     * @desc 根据主键删除
     */
    public static int deleteById(Object entity) {
        return sqlExecutor.deleteById(entity);
    }

    /**
     * @params [sql 简单增删改sql语句，可含有占位符，但不能含有动态语句, data 占位符对应的参数列表]
     * @desc 通过简单sql进行增删改
     */
    public static int simpleExecute(String sql, Object... data) {
        return sqlExecutor.simpleExecute(sql, data);
    }

    /**
     * @params [sql 复杂增删改sql语句，含有动态语句, data 参数]
     * @desc 通过带有动态语句的sql进行增删改
     */
    public static int execute(String sql, Object data) {
        return sqlExecutor.execute(sql, data);
    }

    /**
     * @params [clazz 需要返回的对象字节码, sql 简单查询sql语句，可含有占位符，但不能含有动态语句, data 占位符对应的参数列表]
     * @desc 通过简单sql语句查询单个，
     */
    public static <T> T simpleSelectOne(Class<T> clazz, String sql, Object... data) {
        return doSelectOne(sqlExecutor.simpleQuery(clazz, sql, data));
    }

    /**
     * @params [clazz 需要返回的对象字节码, sql 简单查询sql语句，可含有占位符，但不能含有动态语句, data 占位符对应的参数列表]
     * @desc 通过简单sql语句查询多个
     */
    public static <T> List<T> simpleSelectList(Class<T> clazz, String sql, Object data) {
        return sqlExecutor.simpleQuery(clazz, sql, data);
    }

    /**
     * @params [clazz 需要返回的对象类型,sql 复杂查询sql语句，含有动态语句, data 参数]
     * @desc 通过带有动态语句的sql查询单个
     */
    public static <T> T selectOne(Class<T> clazz, String sql, Object data) {
        return doSelectOne(sqlExecutor.query(clazz, sql, data));
    }


    /**
     * @params [clazz 需要返回的对象类型,sql 复杂查询sql语句，含有动态语句, data 参数]
     * @desc 通过带有动态语句的sql查询多个
     */
    public static <T> List<T> selectList(Class<T> clazz, String sql, Object data) {
        return sqlExecutor.query(clazz, sql, data);
    }

    /**
     * @params [clazz 需要返回的对象字节码, page 分页对象, sql 简单查询sql语句, data 占位符对应的参数列表]
     * @desc 通过简单sql语句分页查询
     */
    public static <T> Page<T> simpleSelectPage(Class<T> clazz, Page<T> page, String sql, Object... data) {
        if (page.isSearchTotal()) {
            Integer count = doSelectOne(sqlExecutor.simpleQuery(Integer.class, "select count(*) from (" + sql + ")", data));
            page.setTotal(null == count ? 0 : count);
        }
        page.setList(sqlExecutor.simpleQuery(clazz, sql + " limit " + (page.getPageNum() - 1) + "," + page.getPageSize(), data));
        return page;
    }

    /**
     * @params [clazz 需要返回的对象字节码, page 分页对象, sql 复杂查询sql语句，含有动态语句, data 占位符对应的参数列表]
     * @desc 通过带有动态语句的sql分页查询
     */
    public static <T> Page<T> selectPage(Class<T> clazz, Page<T> page, String sql, Object data) {
        if (page.isSearchTotal()) {
            Integer count = doSelectOne(sqlExecutor.query(Integer.class, "select count(*) from (" + sql + ")", data));
            page.setTotal(null == count ? 0 : count);
        }
        page.setList(sqlExecutor.query(clazz, sql + " limit " + (page.getPageNum() - 1) + "," + page.getPageSize(), data));
        return page;
    }

    /**
     * @params [clazz 用于映射表和返回对象]
     * @desc 查询该表中所有数据
     **/
    public static <T> List<T> selectAll(Class<T> clazz) {
        return sqlExecutor.simpleQuery(clazz, "select * from " + CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, clazz.getSimpleName()));
    }

    /**
     * @params [clazz 用于映射表]
     * @desc 查询该表总数
     **/
    public static int selectCount(Class<?> clazz) {
        Integer count = doSelectOne(sqlExecutor.simpleQuery(Integer.class, "select count(*) from " + clazz.getSimpleName()));
        return null == count ? 0 : count;
    }

    /**
     * @params [list 查询结果]
     * @desc 查询一个的执行者
     */
    private static <T> T doSelectOne(List<T> list) {
        if (list.size() == 0) {
            return null;
        } else if (list.size() == 1) {
            return list.get(0);
        } else {
            throw new SqlException("too many result");
        }
    }

    private static String getPackagePath() {
        return Thread.currentThread().getStackTrace()[3].getClassName();
    }
}
