package com.tm.orm.silence.core;


import com.google.common.base.CaseFormat;
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
     * @params [entity 实体对象]
     * @desc 单条插入，为null的字段会被过滤掉
     */
    public static int insert(Object entity) {
        return sqlExecutor.insert(entity, true, false);
    }

    /**
     * @params [entity 实体对象]
     * @desc 单条插入，并回显主键的值，为null的字段会被过滤掉
     */
    public static int insertAndEchoId(Object entity) {
        return sqlExecutor.insert(entity, true, true);
    }

    /**
     * @params [entities 实体对象列表]
     * @desc 批量插入，为null的字段会被过滤掉
     */
    public static <T> int insertList(List<T> entities) {
        return sqlExecutor.insertList(entities, true, false);
    }

    /**
     * @params [entities 实体对象列表]
     * @desc 批量插入，并回显主键的值，为null的字段会被过滤掉
     */
    public static <T> int insertListAndEchoId(List<T> entities) {
        return sqlExecutor.insertList(entities, true, true);
    }

    /**
     * @params [entity 实体对象, selective 是否过滤掉null]
     * @desc 根据主键更新
     */
    public static int updateById(Object entity) {
        return sqlExecutor.updateById(entity, true);
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
     * @Param [clazz 用于映射表、获取主键名、返回对象类型, id 主键值]
     * @Desc 通过主键查询
     **/
    public static <T> T selectById(Class<T> clazz, Object id) {
        return sqlExecutor.doSelectOne(sqlExecutor.selectById(clazz, id));
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
        return sqlExecutor.doSelectOne(sqlExecutor.simpleQuery(Integer.class, "select count(*) from " + clazz.getSimpleName()));
    }

    /**
     * @params [clazz 需要返回的对象字节码, sql 简单查询sql语句，可含有占位符，但不能含有动态语句, data 占位符对应的参数列表]
     * @desc 通过简单sql语句查询单个，
     */
    public static <T> T simpleSelectOne(Class<T> clazz, String sql, Object... data) {
        return sqlExecutor.doSelectOne(sqlExecutor.simpleQuery(clazz, sql, data));
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
        return sqlExecutor.doSelectOne(sqlExecutor.query(clazz, sql, data));
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
        return sqlExecutor.simplePage(clazz, page, sql, data);
    }

    /**
     * @params [clazz 需要返回的对象字节码, page 分页对象, sql 复杂查询sql语句，含有动态语句, data 占位符对应的参数列表]
     * @desc 通过带有动态语句的sql分页查询
     */
    public static <T> Page<T> selectPage(Class<T> clazz, Page<T> page, String sql, Object data) {
        return sqlExecutor.page(clazz, page, sql, data);
    }

    /**
     * @Desc 获取service层调用者的包路径，用于切换数据源
     **/
    private static String getPackagePath() {
        return Thread.currentThread().getStackTrace()[3].getClassName();
    }
}
