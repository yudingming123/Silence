package com.tm.orm.silence.core;


import com.google.common.base.CaseFormat;
import com.tm.orm.silence.exception.SqlException;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.List;

/**
 * @Author yudm
 * @Date 2020/9/19 17:32
 * @Desc 用于操作数据库的类，提供一些简单通用的CURD操作，同时也可以动态的SQL语句。
 */
@Component
public class Table {
    private static SqlExecutor sqlExecutor;

    @Resource
    public void setSqlExecutor(SqlExecutor sqlExecutor) {
        Table.sqlExecutor = sqlExecutor;
    }

    /**
     * @Param [entity 实体类对象，用于用于映射表同时也是入参]
     * @Desc 通用添加，null值也会写入。
     **/
    public static int insert(Object entity, boolean selective, boolean echoId) {
        return sqlExecutor.insert(entity, selective, echoId);
    }

    /**
     * @Param [entities 实体对象列表]
     * @Desc 批量插入
     **/
    public static <T> int insertList(List<T> entities, boolean selective, boolean echoId) {
        return sqlExecutor.insertList(entities, selective, echoId);
    }

    /**
     * @Param [entity 实体类对象，用于用于映射表同时也是入参]
     * @Desc 通过主键更新，以第一个字段作为主键
     **/
    public static int updateById(Object entity) {
        return sqlExecutor.updateById(entity, false);
    }

    /**
     * @Param [entity 实体类对象，用于用于映射表同时也是入参]
     * @Desc 通过主键更新，以第一个字段作为主键，null值不写入
     **/
    public static int updateByIdSelective(Object entity) {
        return sqlExecutor.updateById(entity, true);
    }

    public static int deleteById(Object entity) {
        return sqlExecutor.deleteById(entity);
    }

    /**
     * @Param [pSql 自定义动态sql, data 参数]
     * @Desc 执行任意写操作
     **/
    public static int simpleExecute(String sql, Object... data) {
        return sqlExecutor.simpleExecute(sql, data);
    }

    /**
     * @Param [pSql 自定义动态sql, data 参数]
     * @Desc 执行任意写操作
     **/
    public static int execute(String sql, Object data) {
        return sqlExecutor.execute(sql, data);
    }

    public static <T> T simpleSelectOne(Class<T> clazz, String sql, Object... data) {
        return doSelectOne(sqlExecutor.simpleQuery(clazz, sql, data));
    }

    /**
     * @Param [pSql 动态sql, data 入参, clazz 返回类型]
     * @Desc 查询单条数据
     **/
    public static <T> T selectOne(Class<T> clazz, String sql, Object data) {
        return doSelectOne(sqlExecutor.query(clazz, sql, data));
    }

    public static <T> List<T> simpleSelectList(Class<T> clazz, String sql, Object data) {
        return sqlExecutor.simpleQuery(clazz, sql, data);
    }

    /**
     * @Param [pSql 动态sql, data 入参, clazz 返回类型]
     * @Desc 查询多条数据
     **/
    public static <T> List<T> selectList(Class<T> clazz, String sql, Object data) {
        return sqlExecutor.query(clazz, sql, data);
    }


    public static <T> Page<T> simpleSelectPage(Class<T> clazz, Page<T> page, String sql, Object... data) {
        if (page.isSearchTotal()) {
            Integer count = doSelectOne(sqlExecutor.simpleQuery(Integer.class, "select count(*) from (" + sql + ")", data));
            page.setTotal(null == count ? 0 : count);
        }
        page.setList(sqlExecutor.simpleQuery(clazz, sql + " limit " + (page.getPageNum() - 1) + "," + page.getPageSize(), data));
        return page;
    }

    /**
     * @Param [pageNum 当前页号, pageSize 每页大小, total 是否查询总数量, clazz 用于映射表及返回类型]
     * @Desc 分页查询该表
     **/
    public static <T> Page<T> selectPage(Class<T> clazz, Page<T> page, String sql, Object data) {
        if (page.isSearchTotal()) {
            Integer count = doSelectOne(sqlExecutor.query(Integer.class, "select count(*) from (" + sql + ")", data));
            page.setTotal(null == count ? 0 : count);
        }
        page.setList(sqlExecutor.query(clazz, sql + " limit " + (page.getPageNum() - 1) + "," + page.getPageSize(), data));
        return page;
    }

    /**
     * @Param [clazz 用于映射表]
     * @Desc 查询该表中所有数据
     **/
    public static <T> List<T> selectAll(Class<T> clazz) {
        return sqlExecutor.simpleQuery(clazz, "select * from " + CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, clazz.getSimpleName()));
    }

    /**
     * @Param [clazz 用于映射表]
     * @Desc 查询该表总数
     **/
    public static int selectCount(Class<?> clazz) {
        Integer count = doSelectOne(sqlExecutor.simpleQuery(Integer.class, "select count(*) from " + clazz.getSimpleName()));
        return null == count ? 0 : count;
    }

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
