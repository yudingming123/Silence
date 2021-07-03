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
 * @Author yudm
 * @Date 2021/5/13 17:40
 * @Desc
 */
@Component
public class SqlExecutor {
    @Resource
    private DataSource dataSource;
    //存放sql参数
    private final ThreadLocal<List<Object>> threadLocal = new ThreadLocal<>();
    private final SqlBuilder sqlBuilder = new SqlBuilder(threadLocal);

    @PostConstruct
    public void init() {
        if (null == dataSource) {
            throw new SqlException("Datasource is not initialized");
        }
    }

    public <T> int insert(T entity, boolean selective, boolean echoId) {
        return doInsert(sqlBuilder.buildInsertSql(entity, selective), entity, echoId);
    }

    public <T> int insertList(List<T> entities, boolean selective, boolean echoId) {
        return doInsertList(sqlBuilder.buildInsertListSql(entities, selective), entities, echoId);
    }

    public int updateById(Object entity, boolean selective) {
        return doExecute(sqlBuilder.buildUpdateByIdSql(entity, selective));
    }

    public int deleteById(Object entity) {
        return doExecute(sqlBuilder.buildDeleteByIdSql(entity));
    }

    public int simpleExecute(String sql, Object... data) {
        threadLocal.set(Arrays.asList(data));
        return doExecute(sql);
    }

    public int execute(String sql, Object data) {
        return doExecute(sqlBuilder.build(sql, toMap(data)));
    }

    public <T> List<T> simpleQuery(Class<T> clazz, String sql, Object... data) {
        threadLocal.set(Arrays.asList(data));
        return doQuery(sql, clazz);
    }

    public <T> List<T> query(Class<T> clazz, String sql, Object data) {
        return doQuery(sqlBuilder.build(sql, toMap(data)), clazz);
    }

    private int doInsert(String sql, Object obj, boolean echoId) {
        ResultSet rs = null;
        PreparedStatement pst = null;
        try (Connection cn = getCn()) {
            if (echoId) {
                pst = cn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
                fillValues(pst, threadLocal.get());
                int rows = pst.executeUpdate();
                rs = pst.getGeneratedKeys();
                echoId(rs, obj);
                return rows;
            }
            pst = cn.prepareStatement(sql);
            fillValues(pst, threadLocal.get());
            return pst.executeUpdate();
        } catch (Exception e) {
            throw new SqlException(e);
        } finally {
            releaseRs(pst, rs);
        }
    }

    private <T> int doInsertList(String sql, List<T> objs, boolean echoId) {
        ResultSet rs = null;
        PreparedStatement pst = null;
        try (Connection cn = getCn()) {
            if (echoId) {
                pst = cn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
                fillValuesList(pst, threadLocal.get());
                int rows = pst.executeBatch().length;
                rs = pst.getGeneratedKeys();
                echoIdList(rs, objs);
                return rows;
            }
            pst = cn.prepareStatement(sql);
            fillValuesList(pst, threadLocal.get());
            return pst.executeUpdate();
        } catch (Exception e) {
            throw new SqlException(e);
        } finally {
            releaseRs(pst, rs);
        }
    }

    private int doExecute(String sql) {
        try (Connection cn = getCn(); PreparedStatement pst = cn.prepareStatement(sql)) {
            fillValues(pst, threadLocal.get());
            return pst.executeUpdate();
        } catch (SQLException e) {
            throw new SqlException(e);
        } finally {
            threadLocal.remove();
        }
    }

    private <T> List<T> doQuery(String sql, Class<T> clazz) {
        ResultSet rs = null;
        try (Connection cn = getCn(); PreparedStatement pst = cn.prepareStatement(sql)) {
            fillValues(pst, threadLocal.get());
            rs = pst.executeQuery();
            return parsRs(rs, clazz);
        } catch (Exception e) {
            throw new SqlException(e);
        } finally {
            releaseRs(null, rs);
        }
    }

    /*public List<Map<String, Object>> query(String sql, List<Object> values) {
        Connection cn = null;
        PreparedStatement pst = null;
        ResultSet rs = null;
        try {
            cn = getCn();
            pst = cn.prepareStatement(sql);
            fillPst(pst, values);
            rs = pst.executeQuery();
            return parsRs(rs);
        } catch (Exception e) {
            throw new SqlException(e);
        } finally {
            release(cn, pst, rs);
        }
    }*/

    @SuppressWarnings("unchecked")
    private Map<String, Object> toMap(Object obj) {
        if (obj instanceof Map) {
            return (Map<String, Object>) obj;
        }
        Map<String, Object> param = new HashMap<>();
        if (null == obj) {
            return param;
        }
        Field[] fields = obj.getClass().getDeclaredFields();
        for (Field field : fields) {
            boolean flag = field.isAccessible();
            field.setAccessible(true);
            try {
                param.put(field.getName(), field.get(obj));
            } catch (IllegalAccessException e) {
                throw new SqlException(e);
            }
            field.setAccessible(flag);
        }
        return param;
    }

    private Connection getCn() throws SQLException {
        Connection cn = DataSourceUtils.getConnection(dataSource);
        if (!DataSourceUtils.isConnectionTransactional(cn, dataSource)) {
            cn.setAutoCommit(true);
        }
        return cn;
    }


    private void fillValuesList(PreparedStatement pst, List<Object> values) throws SQLException {
        for (Object value : values) {
            fillValues(pst, (List<Object>) value);
            pst.addBatch();
        }
    }

    /**
     * @Author yudm
     * @Date 2020/9/25 15:48
     * @Param [statement, values]
     * @Desc 向sql的占位符中填充值
     */
    private void fillValues(PreparedStatement pst, List<Object> values) throws SQLException {
        for (int i = 0; i < values.size(); ++i) {
            pst.setObject(i + 1, values.get(i));
        }
    }

    private void echoId(ResultSet rs, Object obj) throws Exception {
        Field field = obj.getClass().getDeclaredFields()[0];
        rs.next();
        boolean accessible = field.isAccessible();
        field.setAccessible(true);
        field.set(obj, rs.getObject(1));
        field.setAccessible(accessible);
    }

    private <T> void echoIdList(ResultSet rs, List<T> objs) throws Exception {
        Field field = objs.get(0).getClass().getDeclaredFields()[0];
        for (int i = 0; rs.next(); ++i) {
            boolean accessible = field.isAccessible();
            field.setAccessible(true);
            field.set(objs.get(i), rs.getObject(1));
            field.setAccessible(accessible);
        }
    }

    private <T> List<T> parsRs(ResultSet rs, Class<T> clazz) throws Exception {
        List<T> list = new ArrayList<>();
        if (null == rs) {
            return list;
        }
        Map<String, Field> fieldMap = getFieldMap(clazz);
        ResultSetMetaData md = rs.getMetaData();
        int count = md.getColumnCount();
        //跳过表头
        while (rs.next()) {
            T t = clazz.newInstance();
            parsOne(rs, md, count, t, fieldMap);
            list.add(t);
        }
        return list;
    }

    /**
     * @Author yudm
     * @Date 2020/10/4 12:33
     * @Param [clazz, resultSet]
     * @Desc 将ResultSet转化成对应的实体类集合
     */
    private List<Map<String, Object>> parsRs(ResultSet rs) throws SQLException {
        if (null == rs) {
            return null;
        }
        List<Map<String, Object>> list = new ArrayList<>();
        Map<String, Object> map = new HashMap<>();

        ResultSetMetaData md = rs.getMetaData();
        int count = md.getColumnCount();
        //跳过表头
        while (rs.next()) {
            for (int i = 1; i <= count; ++i) {
                //获取表中字段的名字并转为小写
                String colName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, md.getColumnName(i));
                map.put(colName, rs.getObject(i));
            }
            list.add(map);
        }

        return list;
    }

    private <T> Map<String, Field> getFieldMap(Class<T> clazz) {
        Field[] fields = clazz.getDeclaredFields();
        Map<String, Field> fMap = new HashMap<>();
        for (Field field : fields) {
            fMap.put(field.getName(), field);
        }
        return fMap;
    }

    private void parsOne(ResultSet rs, ResultSetMetaData md, int count, Object obj, Map<String, Field> fieldMap) throws Exception {
        for (int i = 1; i <= count; ++i) {
            String colName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, md.getColumnName(i));
            Field field = fieldMap.get(colName);
            if (null != field) {
                boolean flag = field.isAccessible();
                field.setAccessible(true);
                //从rs中获取值不要勇字段名获取，性能会很低
                field.set(obj, rs.getObject(i));
                field.setAccessible(flag);
            }
        }
    }

    private void releaseRs(Statement st, ResultSet rs) {
        threadLocal.remove();
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
