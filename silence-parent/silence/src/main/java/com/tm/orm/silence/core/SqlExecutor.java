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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author yudm
 * @Date 2021/5/13 17:40
 * @Desc
 */
@Component
public class SqlExecutor {
    @Resource
    private DataSource dataSource;

    @PostConstruct
    public void init() {
        if (null == dataSource) {
            throw new SqlException("Datasource is not initialized");
        }
    }

    public int update(String sql, List<Object> values) {
        Connection cn = null;
        PreparedStatement pst = null;
        try {
            cn = getCn();
            pst = cn.prepareStatement(sql);
            fillPst(pst, values);
            return pst.executeUpdate();
        } catch (SQLException e) {
            throw new SqlException(e);
        } finally {
            release(cn, pst, null);
        }
    }

    public int[] saveBatch(String sql, List<List<Object>> values) {
        Connection cn = null;
        PreparedStatement pst = null;
        try {
            cn = getCn();
            pst = cn.prepareStatement(sql);
            for (List<Object> value : values) {
                fillPst(pst, value);
                pst.addBatch();
            }
            return pst.executeBatch();
        } catch (SQLException e) {
            throw new SqlException(e);
        } finally {
            release(cn, pst, null);
        }
    }

    public <T> List<T> query(String sql, List<Object> values, Class<T> clazz) {
        Connection cn = null;
        PreparedStatement pst = null;
        ResultSet rs = null;
        try {
            cn = getCn();
            pst = cn.prepareStatement(sql);
            fillPst(pst, values);
            rs = pst.executeQuery();
            return parsRs(rs, clazz);
        } catch (Exception e) {
            throw new SqlException(e);
        } finally {
            release(cn, pst, rs);
        }
    }

    public List<Map<String, Object>> query(String sql, List<Object> values) {
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
    }


    private Connection getCn() throws SQLException {
        Connection cn = DataSourceUtils.getConnection(dataSource);
        if (!DataSourceUtils.isConnectionTransactional(cn, dataSource)) {
            cn.setAutoCommit(true);
        }
        return cn;
    }

    /**
     * @Author yudm
     * @Date 2020/9/25 15:48
     * @Param [statement, values]
     * @Desc 向sql的占位符中填充值
     */
    private void fillPst(PreparedStatement pst, List<Object> values) throws SQLException {
        if (null != values && values.size() > 0) {
            for (int i = 0; i < values.size(); ++i) {
                pst.setObject(i + 1, values.get(i));
            }
        }
    }

    private <T> List<T> parsRs(ResultSet rs, Class<T> clazz) throws Exception {
        if (null == rs) {
            return null;
        }
        List<T> list = new ArrayList<>();
        Field[] fields = clazz.getDeclaredFields();
        Map<String, Field> fMap = new HashMap<>();
        for (Field field : fields) {
            if (!Modifier.isStatic(field.getModifiers())) {
                fMap.put(field.getName(), field);
            }
        }
        T t = clazz.newInstance();
        ResultSetMetaData md = rs.getMetaData();
        int count = md.getColumnCount();
        //跳过表头
        while (rs.next()) {
            for (int i = 1; i <= count; ++i) {
                String colName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, md.getColumnName(i));
                Field field = fMap.get(colName);
                if (null != field) {
                    boolean flag = field.isAccessible();
                    field.setAccessible(true);
                    //从rs中获取值不要勇字段名获取，性能会很低
                    field.set(t, rs.getObject(i));
                    field.setAccessible(flag);
                }
            }
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

    private void release(Connection cn, Statement st, ResultSet rs) {
        try {
            if (null != cn && !DataSourceUtils.isConnectionTransactional(cn, dataSource)) {
                cn.close();
            }
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
