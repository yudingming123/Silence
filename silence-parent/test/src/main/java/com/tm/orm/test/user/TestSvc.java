package com.tm.orm.test.user;

import com.tm.orm.silence.core.SqlExecutor;
import com.tm.orm.silence.core.Table;
import com.tm.orm.test.dao.entity.Test;
import com.tm.orm.test.dao.mapper.TestMapper;
import org.mybatis.spring.SqlSessionTemplate;
import org.mybatis.spring.SqlSessionUtils;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


/**
 * @Author yudm
 * @Date 2021/5/30 20:13
 * @Desc
 */
@Service
//@Transactional
public class TestSvc {
    @Resource
    private DataSource dataSource;
    @Resource
    private SqlSessionTemplate st;
    @Resource
    private TestMapper testMapper;

    @Resource
    private SqlExecutor sqlExecutor;

    public void insert() {
        for (int i = 0; i < 30; ++i) {
            doInsert(10 * i, 10 * i + 10);
        }
        for (int i = 400; i < 500; ++i) {
            Test test = new Test();
            test.setName(i + "");
            Table.insert(test);
        }
    }

    @Transactional
    @Async
    public void doInsert(int begin, int end) {
        for (int i = begin; i < end; ++i) {
            Test test = new Test();
            test.setId(i);
            Table.insert(test);
        }
    }

    @Transactional
    public Long add1() {
        Test test = new Test();
        test.setName("1");
        long begin = System.currentTimeMillis();
        for (int i = 0; i < 1000; ++i) {
            Table.insert(test);
        }
        long end = System.currentTimeMillis();
        Table.update("delete from test", null);
        return end - begin;
    }

    @Transactional
    public Long add2() {

        Test test = new Test();
        test.setName("2");
        long begin = System.currentTimeMillis();
        for (int i = 0; i < 1000; ++i) {
            testMapper.insert();
        }
        long end = System.currentTimeMillis();
        Table.update("delete from test", null);
        return end - begin;
    }

    @Transactional
    public Long add11() {
        long begin = System.currentTimeMillis();
        for (int i = 0; i < 1000; ++i) {
            Table.update("insert into test (name) values ('1')", null);
        }
        long end = System.currentTimeMillis();
        Table.update("delete from test", null);
        return end - begin;
    }

    @Transactional
    public Long add22() {
        long begin = System.currentTimeMillis();
        for (int i = 0; i < 1000; ++i) {
            testMapper.insert();
        }
        long end = System.currentTimeMillis();
        Table.update("delete from test", null);
        return end - begin;
    }

    public void stsTest() throws Exception {
        Connection cn = DataSourceUtils.getConnection(dataSource);
        cn.setAutoCommit(false);
        ExecutorService service = Executors.newSingleThreadExecutor();
        service.execute(() -> {
            for (int i = 0; i < 10; ++i) {
                try {
                    PreparedStatement pst = cn.prepareStatement("insert into test (name) values (?)", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
                    pst.setObject(1, i + "");
                    pst.executeUpdate();
                    cn.commit();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }
        });
        Thread.sleep(1000);
        /*for (int i = 0; i < 10; ++i) {
            try {
                PreparedStatement pst = cn.prepareStatement("insert into test (name) values (?)");
                pst.setObject(1, i + "");
                pst.executeUpdate();
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }*/
        cn.commit();

    }

    public void dusu() throws Exception {
        try {
            long s2 = System.currentTimeMillis();
            for (int i = 0; i < 100000; ++i) {
                DataSourceUtils.getConnection(dataSource).close();
            }
            long e2 = System.currentTimeMillis();
            long s1 = System.currentTimeMillis();
            for (int i = 0; i < 100000; ++i) {
                SqlSessionUtils.getSqlSession(st.getSqlSessionFactory(), st.getExecutorType(), st.getPersistenceExceptionTranslator()).getConnection().close();
            }
            long e1 = System.currentTimeMillis();
            System.out.println(e1 - s1);
            System.out.println(e2 - s2);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

    }

}
