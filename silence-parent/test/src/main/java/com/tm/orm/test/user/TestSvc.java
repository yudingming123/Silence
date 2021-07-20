package com.tm.orm.test.user;

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
import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.*;
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

    public void insert() {
        String sql1 = "select * from test";
        Connection cn = null;
        try {
            cn = dataSource.getConnection();
            PreparedStatement pst = cn.prepareStatement(sql1);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
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
        Table.simpleUpdate("delete from test");
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
        Table.simpleUpdate("delete from test");
        return end - begin;
    }

    @Transactional
    public Long add11() {
        long begin = System.currentTimeMillis();
        for (int i = 0; i < 1000; ++i) {
            Table.simpleUpdate("insert into test (name) values ('1')");
        }
        long end = System.currentTimeMillis();
        Table.simpleUpdate("delete from test");
        return end - begin;
    }

    @Transactional
    public Long add22() {
        long begin = System.currentTimeMillis();
        for (int i = 0; i < 1000; ++i) {
            testMapper.insert();
        }
        long end = System.currentTimeMillis();
        Table.simpleUpdate("delete from test");
        return end - begin;
    }

    public int addList() {
        List<Test> tests = new ArrayList<>();
        for (int i = 0; i < 100; ++i) {
            Test test = new Test();
            test.setName(i + "");
            tests.add(test);
        }
        int row = Table.insertList(tests);
        return row;
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

    public static final byte[][][] model = new byte[9][11][30];

    /**
     * @params [clazz 需要返回对象的字节码]
     * @desc 一次性获取clazz的所有字段并转化成name->field的map
     */
    private static <T> Map<String, Field> getFieldMap(Class<T> clazz) {
        Field[] fields = clazz.getDeclaredFields();
        Map<String, Field> fieldMap = new HashMap<>();
        for (Field field : fields) {
            fieldMap.put(field.getName(), field);
        }
        return fieldMap;
    }

    public static void main(String[] args) throws Exception {
     /*
    __  嵌套映射
    @[  where语句块
    &[  if语句块
    %[  foreach语句块
     */
    /*String s = "SELECT partition_name name, partition_expression expression, partition_description description, table_rows tableRows " +
            "FROM information_schema.PARTITIONS" +
            "@[" +
            "&[tableName!= null&&tableName != "": AND table_name = ${tableName}]" +
            "&[beginTime != null: AND partition_description>=#{beginTime}]" +
            "&[endTime != null: AND partition_description<=#{endTime}]" +
            "]";
    Map<String, Object> map = new HashMap<>();
    map.put("tableName", "sss");
    map.put("beginTime", 1);
    map.put("endTime", 2);
    *//*SqlBuilder builder = new SqlBuilder(null);
    List<Object> list = new ArrayList<>();
    String sql = builder.build(s, map, list);
    System.out.println(sql);*//*

    long b = System.currentTimeMillis();
    *//*for (int i = 0; i < 1000; ++i) {
        String s1 = builder.build(s, map, list);
    }*//*
    long e = System.currentTimeMillis();
    System.out.println(e - b);*/
        /*TreeSet<Integer> set = new TreeSet<>();
        TreeMap<Integer, Integer> map1 = new TreeMap<>();
        byte b1 = 10;
        LocalDate date = LocalDate.now();
        int y = date.getYear();
        int m = date.getMonthValue();
        int d = date.getDayOfYear();
        int d1 = date.getDayOfMonth();
        long t = date.toEpochDay();
        System.out.println(y);
        System.out.println(m);
        System.out.println(d);
        System.out.println(d1);
        System.out.println(t);
        System.out.println(date.toEpochDay());
        System.out.println(LocalDate.of(2000, 1, 1).toEpochDay());
        System.out.println(LocalDate.of(2100, 1, 1).toEpochDay());
        System.out.println(LocalDate.ofYearDay(2021, 182));

        byte[][][] model = new byte[9][11][30];*/
        /*Class<?> clazz = Test.class;
        String[] keys = {"id", "a", "name", "nameasd", "namesdb", "namexcv", "nameghjkkjkjjklg", "namesdfffdfg", "namewetsgfgj", "namewetsgfgffj", "namewetsgffhgj", "namewetsgkufgj", "namewetsggjfgj"};
        Map<String, String> map = new HashMap<>(100);
        for (int i = 0; i < 100; ++i) {
            map.put("" + i, "" + i);
        }

        long s1 = System.nanoTime();
        //Map<String, Field> fieldMap = getFieldMap(clazz);
        for (int i = 0; i < 1000; ++i) {
            *//*for (String key : keys) {
                Field field = fieldMap.get(key);
            }*//*
            map.clear();
        }
        long e1 = System.nanoTime();
        System.out.println(e1 - s1);

        long s2 = System.nanoTime();
        for (int i = 0; i < 1000; ++i) {
            *//*for (String key : keys) {
                Field field = clazz.getDeclaredField(key);
            }*//*
            new HashMap<>();
        }
        long e2 = System.nanoTime();
        System.out.println(e2 - s2);*/

        Test test = select();
        System.out.println(test.toString());

    }

    private static <T> T select() throws Exception {
        return doSelect(new TypeReference<T>() {});
    }

    private static <T> T doSelect(TypeReference<T> tTypeReference) throws Exception {
        Type type = tTypeReference.getType();
        Class<?> clazz = type.getClass();
        return (T) (clazz.newInstance());
    }

    private static boolean isTradingDay(LocalDate date) {
        byte year = (byte) date.getYear();
        byte month = (byte) date.getMonthValue();
        byte day = (byte) date.getDayOfMonth();
        return 1 == model[year - 1][month - 1][day - 1];
    }

    private static Optional<LocalDate> queryNextTradingDay(LocalDate date) {
        byte year = (byte) date.getYear();
        byte month = (byte) date.getMonthValue();
        byte day = (byte) date.getDayOfMonth();

        int y = year - 1;
        int m = month - 1;
        int d = day;
        for (; y < 10; ++y) {
            byte[][] ms = model[y];
            for (; m < 12; ++m) {
                byte[] ds = ms[m];
                for (; d < 31; ++d) {
                    if (ds[d] == 1) {
                        break;
                    }
                }
            }
        }
        return Optional.of(LocalDate.of(2020 + y + 1, m + 1, d + 1));
    }

    private static List<LocalDate> queryBetween(LocalDate from, LocalDate to) {

        return null;
    }


}
