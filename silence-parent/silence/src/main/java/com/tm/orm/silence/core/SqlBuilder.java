package com.tm.orm.silence.core;

import com.google.common.base.CaseFormat;
import com.tm.orm.silence.exception.SqlException;
import org.apache.commons.jexl2.Expression;
import org.apache.commons.jexl2.JexlContext;
import org.apache.commons.jexl2.JexlEngine;
import org.apache.commons.jexl2.MapContext;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @Author yudm
 * @Date 2021/1/1 13:16
 * @Desc
 */
public class SqlBuilder {
    public static void main(String[] args) {
        String s = "SELECT partition_name name, partition_expression expression, partition_description description, table_rows tableRows " +
                "FROM information_schema.PARTITIONS" +
                "@[" +
                "&[tableName!= null&&tableName != '': AND table_name = ${tableName}]" +
                "&[beginTime != null: AND partition_description>=#{beginTime}]" +
                "&[endTime != null: AND partition_description<=#{endTime}]" +
                "]";
        Map<String, Object> map = new HashMap<>();
        map.put("tableName", "sss");
        map.put("beginTime", 1);
        map.put("endTime", 2);
        SqlBuilder builder = new SqlBuilder();
        List<Object> list = new ArrayList<>();
        String sql = builder.build(s, map, list);
        System.out.println(sql);

        long b = System.currentTimeMillis();
        /*for (int i = 0; i < 1000; ++i) {
            String s1 = builder.build(s, map, list);
        }*/
        long e = System.currentTimeMillis();
        System.out.println(e - b);


    }

    /**
     * @Author yudm
     * @Date 2020/9/25 15:49
     * @Param [tableName, columns]
     * @Desc 构建插入sql
     */
    public String buildInsertSql(String tableName, List<String> columns) {
        StringBuilder sql = new StringBuilder("INSERT INTO ").append(CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, tableName)).append(" (");
        for (String column : columns) {
            sql.append(CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, column)).append(",");
        }
        //去掉最后一个“,”号
        sql.deleteCharAt(sql.length() - 1);
        sql.append(") VALUES (");
        for (int i = 0; i < columns.size(); ++i) {
            sql.append("?,");
        }
        //去掉最后一个“,”号
        sql.deleteCharAt(sql.length() - 1);
        sql.append(")");
        return sql.toString();
    }

    /**
     * @Author yudm
     * @Date 2020/9/25 15:49
     * @Param [tableName, columns]
     * @Desc 构建更新sql
     */
    public String buildUpdateSql(String tableName, List<String> columns) {
        StringBuilder sql = new StringBuilder("UPDATE ").append(CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, tableName)).append(" SET ");
        for (String column : columns) {
            sql.append(CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, column)).append("=?,");
        }
        //去掉最后一个“,”号
        sql.deleteCharAt(sql.length() - 1);
        sql.append(" where ").append(CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, columns.get(0))).append("=?");
        return sql.toString();
    }

    /**
     * @Author yudm
     * @Date 2020/9/25 15:49
     * @Param [tableName, columns]
     * @Desc 构建删除sql
     */
    public String buildDeleteSql(String tableName, String keyName) {
        return "DELETE FROM " + CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, tableName) + " WHERE " + CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, keyName) + "=?";
    }

    /**
     * @Author yudm
     * @Date 2020/9/25 15:49
     * @Param [tableName, columns]
     * @Desc 构建查询sql
     */
    public String buildSelectSql(String tableName, List<String> columns) {
        StringBuilder sql = new StringBuilder("SELECT * FROM ").append(CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, tableName));
        if (null == columns || columns.size() < 1) {
            return sql.toString();
        }
        sql.append(" WHERE ");
        for (String column : columns) {
            sql.append(CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, column)).append("=? AND ");
        }
        //去掉最后一个空格和AND号
        sql.delete(sql.length() - 5, sql.length() - 1);
        return sql.toString();
    }

    public String build(String pSql, Map<String, Object> param, List<Object> values) {
        ArrayList<Entry> ens = entries(pSql);
        if (ens.size() == 0) {
            //不存在动态语句
            return paramBlock(pSql, param, values);
        } else {
            //截取第一个非动态语句
            StringBuilder sb = new StringBuilder(paramBlock(pSql.substring(0, ens.get(0).getBegin()), param, values));
            for (int i = 0; i < ens.size(); ++i) {
                Entry en = ens.get(i);
                //截取第i个动态语句
                String ds = pSql.substring(en.getBegin(), en.getEnd() + 1);
                //如果是if语句块
                if (ds.startsWith("&[")) {
                    sb.append(ifBlock(ds, param, values));
                }
                //如果是where语句块
                if (ds.startsWith("@[")) {
                    sb.append(whereBlock(ds, param, values));
                }
                if (i < ens.size() - 1) {
                    //截取最后的非动态语句
                    sb.append(pSql, en.getEnd() + 1, ens.get(i + 1).getBegin());
                }
            }
            sb.append(pSql, ens.get(ens.size() - 1).getEnd() + 1, pSql.length());
            return sb.toString();
        }
    }

    public List<String> getColumns(Class<?> clazz) {
        if (null == clazz) {
            throw new SqlException("clazz can not be null");
        }
        List<String> columns = new ArrayList<>();
        for (Field field : clazz.getDeclaredFields()) {
            if (Modifier.isStatic(field.getModifiers())) {
                continue;
            }
            columns.add(field.getName());
        }
        return columns;
    }

    public List<String> getColumns(Map<String, Object> kv, boolean selective) {
        if (kv == null || kv.size() == 0) {
            return null;
        }
        List<String> columns = new ArrayList<>();
        for (Map.Entry<String, Object> entry : kv.entrySet()) {
            if (selective) {
                if (null == entry.getValue()) {
                    continue;
                }
            }
            columns.add(entry.getKey());
        }
        return columns;
    }

    public List<Object> getValues(Map<String, Object> kv, boolean selective) {
        if (kv == null || kv.size() == 0) {
            return null;
        }
        List<Object> values = new ArrayList<>();
        for (Map.Entry<String, Object> entry : kv.entrySet()) {
            if (selective) {
                if (null == entry.getValue()) {
                    continue;
                }
            }
            values.add(entry.getValue());
        }
        return values;
    }


    @SuppressWarnings("unchecked")
    public Map<String, Object> toMap(Object obj) {
        if (null == obj) {
            throw new SqlException("value can not be null");
        }
        if (obj instanceof Map) {
            return (Map<String, Object>) obj;
        }
        Field[] fields = obj.getClass().getDeclaredFields();
        Map<String, Object> param = new HashMap<>();
        for (Field field : fields) {
            if (Modifier.isStatic(field.getModifiers())) {
                continue;
            }
            try {
                boolean flag = field.isAccessible();
                field.setAccessible(true);
                param.put(field.getName(), field.get(obj));
                field.setAccessible(flag);
            } catch (IllegalAccessException e) {
                throw new SqlException(e);
            }
        }
        return param;
    }

    private String paramBlock(String pSql, Map<String, Object> param, List<Object> values) {
        if (pSql.contains("#{")) {
            ArrayList<String> ks = extractAll(pSql, "#\\{.*?}");
            for (String k : ks) {
                //真正的变量名
                k = k.replaceAll("#\\{|}", "");
                if (null == param || !param.containsKey(k)) {
                    throw new SqlException("there is no field named:" + k);
                }
                if (null == values) {
                    throw new SqlException("there is no values");
                }
                //加入到sql参数列表中
                values.add(param.get(k));
            }
            pSql = pSql.replaceAll("#\\{.*?}", "?");
        } else if (pSql.contains("${")) {
            ArrayList<String> ks = extractAll(pSql, "\\$\\{.*?}");
            for (String t : ks) {
                String k = t.replaceAll("\\$\\{|}", "");
                if (!param.containsKey(k)) {
                    throw new SqlException("there is no field named:" + k);
                }
                Object obj = param.get(k);
                if (obj instanceof String) {
                    pSql = pSql.replace(t, (String) obj);
                } else {
                    pSql = pSql.replace(t, String.valueOf(obj));
                }
            }
        } else if (pSql.contains("%{")) {
            ArrayList<String> ks = extractAll(pSql, "%\\{.*?}");
            for (String t : ks) {
                String k = t.replaceAll("%\\{|}", "");
                if (!param.containsKey(k)) {
                    throw new SqlException("there is no field named:" + k);
                }
                Object obj = param.get(k);
                if (obj instanceof List) {
                    List<?> list = (List<?>) obj;
                    StringBuilder b = new StringBuilder("(");
                    for (Object o : list) {
                        b.append("?,");
                        values.add(o);
                    }
                    b.deleteCharAt(b.length() - 1);
                    b.append(")");
                    pSql = pSql.replaceAll(t, b.toString());
                } else {
                    throw new SqlException(k + "is not a list");
                }
            }
        }
        return pSql + " ";
    }

    private String ifBlock(String ds, Map<String, Object> param, List<Object> values) {
        String str = extract(ds, "&\\[.+?:");
        if (null == str || str.isEmpty()) {
            throw new SqlException("sql statement error: there is no conditions in &[]");
        }
        //截取真正的条件表达式
        String cd = str.replaceAll("&\\[|:", "").replaceAll("'", "\"");
        JexlEngine je = new JexlEngine();
        Expression ep = je.createExpression(cd);
        JexlContext jc = new MapContext();
        //条件表达式中的变量名
        ArrayList<String> ks = extractAll(ds, "(\\[|&&|\\|\\|).+?[!=><]");
        for (String k : ks) {
            //真正的变量名字
            k = k.replaceAll("(\\[|&&|\\|\\|)|[!=><]|\\s*", "");
            if (null == param || !param.containsKey(k)) {
                throw new SqlException("there is no field named:" + k);
            }
            jc.set(k, param.get(k));
        }
        if ((boolean) ep.evaluate(jc)) {
            return build(extract(ds, ":.*]").replaceAll("[:\\]]", ""), param, values);
        }
        return "";
    }

    private String whereBlock(String ds, Map<String, Object> param, List<Object> values) {
        String w = build(ds.substring(2, ds.length() - 1), param, values);
        if (null == w || w.isEmpty()) {
            return "";
        }
        //把多余的and或or去掉
        if (match(w, "\\s*((?i)and|or).*")) {
            w = w.replaceFirst("\\s*(?i)and|or", "");
        }
        return "where" + w;
    }

    private String extract(String str, String regex) {
        Pattern p = Pattern.compile(regex);
        Matcher m = p.matcher(str);
        if (m.find()) {
            return m.group(0);
        }
        return "";
    }

    private ArrayList<String> extractAll(String str, String regex) {
        ArrayList<String> s = new ArrayList<>();
        Pattern p = Pattern.compile(regex);
        Matcher m = p.matcher(str);
        while (m.find()) {
            s.add(m.group(0));
        }
        return s;
    }

    public ArrayList<Entry> entries(String str) {
        ArrayList<Entry> ens = new ArrayList<>();
        ArrayList<Integer> b = new ArrayList<>();
        ArrayList<Integer> e = new ArrayList<>();
        Pattern p = Pattern.compile("[@&]\\[|]");
        Matcher m = p.matcher(str);
        while (m.find()) {
            if (m.group(0).contains("[")) {
                b.add(m.start());
            } else if (m.group(0).contains("]")) {
                e.add(m.start());
            }
            if (b.size() == e.size()) {
                Entry en = new Entry();
                en.setBegin(b.get(0));
                en.setEnd(e.get(e.size() - 1));
                b.clear();
                e.clear();
                ens.add(en);
            }
        }
        if (b.size() != e.size()) {
            throw new SqlException("sql statement error: [ is not closed");
        }
        return ens;
    }

    private Boolean match(String str, String regex) {
        return Pattern.compile(regex).matcher(str).matches();
    }

    static class Entry {
        private int begin;
        private int end;

        public int getBegin() {
            return begin;
        }

        public void setBegin(int begin) {
            this.begin = begin;
        }

        public int getEnd() {
            return end;
        }

        public void setEnd(int end) {
            this.end = end;
        }
    }
}
