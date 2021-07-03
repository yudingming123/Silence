package com.tm.orm.silence.core;

import com.google.common.base.CaseFormat;
import com.tm.orm.silence.annotation.Id;
import com.tm.orm.silence.exception.SqlException;
import org.apache.commons.jexl2.Expression;
import org.apache.commons.jexl2.JexlContext;
import org.apache.commons.jexl2.JexlEngine;
import org.apache.commons.jexl2.MapContext;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @Author yudm
 * @Date 2021/1/1 13:16
 * @Desc
 */
public class SqlBuilder {
    private final JexlEngine jexlEngine = new JexlEngine();
    private final ThreadLocal<List<Object>> threadLocal;

    SqlBuilder(ThreadLocal<List<Object>> threadLocal) {
        this.threadLocal = threadLocal;
    }

    /**
     * @Param [tableName 表明, columns 列名]
     * @Desc 构建插入sql
     */
    public String buildInsertSql(Object entity, boolean selective) {
        if (null == entity) {
            throw new SqlException("entity can not be null");
        }
        //获取满足条件的字段，并将对应字段的值加入到threadLocal
        return doBuildInsertSql(entity.getClass().getSimpleName(), getNames(getFields(entity, selective)));
    }

    public <T> String buildInsertListSql(List<T> entities, boolean selective) {
        if (null == entities || entities.isEmpty()) {
            throw new SqlException("entities can not be empty");
        }
        //获取满足条件的字段，并将对应字段的值加入到threadLocal
        Object entity = entities.get(0);
        List<Field> fields = getFields(entity, selective);
        List<Object> valuesList = new ArrayList<>();
        //添加第一个对象中字段的值
        valuesList.add(threadLocal.get());
        //添加剩余对象中字段的值
        for (int i = 1; i < entities.size(); ++i) {
            valuesList.add(getValues(entities.get(i), fields));
        }
        threadLocal.set(valuesList);
        return doBuildInsertSql(entity.getClass().getSimpleName(), getNames(fields));

    }

    private String doBuildInsertSql(String tableName, List<String> names) {
        StringBuilder sql = new StringBuilder("insert into ").append(CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, tableName)).append(" (");
        for (String name : names) {
            sql.append(CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, name)).append(",");
        }
        //去掉最后一个“,”号
        sql.deleteCharAt(sql.length() - 1);
        sql.append(") values (");
        for (int i = 0; i < names.size(); ++i) {
            sql.append("?,");
        }
        //去掉最后一个“,”号
        sql.deleteCharAt(sql.length() - 1);
        sql.append(")");
        return sql.toString();
    }

    /**
     * @Param [tableName 表明, columns 列名]
     * @Desc 构建更新sql，以第一个字段作为主键
     */
    public String buildUpdateByIdSql(Object entity, boolean selective) {
        List<Field> fields = getFields(entity, selective);
        StringBuilder sql = new StringBuilder("update ").append(CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, entity.getClass().getSimpleName())).append(" set ");
        for (String name : getNames(fields)) {
            sql.append(CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, name)).append("=?,");
        }
        sql.append(" where ").append(getIdName(getFields(entity, false))).append(" = ?");
        return sql.toString();
    }

    /**
     * @Param [tableName 表明, keyName 主键名]
     * @Desc 构建删除sql
     */
    public String buildDeleteByIdSql(Object entity) {
        StringBuilder sql = new StringBuilder("update ").append(CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, entity.getClass().getSimpleName()));
        sql.append(" where ").append(getIdName(getFields(entity, false))).append(" = ?");
        return sql.toString();
    }

    /**
     * @Param [tableName 表名, columns 列名即查询条件]
     * @Desc 构建查询sql 多条件都已and连接
     */
    public String buildSelectSql(Object entity) {
        StringBuilder sql = new StringBuilder("select * from ").append(CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, entity.getClass().getSimpleName()));
        buildWhere(sql, getNames(getFields(entity, true)));
        return sql.toString();
    }

    private void buildWhere(StringBuilder sql, List<String> names) {
        sql.append(" where ");
        for (String name : names) {
            sql.append(CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, name)).append("=? and");
        }
        //去掉最后一个空格和and
        sql.delete(sql.length() - 5, sql.length() - 1);
    }

    public String build(String sql, Map<String, Object> param) {
        ArrayList<Entry> ens = entries(sql);
        if (ens.size() == 0) {
            //不存在动态语句
            return paramBlock(sql, param);
        } else {
            //截取第一个非动态语句
            StringBuilder sb = new StringBuilder(paramBlock(sql.substring(0, ens.get(0).getBegin()), param));
            for (int i = 0; i < ens.size(); ++i) {
                Entry en = ens.get(i);
                //截取第i个动态语句
                String ds = sql.substring(en.getBegin(), en.getEnd() + 1);
                //如果是if语句块
                if (ds.startsWith("&\\[")) {
                    sb.append(ifBlock(ds, param));
                }
                //如果是where语句块
                if (ds.startsWith("@\\[")) {
                    sb.append(whereBlock(ds, param));
                }
                //如果是foreach语句块
                if (ds.startsWith("%\\[")) {
                    sb.append(foreachBlock(ds, param));
                }
                if (i < ens.size() - 1) {
                    //截取动态sql之间的非动态sql
                    sb.append(sql, en.getEnd() + 1, ens.get(i + 1).getBegin());
                }
            }
            //截取最后的非动态语句
            sb.append(sql, ens.get(ens.size() - 1).getEnd() + 1, sql.length());
            return sb.toString();
        }
    }

    private String paramBlock(String sql, Map<String, Object> param) {
        if (sql.contains("#{")) {
            ArrayList<String> ks = extractAll(sql, "#\\{.*?}");
            for (String k : ks) {
                //真正的变量名
                k = k.replaceAll("#\\{|}", "");
                if (!param.containsKey(k)) {
                    throw new SqlException("there is no field named:" + k);
                }
                //加入到sql参数列表中
                threadLocal.get().add(param.get(k));
            }
            sql = sql.replaceAll("#\\{.*?}", "?");
        } else if (sql.contains("${")) {
            ArrayList<String> ks = extractAll(sql, "\\$\\{.*?}");
            for (String t : ks) {
                String k = t.replaceAll("\\$\\{|}", "");
                if (!param.containsKey(k)) {
                    throw new SqlException("there is no field named:" + k);
                }
                Object obj = param.get(k);
                if (obj instanceof String) {
                    sql = sql.replace(t, (String) obj);
                } else {
                    sql = sql.replace(t, String.valueOf(obj));
                }
            }
        }
        return sql + " ";
    }

    private String ifBlock(String ds, Map<String, Object> param) {
        String str = extract(ds, "&\\[.+?:");
        if (null == str || str.isEmpty()) {
            throw new SqlException("sql statement error: there is no conditions in &[]");
        }
        //截取真正的条件表达式
        String cd = str.replaceAll("&\\[|:", "").replaceAll("'", "\"");
        Expression expression = jexlEngine.createExpression(cd);
        JexlContext jexlContext = new MapContext();
        //条件表达式中的变量名
        ArrayList<String> ks = extractAll(ds, "(\\[|&&|\\|\\|).+?[!=><]");
        for (String k : ks) {
            //真正的变量名字
            k = k.replaceAll("(\\[|&&|\\|\\|)|[!=><]|\\s*", "");
            if (null == param || !param.containsKey(k)) {
                throw new SqlException("there is no field named:" + k);
            }
            jexlContext.set(k, param.get(k));
        }
        if ((boolean) expression.evaluate(jexlContext)) {
            return build(extract(ds, ":.*]").replaceAll("[:\\]]", ""), param);
        }
        return "";
    }

    private String whereBlock(String ds, Map<String, Object> param) {
        String w = build(ds.substring(2, ds.length() - 1), param);
        if (w.isEmpty()) {
            return "";
        }
        //把多余的and或or去掉
        if (Pattern.compile("\\s*((?i)and|or).*").matcher(w).matches()) {
            w = w.replaceFirst("\\s*(?i)and|or", "");
        }
        return "where" + w;
    }

    private String foreachBlock(String ds, Map<String, Object> param) {
        String attribute = extract(ds, "%\\[.+?:").replaceAll("%\\[|:", "");
        String[] attributes = attribute.split(",");
        if (attributes.length < 5) {
            throw new SqlException("bad foreach statement");
        }
        Map<String, String> attributeMap = new HashMap<>(5);
        for (String a : attributes) {
            String[] kv = a.split("=");
            if (kv.length < 2) {
                throw new SqlException("bad foreach statement");
            }
            attributeMap.put(kv[0].trim(), kv[1].trim());
        }
        if (!attributeMap.keySet().containsAll(Arrays.asList("o", "c", "s", "i", "v"))) {
            throw new SqlException("Attribute [o,c,s,i,v] is necessary");
        }
        //截取真正的条件表达式
        String k = attributeMap.get("v");
        if (!param.containsKey(k)) {
            throw new SqlException("there is no field named:" + k);
        }
        Object v = param.get(k);
        StringBuilder sb = new StringBuilder();
        sb.append(attributeMap.get("o"));
        if (v instanceof Collection) {
            for (Object obj : (Collection) v) {
                param.put(attributeMap.get("i"), obj);
                sb.append(build(extract(ds, ":.*]").replaceAll("[:\\]]", ""), param));
                sb.append(attributeMap.get("s"));
            }
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.append(attributeMap.get("c"));
        sb.append(" ");
        return sb.toString();
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

    private ArrayList<Entry> entries(String str) {
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
            throw new SqlException("sql statement error: '[' is not closed");
        }
        return ens;
    }

    private List<Field> getFields(Object data, boolean selective) {
        if (null == data) {
            throw new SqlException("obj can not be null");
        }
        Class<?> clazz = data.getClass();
        List<Field> fields = new ArrayList<>();
        List<Object> values = new ArrayList<>();
        for (Field field : clazz.getDeclaredFields()) {
            boolean accessible = field.isAccessible();
            field.setAccessible(true);
            try {
                Object obj = field.get(data);
                if (selective && null == obj) {
                    continue;
                }
                values.add(obj);
            } catch (IllegalAccessException e) {
                throw new SqlException(e);
            }
            fields.add(field);
            field.setAccessible(accessible);
        }
        threadLocal.set(values);
        return fields;
    }

    public List<String> getNames(List<Field> fields) {
        List<String> names = new ArrayList<>();
        for (Field field : fields) {
            names.add(field.getName());
        }
        return names;
    }

    public String getIdName(List<Field> fields) {
        String idName = null;
        for (Field field : fields) {
            if (null != field.getAnnotation(Id.class)) {
                idName = field.getName();
                break;
            }
        }
        if (null == idName) {
            throw new SqlException("can not find primary key consider use @Id on primary key field");
        }

        return idName;
    }

    public List<Object> getValues(Object data, List<Field> fields) {
        List<Object> columns = new ArrayList<>();
        try {
            for (Field field : fields) {
                boolean accessible = field.isAccessible();
                field.setAccessible(true);
                columns.add(field.get(data));
                field.setAccessible(accessible);
            }
        } catch (IllegalAccessException e) {
            throw new SqlException(e);
        }
        return columns;
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
