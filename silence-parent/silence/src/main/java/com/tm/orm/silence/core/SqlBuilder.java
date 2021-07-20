package com.tm.orm.silence.core;

import com.google.common.base.CaseFormat;
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
 * @author yudm
 * @date 2021/1/1 13:16
 * @desc 构建sql的类
 */
public class SqlBuilder {
    //用于执行if条件表达式
    private final JexlEngine jexlEngine = new JexlEngine();
    private final ThreadLocal<List<Object>> valuesThreadLocal;

    //用于查找动态语句块的开始和结束位置
    private final Pattern dynamicLabelPt = Pattern.compile("[@&%]\\[|]");
    //用于匹配if语句块及条件
    private final Pattern ifLabelPt = Pattern.compile("&\\[.+?:");
    //用于匹配if条件表达式中参数名
    private final Pattern ifKeyPt = Pattern.compile("(\\[|&&|\\|\\|).+?[!=><]");
    //用于匹配if和foreach语句块中的内容部分
    private final Pattern contentPt = Pattern.compile(":.*]");
    //用于匹配foreach语句块及属性值
    private final Pattern foreachLabelPt = Pattern.compile("%\\[.+?:");
    //用于匹配预编译参数
    private final Pattern preKeyLabelPt = Pattern.compile("#\\{.*?}");
    //用于匹配非预编译参数
    private final Pattern keyLabelPt = Pattern.compile("\\$\\{.*?}");


    SqlBuilder(ThreadLocal<List<Object>> valuesThreadLocal) {
        this.valuesThreadLocal = valuesThreadLocal;
    }

    /**
     * @params [entity 实体对象, selective 是否过滤null值]
     * @desc 构建插入sql
     */
    public String buildInsertSql(Object entity) {
        if (null == entity) {
            throw new SqlException("entity can not be null");
        }
        //获取满足条件的字段，并将对应字段的值加入到threadLocal
        return doBuildInsertSql(entity.getClass().getSimpleName(), getNames(getFields(entity)));
    }

    /**
     * @params [entity 实体对象, selective 是否过滤null值]
     * @desc 构建批量插入sql，并且将对应的值放入threadLocal
     */
    public <T> String buildInsertListSql(List<T> entities) {
        if (null == entities || entities.isEmpty()) {
            throw new SqlException("entities can not be empty");
        }
        //获取满足条件的字段，并将对应字段的值加入到threadLocal
        Object entity = entities.get(0);
        List<Field> fields = getFields(entity);
        List<Object> valuesList = new ArrayList<>();
        //添加第一个对象中字段的值
        valuesList.add(valuesThreadLocal.get());
        //添加剩余对象中字段的值
        for (int i = 1; i < entities.size(); ++i) {
            valuesList.add(getValues(entities.get(i), fields));
        }
        valuesThreadLocal.set(valuesList);
        return doBuildInsertSql(entity.getClass().getSimpleName(), getNames(fields));

    }

    /**
     * @params [entity 实体对象, selective 是否过滤null值]
     * @desc 构建通过主键更新sql
     */
    public String buildUpdateByIdSql(Object entity) {
        List<Field> fields = getFields(entity);
        StringBuilder sql = new StringBuilder("update ").append(CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, entity.getClass().getSimpleName())).append(" set ");
        for (String name : getNames(fields)) {
            sql.append("`");
            sql.append(CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, name)).append("` = ?, ");
        }
        //去掉最后的逗号
        sql.deleteCharAt(sql.length() - 2);
        sql.append("where ").append(getIdName(entity, fields)).append(" = ?");
        return sql.toString();
    }

    /**
     * @params [entity 实体对象]
     * @desc 构建通过主键删除sql
     */
    public String buildDeleteByIdSql(Object entity) {
        StringBuilder sql = new StringBuilder("delete from ").append(CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, entity.getClass().getSimpleName()));
        sql.append(" where ").append(getIdName(entity, getFields(entity))).append(" = ?");
        return sql.toString();
    }

    /**
     * @Param [clazz 实体类的字节码，用于映射表和获取主键名, id 主键值]
     * @Desc 构建通过主键查询sql
     **/
    public String buildSelectByIdSql(Class<?> clazz, Object id) {
        StringBuilder sql = new StringBuilder("select * from ").append(CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, clazz.getSimpleName()));
        sql.append(" where ").append(getIdName(clazz, id)).append(" = ?");
        return sql.toString();
    }

    /**
     * @Param [sql 动态sql语句, data 参数]
     * @Desc 根据动态sql和参数构建出最终的sql
     **/
    public String build(String sql, Object data) {
        Map<String, Object> param = toMap(data);
        //用于查找动态语句块的开始和结束位置
        return doBuild(sql, param);
    }

    /**
     * @Param [sql 动态sql语句, param map参数]
     * @Desc 执行根据动态sql和参数构建出最终的sql
     **/
    private String doBuild(String sql, Map<String, Object> param) {
        ArrayList<Position> positions = getPositions(sql);
        if (positions.size() == 0) {//不存在动态语句
            return paramBlock(sql, param);
        } else {
            //截取第一个非动态语句
            StringBuilder sb = new StringBuilder(paramBlock(sql.substring(0, positions.get(0).getBegin()), param));
            for (int i = 0; i < positions.size(); ++i) {
                Position position = positions.get(i);
                //截取第i个动态语句
                String ds = sql.substring(position.getBegin(), position.getEnd() + 1);
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
                if (i < positions.size() - 1) {
                    //截取动态sql之间的非动态sql
                    sb.append(sql, position.getEnd() + 1, positions.get(i + 1).getBegin());
                }
            }
            //截取最后的非动态语句
            sb.append(sql, positions.get(positions.size() - 1).getEnd() + 1, sql.length());
            return sb.toString();
        }
    }

    /**
     * @Param [sql 动态sql语句, param map参数]
     * @Desc 解析包含参数的语句块
     **/
    private String paramBlock(String sql, Map<String, Object> param) {
        if (sql.contains("#{")) {
            ArrayList<String> ks = extractAll(sql, this.preKeyLabelPt);
            for (String k : ks) {
                //真正的变量名
                k = k.replaceAll("#\\{|}", "");
                if (!param.containsKey(k)) {
                    throw new SqlException("there is no param named:" + k);
                }
                //加入到sql参数列表中
                valuesThreadLocal.get().add(param.get(k));
            }
            sql = sql.replaceAll("#\\{.*?}", "?");
        } else if (sql.contains("${")) {
            ArrayList<String> ks = extractAll(sql, this.keyLabelPt);
            for (String t : ks) {
                String k = t.replaceAll("\\$\\{|}", "");
                if (!param.containsKey(k)) {
                    throw new SqlException("there is no param named:" + k);
                }
                Object obj = param.get(k);
                if (obj instanceof String) {
                    sql = sql.replace(t, (String) obj);
                } else {
                    sql = sql.replace(t, String.valueOf(obj));
                }
            }
        }
        return sql;
    }

    /**
     * @Param [sql 动态sql语句, param map参数]
     * @Desc 解析if语句块
     **/
    private String ifBlock(String ds, Map<String, Object> param) {
        String condition = extract(ds, this.ifLabelPt);
        if (null == condition || condition.isEmpty()) {
            throw new SqlException("sql statement error: there is no conditions in &[]");
        }
        //截取真正的条件表达式
        condition = condition.replaceAll("&\\[|:", "").replaceAll("'", "\"");
        Expression expression = jexlEngine.createExpression(condition);
        JexlContext jexlContext = new MapContext();
        //条件表达式中的变量名
        ArrayList<String> ks = extractAll(ds, this.ifKeyPt);
        for (String k : ks) {
            //真正的变量名字
            k = k.replaceAll("(\\[|&&|\\|\\|)|[!=><]|\\s*", "");
            if (null == param || !param.containsKey(k)) {
                throw new SqlException("there is no param named:" + k);
            }
            jexlContext.set(k, param.get(k));
        }
        //判断表达式是否成立
        if ((boolean) expression.evaluate(jexlContext)) {
            return doBuild(extract(ds, this.contentPt).replaceAll("[:\\]]", ""), param);
        }
        return "";
    }

    /**
     * @Param [sql 动态sql语句, param map参数]
     * @Desc 解析where语句块
     **/
    private String whereBlock(String ds, Map<String, Object> param) {
        String w = doBuild(ds.substring(2, ds.length() - 1), param);
        if (w.isEmpty()) {
            return "";
        }
        //把多余的and或or去掉
        w = w.replaceFirst("^\\s*(?i)(and|or)", "");
        return "where" + w;
    }

    /**
     * @Param [sql 动态sql语句, param map参数]
     * @Desc 解析foreach语句块
     **/
    private String foreachBlock(String ds, Map<String, Object> param) {
        //获取属性
        String attribute = extract(ds, this.foreachLabelPt).replaceAll("%\\[|:", "");
        String[] attributes = attribute.split(",");
        if (attributes.length < 5) {
            throw new SqlException("bad foreach statement");
        }
        //将属性和属性的值转化成map
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
        //获取集合
        String collectionName = attributeMap.get("v");
        if (!param.containsKey(collectionName)) {
            throw new SqlException("there is no field named:" + collectionName);
        }
        Object collection = param.get(collectionName);
        if (!(collection instanceof Collection)) {
            throw new SqlException(collectionName + " is not a collection");
        }
        //开始拼接foreach语句块
        StringBuilder sb = new StringBuilder();
        sb.append(attributeMap.get("o"));
        for (Object obj : (Collection) collection) {
            param.put(attributeMap.get("i"), obj);
            sb.append(doBuild(extract(ds, this.contentPt).replaceAll("[:\\]]", ""), param));
            sb.append(attributeMap.get("s"));
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.append(attributeMap.get("c"));
        sb.append(" ");
        return sb.toString();
    }

    /**
     * @params [obj 入参]
     * @desc 将入参转化成map
     */
    private Map<String, Object> toMap(Object obj) {
        Map<String, Object> param = new HashMap<>();
        if (null == obj) {
            return param;
        }
        if (obj instanceof Map) {
            return (Map<String, Object>) obj;
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

    /**
     * @params [tableName 表名, names 字段名]
     * @desc 真正执行构建插入sql
     */
    private String doBuildInsertSql(String tableName, List<String> names) {
        StringBuilder sql = new StringBuilder("insert into ").append(CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, tableName)).append(" (");
        for (String name : names) {
            sql.append("`");
            sql.append(CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, name)).append("`,");
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
     * @Param [str , pattern]
     * @Desc 提取出第一个匹配的字符串
     **/
    private String extract(String str, Pattern pattern) {
        Matcher matcher = pattern.matcher(str);
        if (matcher.find()) {
            return matcher.group(0);
        }
        return "";
    }

    /**
     * @Param [str , pattern]
     * @Desc 提取出所有匹配的字符串
     **/
    private ArrayList<String> extractAll(String str, Pattern pattern) {
        ArrayList<String> s = new ArrayList<>();
        Matcher matcher = pattern.matcher(str);
        while (matcher.find()) {
            s.add(matcher.group(0));
        }
        return s;
    }

    /**
     * @params [sql 动态sql语句]
     * @desc 解析动态语句块开始和结束的位置
     */
    private ArrayList<Position> getPositions(String sql) {
        ArrayList<Position> positions = new ArrayList<>();
        ArrayList<Integer> begins = new ArrayList<>();
        ArrayList<Integer> ends = new ArrayList<>();

        Matcher matcher = this.dynamicLabelPt.matcher(sql);
        while (matcher.find()) {
            if (matcher.group(0).contains("[")) {
                begins.add(matcher.start());
            } else if (matcher.group(0).contains("]")) {
                ends.add(matcher.start());
            }
            //如果数量相同说明已经是一个最大的完整的动态语句块
            if (begins.size() == ends.size()) {
                //只获取最外层的位置
                Position position = new Position();
                position.setBegin(begins.get(0));
                position.setEnd(ends.get(begins.size() - 1));
                positions.add(position);
                begins.clear();
                ends.clear();
            }
        }
        //如果最终数量不相等，则动态语句块没有闭合
        if (begins.size() != ends.size()) {
            throw new SqlException("sql statement error: '[' is not closed");
        }
        return positions;
    }

    /**
     * @params [data 入参对象, selective 是否过滤null值]
     * @desc 解析非静态的成员属性，并且将对应的值存入threadLocal中
     */
    private List<Field> getFields(Object data) {
        if (null == data) {
            throw new SqlException("obj can not be null");
        }
        Class<?> clazz = data.getClass();
        Field[] fields = clazz.getDeclaredFields();
        if (fields.length < 1) {
            throw new SqlException("there is no field in your param");
        }
        List<Field> realFields = new ArrayList<>();
        List<Object> values = new ArrayList<>();
        for (Field field : fields) {
            boolean accessible = field.isAccessible();
            field.setAccessible(true);
            //排除静态成员
            if (Modifier.isStatic(field.getModifiers())) {
                continue;
            }
            try {
                Object obj = field.get(data);
                if (null == obj) {
                    continue;
                }
                values.add(obj);
            } catch (IllegalAccessException e) {
                throw new SqlException(e);
            }
            realFields.add(field);
            field.setAccessible(accessible);
        }
        valuesThreadLocal.set(values);
        return realFields;
    }

    /**
     * @params [fields 字段列表]
     * @desc 获取字段名
     */
    private List<String> getNames(List<Field> fields) {
        List<String> names = new ArrayList<>();
        for (Field field : fields) {
            names.add(field.getName());
        }
        return names;
    }

    /**
     * @params [entity, fields]
     * @desc 获取主键名，以第一个字段作为主键，并将对应的值存入threadLocal
     */
    private String getIdName(Object entity, List<Field> fields) {
        Field field = fields.get(0);
        try {
            boolean accessible = field.isAccessible();
            field.setAccessible(true);
            valuesThreadLocal.get().add(field.get(entity));
            field.setAccessible(accessible);
        } catch (IllegalAccessException e) {
            throw new SqlException(e);
        }
        return CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, field.getName());
    }

    /**
     * @Param [clazz 实体类对应的字节码]
     * @Desc 获取主键名
     **/
    private String getIdName(Class<?> clazz, Object id) {
        Field[] fields = clazz.getDeclaredFields();
        if (fields.length < 1) {
            throw new SqlException("there is no field in " + clazz);
        }
        for (Field field : fields) {
            //排除静态成员
            if (!Modifier.isStatic(field.getModifiers())) {
                valuesThreadLocal.get().add(id);
                return CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, field.getName());
            }
        }
        throw new SqlException("can not find primary key in " + clazz);
    }

    /**
     * @params [data 入参对象,fields 字段列表]
     * @desc 获取字段的值
     */
    private List<Object> getValues(Object data, List<Field> fields) {
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

    /**
     * @author yudm
     * @date 2021/1/1 13:16
     * @desc 用于记录动态语句块开始和结束位置的类
     */
    static class Position {
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
