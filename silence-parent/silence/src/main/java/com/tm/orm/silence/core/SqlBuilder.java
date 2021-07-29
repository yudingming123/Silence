package com.tm.orm.silence.core;

import com.tm.orm.silence.exception.SqlException;

import org.apache.commons.jexl2.Expression;
import org.apache.commons.jexl2.JexlContext;
import org.apache.commons.jexl2.JexlEngine;
import org.apache.commons.jexl2.MapContext;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.*;

import static com.tm.orm.silence.util.StringUtil.*;
import static com.tm.orm.silence.util.ReflectUtil.*;

/**
 * @author yudm
 * @date 2021/1/1 13:16
 * @desc 构建sql的类
 */
public class SqlBuilder {
    //用于执行if条件表达式
    private final JexlEngine jexlEngine = new JexlEngine();
    private final ThreadLocal<List<Object>> valuesThreadLocal;

    SqlBuilder(ThreadLocal<List<Object>> valuesThreadLocal) {
        this.valuesThreadLocal = valuesThreadLocal;
    }

    /**
     * @params [entity 实体对象, selective 是否过滤null值]
     * @desc 构建插入sql
     */
    public String buildInsertSql(Object entity) {
        return doBuildInsertSql(entity.getClass().getSimpleName(), getFieldNames(getRealFields(entity)));
    }

    /**
     * @params [entity 实体对象, selective 是否过滤null值]
     * @desc 构建批量插入sql，并且将对应的值放入threadLocal
     */
    public String buildInsertListSql(List<?> entities) {
        if (entities.size() < 1) {
            throw new SqlException("entities can not be empty");
        }
        //获取满足条件的字段，并将对应字段的值加入到threadLocal
        Object entity = entities.get(0);
        if (null == entity) {
            throw new SqlException("entity can not be empty");
        }
        List<Field> fields = getRealFields(entity);
        List<Object> valuesList = new ArrayList<>();
        //添加第一个对象中字段的值
        valuesList.add(valuesThreadLocal.get());
        //添加剩余对象中字段的值
        for (int i = 1; i < entities.size(); ++i) {
            valuesList.add(getValues(entities.get(i), fields));
        }
        valuesThreadLocal.set(valuesList);
        return doBuildInsertSql(entity.getClass().getSimpleName(), getFieldNames(fields));

    }

    /**
     * @params [entity 实体对象, selective 是否过滤null值]
     * @desc 构建通过主键更新sql
     */
    public String buildUpdateByIdSql(Object entity) {
        List<Field> fields = getRealFields(entity);
        StringBuilder sql = new StringBuilder("update ").append(toUnderscore(entity.getClass().getSimpleName())).append(" set ");
        for (String name : getFieldNames(fields)) {
            sql.append("`");
            sql.append(toUnderscore(name)).append("` = ?, ");
        }
        //去掉最后的逗号
        sql.deleteCharAt(sql.length() - 2);
        Field field = fields.get(0);
        valuesThreadLocal.get().add(getFieldValue(entity, field));
        sql.append("where `").append(toUnderscore(field.getName())).append("` = ?");
        return sql.toString();
    }

    /**
     * @params [entity 实体对象]
     * @desc 构建通过主键删除sql
     */
    public String buildDeleteByIdSql(Object entity) {
        Class<?> clazz = entity.getClass();
        StringBuilder sql = new StringBuilder("delete from ").append(toUnderscore(clazz.getSimpleName()));
        Field field = getIdField(clazz);
        valuesThreadLocal.get().add(getFieldValue(entity, field));
        sql.append("where `").append(toUnderscore(field.getName())).append("` = ?");
        return sql.toString();
    }

    /**
     * @params [clazz 实体类的字节码，用于映射表和获取主键名, id 主键值]
     * @desc 构建通过主键查询sql
     **/
    public String buildSelectByIdSql(Class<?> clazz, Object id) {
        StringBuilder sql = new StringBuilder("select * from ").append(toUnderscore(clazz.getSimpleName()));
        Field field = getIdField(clazz);
        valuesThreadLocal.get().add(id);
        sql.append("where `").append(toUnderscore(field.getName())).append("` = ?");
        return sql.toString();
    }

    /**
     * @params [sql 动态sql语句, data 参数]
     * @desc 根据动态sql和参数构建出最终的sql
     **/
    public String build(String sql, Object data) {
        Map<String, Object> param = toMap(data, extractAll(sql, PARAM));
        //用于查找动态语句块的开始和结束位置
        return doBuild(sql, param);
    }

    /**
     * @params [tableName 表名, names 字段名]
     * @desc 真正执行构建插入sql
     */
    private String doBuildInsertSql(String tableName, List<String> names) {
        StringBuilder sql = new StringBuilder("insert into ").append(toUnderscore(tableName)).append(" (");
        for (String name : names) {
            sql.append("`");
            sql.append(toUnderscore(name)).append("`,");
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
     * @params [sql 动态sql语句, param map参数]
     * @desc 执行根据动态sql和参数构建出最终的sql
     **/
    private String doBuild(String sql, Map<String, Object> param) {
        ArrayList<Position> positions = getPositions(sql, DYNAMIC_LABEL);
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
                if (ds.startsWith("&[")) {
                    sb.append(ifBlock(ds, param));
                }
                //如果是where语句块
                if (ds.startsWith("@[")) {
                    sb.append(whereBlock(ds, param));
                }
                //如果是foreach语句块
                if (ds.startsWith("%[")) {
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
     * @params [sql 动态sql语句, param map参数]
     * @desc 解析包含参数的语句块
     **/
    private String paramBlock(String sql, Map<String, Object> param) {
        if (sql.contains("#{")) {
            List<String> ks = extractAll(sql, PRE_PARAM);
            for (String k : ks) {
                //加入到sql参数列表中
                valuesThreadLocal.get().add(getValueFromMap(param, replaceAll(k, PRE_PARAM_LABEL, "").trim()));
            }
            sql = replaceAll(sql, PRE_PARAM, "?");
        } else if (sql.contains("${")) {
            List<String> ks = extractAll(sql, NORMAL_PARAM);
            for (String t : ks) {
                Object obj = getValueFromMap(param, replaceAll(t, NORMAL_PARAM_LABEL, "").trim());
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
     * @params [sql 动态sql语句, param map参数]
     * @desc 解析if语句块
     **/
    private String ifBlock(String ds, Map<String, Object> param) {
        //if中的条件表达式
        String condition = extractFirst(ds, IF);
        if (null == condition || condition.isEmpty()) {
            throw new SqlException("sql statement error: there is no conditions in &[]");
        }
        //截取真正的条件表达式
        condition = replaceAll(replaceAll(condition, IF_LABEL, ""), SINGLY_QUOTED, "\"");
        Expression expression = jexlEngine.createExpression(condition);
        JexlContext jexlContext = new MapContext();
        //条件表达式中的变量名
        String[] ks = split(condition, LOGIC_LABEL);
        for (String k : ks) {
            //真正的变量名字
            k = split(k, OPERATOR)[0].trim();
            jexlContext.set(k, getValueFromMap(param, k));
        }
        //如果表达式成立
        if ((boolean) expression.evaluate(jexlContext)) {
            return doBuild(replaceAll(extractFirst(ds, IF_FOREACH_CONTENT), IF_FOREACH_LABEL, ""), param);
        }
        return "";
    }

    /**
     * @params [sql 动态sql语句, param map参数]
     * @desc 解析where语句块
     **/
    private String whereBlock(String ds, Map<String, Object> param) {
        String w = doBuild(ds.substring(2, ds.length() - 1), param);
        if (w.isEmpty()) {
            return "";
        }
        //把多余的and或or去掉
        w = replaceFirst(w, SQL_LOGIC_LABEL, " ");
        return "where" + w;
    }

    /**
     * @params [sql 动态sql语句, param map参数]
     * @desc 解析foreach语句块
     **/
    private String foreachBlock(String ds, Map<String, Object> param) {
        //获取属性
        String attribute = replaceAll(extractFirst(ds, FOREACH_ATTR), FOREACH_LABEL, "");
        String[] attributes = attribute.split(",");
        //将属性和属性的值转化成map
        Map<String, String> attributeMap = new HashMap<>(5);
        for (String a : attributes) {
            String[] kv = a.split("=");
            if (kv.length < 2) {
                throw new SqlException("bad foreach statement");
            }
            attributeMap.put(kv[0].trim(), kv[1].trim());
        }
        //o:开始符号，c:结束符号，s:分隔符，i:每一项的名字，v:集合变量的名字
        if (!attributeMap.keySet().containsAll(Arrays.asList("o", "c", "s", "i", "v"))) {
            throw new SqlException("Attribute [o,c,s,i,v] is necessary");
        }
        //获取集合
        String k = attributeMap.get("v");
        Object v = getValueFromMap(param, k);
        if (!(v instanceof Collection<?>)) {
            throw new SqlException(k + " is not a collection");
        }
        //开始拼接foreach语句块
        StringBuilder sb = new StringBuilder();
        sb.append(attributeMap.get("o"));
        for (Object obj : (Collection<?>) v) {
            param.put(attributeMap.get("i"), obj);
            sb.append(doBuild(replaceAll(extractFirst(ds, IF_FOREACH_CONTENT), IF_FOREACH_LABEL, ""), param));
            sb.append(attributeMap.get("s"));
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.append(attributeMap.get("c"));
        sb.append(" ");
        return sb.toString();
    }

    /**
     * @params [entity 实体类]
     * @desc 获取非静态、非null的字段列表
     **/
    private List<Field> getRealFields(Object entity) {
        Class<?> clazz = entity.getClass();
        Field[] fields = clazz.getDeclaredFields();
        if (fields.length < 1) {
            throw new SqlException("there is no field in data");
        }
        List<Field> realFields = new ArrayList<>();
        List<Object> values = new ArrayList<>();
        for (Field field : fields) {
            //排除静态成员
            if (Modifier.isStatic(field.getModifiers())) {
                continue;
            }
            Object obj = getFieldValue(entity, field);
            if (null == obj) {
                continue;
            }
            values.add(obj);
            realFields.add(field);
        }
        valuesThreadLocal.set(values);
        return realFields;
    }
}
