package com.tm.orm.silence.util;

import com.google.common.base.CaseFormat;
import com.tm.orm.silence.core.SqlBuilder;
import com.tm.orm.silence.exception.SqlException;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author yudm
 * @date 2021/7/21 14:51
 * @desc 字符串相关工具类
 */
public class StringUtil {
    //动态语句块的特殊字符
    public static final Pattern DYNAMIC_LABEL = Pattern.compile("[@&%]\\[|]");
    //if语句块中的条件
    public static final Pattern IF = Pattern.compile("&\\[.+?:");
    //if语句块的特殊字符
    public static final Pattern IF_LABEL = Pattern.compile("&\\[|:");
    //逻辑表达式
    public static final Pattern LOGIC_LABEL = Pattern.compile("&&|\\|\\|");
    //sql逻辑表达式
    public static final Pattern SQL_LOGIC_LABEL = Pattern.compile("^\\s*(?i)(and|or)");
    //运算符
    public static final Pattern OPERATOR = Pattern.compile("!=|=|>|<");
    //if和foreach语句块中的内容部分
    public static final Pattern IF_FOREACH_CONTENT = Pattern.compile(":.*]");
    //if和foreach语句块中的特殊字符
    public static final Pattern IF_FOREACH_LABEL = Pattern.compile("[:\\]]");
    //foreach语句块的属性
    public static final Pattern FOREACH_ATTR = Pattern.compile("%\\[.+?:");
    //foreach语句块的特殊字符
    public static final Pattern FOREACH_LABEL = Pattern.compile("%\\[|:");
    //参数语句块
    public static final Pattern PARAM = Pattern.compile("[#%]\\{.*?}");
    //参数语句块特殊字符
    public static final Pattern PARAM_LABEL = Pattern.compile("[#%]\\{|}");
    //预编译参数语句块
    public static final Pattern PARAM = Pattern.compile("[#%]\\{.*?}");
    //预编译参数语句块特殊字符
    public static final Pattern PARAM_LABEL = Pattern.compile("[#%]\\{|}");
    //预编译参数语句块
    public static final Pattern PRE_PARAM = Pattern.compile("#\\{.*?}");
    //预编译参数语句块特殊字符
    public static final Pattern PRE_PARAM_LABEL = Pattern.compile("#\\{|}");
    //非预编译参数语句块
    public static final Pattern NORMAL_PARAM = Pattern.compile("\\$\\{.*?}");
    //非预编译参数语句块特殊字符
    public static final Pattern NORMAL_PARAM_LABEL = Pattern.compile("\\$\\{|}");
    //单引号
    public static final Pattern SINGLY_QUOTED = Pattern.compile("'");

    /**
     * @params [str, pattern, replacement]
     * @desc 替换str中所有pattern对应的字符为replacement
     **/
    public static String replaceAll(String str, Pattern pattern, String replacement) {
        return pattern.matcher(str).replaceAll(replacement);
    }

    /**
     * @params [str, pattern, replacement]
     * @desc 替换str中第一个pattern对应的字符为replacement
     **/
    public static String replaceFirst(String str, Pattern pattern, String replacement) {
        return pattern.matcher(str).replaceFirst(replacement);
    }

    /**
     * @params [str , pattern]
     * @desc 提取出第一个匹配的字符串
     **/
    public static String extractFirst(String str, Pattern pattern) {
        Matcher matcher = pattern.matcher(str);
        if (matcher.find()) {
            return matcher.group(0);
        }
        return "";
    }

    /**
     * @params [str , pattern]
     * @desc 提取出所有匹配的字符串
     **/
    public static List<String> extractAll(String str, Pattern pattern) {
        List<String> s = new ArrayList<>();
        Matcher matcher = pattern.matcher(str);
        while (matcher.find()) {
            s.add(matcher.group(0));
        }
        return s;
    }

    /**
     * @params [str, pattern]
     * @desc 根据pattern切分str
     **/
    public static String[] split(String str, Pattern pattern) {
        return pattern.split(str, 0);
    }

    /**
     * @params [str]
     * @desc 转小写下划线
     **/
    public static String toUnderscore(String str) {
        return CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, str);
    }

    /**
     * @params [str]
     * @desc 转小写驼峰
     **/
    public static String toCamel(String str) {
        return CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, str);
    }

    /**
     * @params [str 字符串, pattern 匹配项]
     * @desc 获取pattern对应的特殊字符在str中的位置
     **/
    public static ArrayList<Position> getPositions(String str, Pattern pattern) {
        ArrayList<Position> positions = new ArrayList<>();
        ArrayList<Integer> begins = new ArrayList<>();
        ArrayList<Integer> ends = new ArrayList<>();

        Matcher matcher = pattern.matcher(str);
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
     * @author yudm
     * @date 2021/1/1 13:16
     * @desc 用于记录某字符块的开始和结束位置的类
     */
    public static class Position {
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
