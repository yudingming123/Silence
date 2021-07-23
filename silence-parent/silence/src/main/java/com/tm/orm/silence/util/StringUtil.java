package com.tm.orm.silence.util;


import com.google.common.base.CaseFormat;

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

    public static String replaceAll(String str, Pattern pattern, String replacement) {
        return pattern.matcher(str).replaceAll(replacement);
    }

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
}
