package com.shf.sql.helper;

import java.util.List;

/**
 * description :
 * 格式化sql文
 *
 * @author songhaifeng
 * @date 2021/5/19 15:13
 */
public class SqlFormatter {

    /**
     * 合并sql文本中的多个空格为一个空格
     *
     * @param sql sql
     * @return sql
     */
    public static String combineBlank(String sql) {
        return sql.replaceAll(" +", " ");
    }

    /**
     * 剔除sql文中的特殊字符
     *
     * @param sql          sql
     * @param specialChars 特殊字符
     * @return sql
     */
    public static String trimSpecialChars(String sql, List<String> specialChars) {
        for (String chars : specialChars) {
            sql = sql.replace(chars, "");
        }
        return sql;
    }

}
