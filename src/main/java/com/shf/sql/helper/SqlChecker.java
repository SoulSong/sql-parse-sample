package com.shf.sql.helper;

import static com.shf.sql.helper.Constants.STAR;

/**
 * description :
 *
 * @author songhaifeng
 * @date 2021/5/14 14:07
 */
public interface SqlChecker {
    /**
     * 校验当前sql最外层是否存在limit限制
     *
     * @param sql sql文本
     * @return true表示通过；反之不通过
     * @throws Exception e
     */
    boolean withLimit(String sql) throws Exception;

    /**
     * 校验是否存在select所有列
     * Note:此处采用的简易检测模式，更具体的可查看{@link SqlInfo#isContainsSelectAllColumns()}
     *
     * @param sql sql文本
     * @return true表示选择了所有列，即存在select *操作
     */
    default boolean containsSelectAllColumns(String sql) {
        return sql.contains(STAR);
    }
}
