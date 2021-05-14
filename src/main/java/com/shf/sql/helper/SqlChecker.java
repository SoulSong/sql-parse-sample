package com.shf.sql.helper;

import org.apache.calcite.sql.parser.SqlParser;

/**
 * description :
 *
 * @author songhaifeng
 * @date 2021/5/14 14:07
 */
public interface SqlChecker {
    boolean withLimit(String sql) throws Exception;

    default boolean selectAllColumn(String sql) {
        return sql.indexOf('*') < 0;
    }
}
