package com.shf.sql.helper;

import com.google.common.collect.ImmutableSet;

import java.util.Set;

/**
 * description :
 *
 * @author songhaifeng
 * @date 2021/5/14 13:45
 */
public interface SqlExtractHelper {

   Set<String> TABLE_PREFIX = ImmutableSet.of("dwd_");

    /**
     * 提取当前sql文中的table和field信息
     *
     * @param sql
     * @param sqlInfo
     * @throws Exception
     */
    void extractSqlInfo(String sql, SqlInfo sqlInfo) throws Exception;
}
