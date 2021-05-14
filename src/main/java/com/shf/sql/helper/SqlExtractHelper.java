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

    /**
     * 约定了待提取表的前缀
     */
    Set<String> TABLE_PREFIX = ImmutableSet.of("dwd_");

    /**
     * 提取当前sql文中的table和field信息
     *
     * @param sql     sql文本
     * @param sqlInfo sql上下文信息存储
     * @throws Exception e
     */
    void extractSqlInfo(String sql, SqlInfo sqlInfo) throws Exception;
}
