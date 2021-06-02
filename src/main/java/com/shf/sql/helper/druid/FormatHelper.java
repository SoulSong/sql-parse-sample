package com.shf.sql.helper.druid;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.visitor.ParameterizedOutputVisitorUtils;

/**
 * description :
 *
 * @author songhaifeng
 * @date 2021/6/2 17:19
 */
public interface FormatHelper {

    /**
     * https://github.com/alibaba/druid/wiki/ParameterizedOutputVisitor
     *
     * @param sql
     * @param dbType
     * @return
     */
    default String formatParameters(String sql, DbType dbType) {
        return ParameterizedOutputVisitorUtils.parameterize(sql, dbType);
    }

}
