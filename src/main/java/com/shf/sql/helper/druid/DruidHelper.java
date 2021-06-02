package com.shf.sql.helper.druid;

import com.alibaba.druid.DbType;
import com.alibaba.druid.proxy.jdbc.DataSourceProxy;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLLimit;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.statement.SQLSelect;
import com.alibaba.druid.sql.ast.statement.SQLSelectQuery;
import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.visitor.SchemaStatVisitor;
import com.alibaba.druid.util.JdbcConstants;
import com.alibaba.druid.wall.Violation;
import com.alibaba.druid.wall.WallCheckResult;
import com.alibaba.druid.wall.WallConfig;
import com.alibaba.druid.wall.WallProvider;
import com.alibaba.druid.wall.spi.MySqlWallProvider;
import com.alibaba.druid.wall.spi.OracleWallProvider;
import com.alibaba.druid.wall.spi.PGWallProvider;
import com.alibaba.druid.wall.spi.SQLServerWallProvider;
import com.shf.sql.helper.SqlChecker;
import com.shf.sql.helper.SqlExtractHelper;
import com.shf.sql.helper.SqlInfo;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;

import java.util.HashSet;
import java.util.List;

/**
 * description :
 * https://github.com/alibaba/druid/wiki/SQL-Parser
 *
 * @author songhaifeng
 * @date 2021/6/2 16:17
 */
@Slf4j
public class DruidHelper implements SqlExtractHelper, FormatHelper, SqlChecker {
    private final static DbType DEFAULT_DB_TYPE = JdbcConstants.MYSQL;
    private final static WallProvider MYSQL_PROVIDER;
    private final static WallProvider ORACLE_PROVIDER;
    private final static WallProvider SQL_SERVER_PROVIDER;
    private final static WallProvider PG_PROVIDER;

    static {
        WallConfig mysqlWallConfig = new WallConfig(MySqlWallProvider.DEFAULT_CONFIG_DIR);
        mysqlWallConfig.setSelectAllColumnAllow(false);
        MYSQL_PROVIDER = new MySqlWallProvider(mysqlWallConfig);

        WallConfig oracleWallConfig = new WallConfig(OracleWallProvider.DEFAULT_CONFIG_DIR);
        oracleWallConfig.setSelectAllColumnAllow(false);
        ORACLE_PROVIDER = new OracleWallProvider(oracleWallConfig);

        WallConfig sqlServerWallConfig = new WallConfig(SQLServerWallProvider.DEFAULT_CONFIG_DIR);
        sqlServerWallConfig.setSelectAllColumnAllow(false);
        SQL_SERVER_PROVIDER = new SQLServerWallProvider(sqlServerWallConfig);

        WallConfig pgWallConfig = new WallConfig(PGWallProvider.DEFAULT_CONFIG_DIR);
        pgWallConfig.setSelectAllColumnAllow(false);
        PG_PROVIDER = new PGWallProvider(pgWallConfig);
    }

    private DruidHelper() {
    }

    private static class DruidHelperHolder {
        private static final DruidHelper DRUID_HELPER = new DruidHelper();
    }

    public static DruidHelper getInstance() {
        return DruidHelperHolder.DRUID_HELPER;
    }

    @Override
    public SqlInfo extractSqlInfo(String sql, SqlInfo sqlInfo) throws Exception {
        return extractSqlInfo(sql, DEFAULT_DB_TYPE, sqlInfo);
    }

    public SqlInfo extractSqlInfo(String sql, DbType dbType, SqlInfo sqlInfo) throws Exception {
        List<SQLStatement> statementList = SQLUtils.parseStatements(sql, dbType);
        statementList.forEach(statement -> handleStatement(statement, dbType, sqlInfo));
        sqlInfo.setContainsSelectAllColumns(containsSelectAllColumns(checkSql(sql, dbType)));
        return sqlInfo;
    }

    /**
     * https://github.com/alibaba/druid/wiki/SchemaStatVisitor
     *
     * @param sqlStatement
     * @param dbType
     * @param sqlInfo
     */
    private void handleStatement(SQLStatement sqlStatement, DbType dbType, SqlInfo sqlInfo) {
        handleSqlSelectStatement((SQLSelectStatement) sqlStatement, dbType, sqlInfo);
    }

    private void handleSqlSelectStatement(SQLSelectStatement sqlSelectStatement, DbType dbType, SqlInfo sqlInfo) {
        SchemaStatVisitor statVisitor = SQLUtils.createSchemaStatVisitor(dbType);
        sqlSelectStatement.accept(statVisitor);
        if (sqlInfo.isNeedExtractFields()) {
            // getColumns contain orderColumns,groupColumns,conditionColumns and relationShip.
            statVisitor.getColumns().forEach(column -> {
                if (TABLE_PREFIX.stream().anyMatch(prefix -> column.getTable().startsWith(prefix))) {
                    sqlInfo.getFields().computeIfAbsent(column.getTable(), tmp -> new HashSet<>()).add(column.getName());
                }
            });
        }
        // statVisitor.getTables() contains table status. Here only needs tableNames.
        if (sqlInfo.isNeedExtractTables()) {
            statVisitor.getOriginalTables().forEach(sqlName -> {
                if (TABLE_PREFIX.stream().anyMatch(prefix -> sqlName.getSimpleName().startsWith(prefix))) {
                    sqlInfo.getTables().add(sqlName.getSimpleName());
                }
            });
        }
    }

    /**
     * 目前仅检查select语句的最外层是否包含limit约束，使用默认的Mysql作为语法解析类型
     *
     * @param sql sql
     * @return true 表示包含；反之不包含
     * @throws Exception e
     */
    @Override
    public boolean withLimit(String sql) throws Exception {
        return withLimit(sql, DEFAULT_DB_TYPE);
    }

    /**
     * 目前仅检查select语句的最外层是否包含limit约束
     *
     * @param sql    sql
     * @param dbType 数据库类型
     * @return true 表示包含；反之不包含
     * @throws Exception e
     */
    public boolean withLimit(String sql, DbType dbType) throws Exception {
        List<SQLStatement> statementList = SQLUtils.parseStatements(sql, dbType);
        return withLimit(statementList.get(0));
    }

    /**
     * 目前仅检查select语句的最外层是否包含limit约束
     *
     * @param sqlStatement sqlStatement
     * @return true 表示包含；反之不包含
     * @throws Exception e
     */
    private boolean withLimit(SQLStatement sqlStatement) throws Exception {
        List<SQLObject> sqlObjects = sqlStatement.getChildren();
        if (CollectionUtils.isNotEmpty(sqlObjects)) {
            SQLObject sqlObject = sqlObjects.get(0);
            if (sqlObject instanceof SQLSelect) {
                SQLSelectQuery sqlSelectQuery = ((SQLSelect) sqlObject).getQuery();
                SQLLimit sqlLimit = ((SQLSelectQueryBlock) sqlSelectQuery).getLimit();
                if (sqlLimit != null) {
                    log.debug("start offset : {} ;rowNumber : {}", sqlLimit.getOffset() == null ? 0 : ((SQLIntegerExpr) sqlLimit.getOffset()).getNumber(), ((SQLIntegerExpr) sqlLimit.getRowCount()).getNumber());
                    return true;
                } else {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public boolean containsSelectAllColumns(String sql) {
        return containsSelectAllColumns(sql, DEFAULT_DB_TYPE);
    }

    public boolean containsSelectAllColumns(String sql, DbType dbType) {
        return containsSelectAllColumns(checkSql(sql, dbType));
    }

    private boolean containsSelectAllColumns(WallCheckResult checkResult) {
        for (Violation violation : checkResult.getViolations()) {
            if (1002 == (violation.getErrorCode())) {
                return true;
            }
        }
        return false;
    }

    /**
     * {@link com.alibaba.druid.wall.WallFilter#init(DataSourceProxy)}
     * refer : https://github.com/alibaba/druid/wiki/%E9%85%8D%E7%BD%AE-wallfilter
     *
     * @return WallCheckResult
     */
    private WallCheckResult checkSql(String sql, DbType dbType) {
        WallProvider provider;
        switch (dbType) {
            case mysql:
            case mariadb:
            case presto:
                provider = MYSQL_PROVIDER;
                break;
            case oracle:
            case ali_oracle:
            case oceanbase_oracle:
                provider = ORACLE_PROVIDER;
                break;
            case sqlserver:
            case jtds:
                provider = SQL_SERVER_PROVIDER;
                break;
            case postgresql:
            case edb:
            case polardb:
                provider = PG_PROVIDER;
                break;
            default:
                throw new IllegalStateException("dbType not support : " + dbType + ".");
        }
        return provider.check(sql);
    }

}
