package com.shf.sql;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLCommentHint;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLJoinTableSource;
import com.alibaba.druid.sql.ast.statement.SQLSelect;
import com.alibaba.druid.sql.ast.statement.SQLSelectQuery;
import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.visitor.SQLASTOutputVisitor;
import com.alibaba.druid.util.JdbcConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

/**
 * description :
 * Add hint by druid.
 *
 * @author songhaifeng
 * @date 2021/10/15 9:49
 */
@Slf4j
public class HintTest {

    @Test
    public void hint() {
        // SELECT /*+ READ_FROM_STORAGE(TIFLASH[test1.t1,test2.t2]) */ t1.a FROM test1.t t1, test2.t t2 WHERE t1.a = t2.a;
        String sql = "SELECT  t1.a FROM test1.t t1, test2.t t2 WHERE t1.a = t2.a ";
        List<SQLStatement> statementList = SQLUtils.parseStatements(sql, JdbcConstants.MYSQL);
        SQLSelectStatement sqlSelectStatement = (SQLSelectStatement) statementList.get(0);

        List<SQLObject> sqlObjects = sqlSelectStatement.getChildren();
        if (CollectionUtils.isNotEmpty(sqlObjects)) {
            SQLObject sqlObject = sqlObjects.get(0);
            if (sqlObject instanceof SQLSelect) {
                SQLSelectQuery sqlSelectQuery = ((SQLSelect) sqlObject).getQuery();
                SQLSelectQueryBlock sqlSelectQueryBlock = ((SQLSelectQueryBlock) sqlSelectQuery);
                SQLJoinTableSource sqlJoinTableSource = (SQLJoinTableSource) sqlSelectQueryBlock.getFrom();
                String leftTable = getFullAliasName((SQLExprTableSource) sqlJoinTableSource.getLeft());
                String rightTable = getFullAliasName((SQLExprTableSource) sqlJoinTableSource.getRight());
                SQLCommentHint sqlCommentHint = new SQLCommentHint();
                sqlCommentHint.setText("+ READ_FROM_STORAGE(TIFLASH[" + leftTable + "," + rightTable + "])");
                sqlSelectQueryBlock.setHint(sqlCommentHint);
                sqlSelectQueryBlock.setHints(Collections.singletonList(sqlCommentHint));
            }
        }

        log.info(sqlSelectStatement.toString());

        StringBuilder out = new StringBuilder();
        sqlSelectStatement.accept(new SQLASTOutputVisitor(out, true));
        String newSQL = out.toString();
        log.info(newSQL);
    }

    private String getFullAliasName(SQLExprTableSource sqlTableSource) {
        String aliasName = sqlTableSource.getAlias();
        SQLPropertyExpr sqlPropertyExpr = (SQLPropertyExpr) sqlTableSource.getExpr();
        String database = sqlPropertyExpr.getOwnerName();
        return database + "." + aliasName;
    }
}
