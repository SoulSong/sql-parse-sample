package com.shf.sql.helper.sqlparse;

import com.shf.sql.helper.SqlExtractHelper;
import com.shf.sql.helper.SqlInfo;
import lombok.extern.slf4j.Slf4j;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.AnalyticExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.ComparisonOperator;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.Statements;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.GroupByElement;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.util.TablesNamesFinder;
import org.apache.commons.collections4.CollectionUtils;

import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author songhaifeng
 */
@Slf4j
public class SqlParseHelper implements SqlExtractHelper {
    private SqlParseHelper() {
    }

    private static class SqlParseHelperHolder {
        private static SqlParseHelper sqlParseHelper = new SqlParseHelper();
    }

    public static SqlParseHelper getInstance() {
        return SqlParseHelperHolder.sqlParseHelper;
    }

    @Override
    public SqlInfo extractSqlInfo(String sql, SqlInfo sqlInfo) throws Exception {
        return extractSqlInfo(sql, true, sqlInfo);
    }

    public SqlInfo extractSqlInfo(String sql, boolean isSingle, SqlInfo sqlInfo) throws JSQLParserException, ParseException {
        if (isSingle) {
            handlerStatement(parseSingleSql(sql), sqlInfo);
        } else {
            List<Statement> statements = parseMultiSql1(sql);
            statements.forEach(statement -> {
                handlerStatement(statement, sqlInfo);
            });
        }
        return sqlInfo;
    }

    private void handlerStatement(Statement statement, SqlInfo sqlInfo) {
        if (statement instanceof Select) {
            handlerSelect((Select) statement, sqlInfo);
        } else {
            log.warn("No handler for statement instanceOf {}", statement.getClass().getSimpleName());
        }
    }

    private void handlerSelect(Select selectStatement, SqlInfo sqlInfo) {
        if (sqlInfo.isNeedExtractTables()) {
            TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
            sqlInfo.getTables().addAll(tablesNamesFinder.getTableList(selectStatement)
                    .stream().filter(tableName -> TABLE_PREFIX.stream().anyMatch(tableName::startsWith)).collect(Collectors.toSet()));
        }

        if (sqlInfo.isNeedExtractFields()) {
            handlerSelectBody(selectStatement.getSelectBody(), sqlInfo);
        }

    }

    private void handlerSelectBody(SelectBody selectBody, SqlInfo sqlInfo) {
        if (selectBody instanceof PlainSelect) {
            handlerPlainSelect((PlainSelect) selectBody, sqlInfo);
        } else {
            log.warn("No handler for selectBody instanceOf {}", selectBody.getClass().getSimpleName());
        }
    }

    private void handlerPlainSelect(PlainSelect plainSelect, SqlInfo sqlInfo) {
        List<SelectItem> selectItemList = plainSelect.getSelectItems();
        if (CollectionUtils.isNotEmpty(selectItemList)) {
            selectItemList.forEach(selectItem -> {
                handlerSelectItem(selectItem, sqlInfo);
            });
        }

        List<Join> joinList = plainSelect.getJoins();
        if (CollectionUtils.isNotEmpty(joinList)) {
            joinList.forEach(join -> {
                handlerJoin(join, sqlInfo);
            });
        }

        FromItem fromItem = plainSelect.getFromItem();
        if (fromItem != null) {
            handlerFromItem(fromItem, sqlInfo);
        }

        Expression where = plainSelect.getWhere();
        if (where != null) {
            handlerExpression(where, sqlInfo);
        }

        List<OrderByElement> orderByElementList = plainSelect.getOrderByElements();
        if (CollectionUtils.isNotEmpty(orderByElementList)) {
            orderByElementList.forEach(orderByElement -> {
                handlerOrderByElement(orderByElement, sqlInfo);
            });
        }

        GroupByElement groupByElement = plainSelect.getGroupBy();
        if (groupByElement != null) {
            handlerGroupByElement(groupByElement, sqlInfo);
        }
    }

    private void handlerSelectItem(SelectItem selectItem, SqlInfo sqlInfo) {
        if (selectItem instanceof SelectExpressionItem) {
            SelectExpressionItem selectExpressionItem = (SelectExpressionItem) selectItem;
            handlerExpression(selectExpressionItem.getExpression(), sqlInfo);
        } else if (selectItem instanceof AllColumns) {
            handlerAllColumns((AllColumns) selectItem, sqlInfo);
        } else {
            log.warn("No handler for selectItem instanceOf {}", selectItem.getClass().getSimpleName());
        }
    }

    private void handlerExpression(Expression expression, SqlInfo sqlInfo) {
        if (expression instanceof Column) {
            handlerColumn((Column) expression, sqlInfo);
        } else if (expression instanceof ComparisonOperator) {
            handlerComparisonOperator((ComparisonOperator) expression, sqlInfo);
        } else if (expression instanceof AndExpression) {
            handlerAndExpression((AndExpression) expression, sqlInfo);
        } else if (expression instanceof AnalyticExpression) {
            handlerAnalyticExpression((AnalyticExpression) expression, sqlInfo);
        } else if (expression instanceof Function) {
            handlerFunction((Function) expression, sqlInfo);
        } else if (expression instanceof StringValue || expression instanceof LongValue) {
            nops();
        } else {
            log.warn("No handler for expression instanceOf {}", expression.getClass().getSimpleName());
        }
    }

    private void handlerColumn(Column column, SqlInfo sqlInfo) {
        Table table = column.getTable();
        if (table == null) {
            return;
        }
        String tableName = table.getName();
        if (TABLE_PREFIX.stream().anyMatch(tableName::startsWith)) {
            sqlInfo.getFields().computeIfAbsent(tableName, tmp -> new HashSet<>()).add(column.getColumnName());
        }
    }

    private void handlerAllColumns(AllColumns allColumns, SqlInfo sqlInfo) {
        sqlInfo.setContainsSelectAllColumns(true);
    }

    private void handlerComparisonOperator(ComparisonOperator comparisonOperator, SqlInfo sqlInfo) {
        handlerExpression(comparisonOperator.getLeftExpression(), sqlInfo);
        handlerExpression(comparisonOperator.getRightExpression(), sqlInfo);
    }

    private void handlerAndExpression(AndExpression andExpression, SqlInfo sqlInfo) {
        handlerExpression(andExpression.getLeftExpression(), sqlInfo);
        handlerExpression(andExpression.getRightExpression(), sqlInfo);
    }

    private void handlerAnalyticExpression(AnalyticExpression analyticExpression, SqlInfo sqlInfo) {
        List<OrderByElement> orderByElementList = analyticExpression.getOrderByElements();
        if (CollectionUtils.isNotEmpty(orderByElementList)) {
            orderByElementList.forEach(orderByElement -> {
                handlerOrderByElement(orderByElement, sqlInfo);
            });
        }

        handlerExpressionList(analyticExpression.getPartitionExpressionList(), sqlInfo);

        Expression filterExpression = analyticExpression.getFilterExpression();
        if (filterExpression != null) {
            handlerExpression(filterExpression, sqlInfo);
        }
    }

    private void handlerFunction(Function functionExpression, SqlInfo sqlInfo) {
        handlerExpressionList(functionExpression.getParameters(), sqlInfo);
    }

    private void handlerExpressionList(ExpressionList expressionList, SqlInfo sqlInfo) {
        if (expressionList != null) {
            List<Expression> expressions = expressionList.getExpressions();
            if (CollectionUtils.isNotEmpty(expressions)) {
                expressions.forEach(expression -> {
                    handlerExpression(expression, sqlInfo);
                });
            }
        }
    }

    private void handlerJoin(Join join, SqlInfo sqlInfo) {
        if (join.getOnExpression() != null) {
            handlerExpression(join.getOnExpression(), sqlInfo);
        }
        if (join.getRightItem() != null) {
            handlerFromItem(join.getRightItem(), sqlInfo);
        }
    }

    private void handlerFromItem(FromItem fromItem, SqlInfo sqlInfo) {
        if (fromItem instanceof SubSelect) {
            handlerSubSelect((SubSelect) fromItem, sqlInfo);
        } else if (fromItem instanceof Table) {
            handlerTable((Table) fromItem, sqlInfo);
        } else {
            log.warn("No handler for fromItem instanceOf {}", fromItem.getClass().getSimpleName());
        }
    }

    private void handlerSubSelect(SubSelect subSelect, SqlInfo sqlInfo) {
        handlerSelectBody(subSelect.getSelectBody(), sqlInfo);
    }

    private void handlerTable(Table table, SqlInfo sqlInfo) {
        nops();
    }

    private void handlerOrderByElement(OrderByElement orderByElement, SqlInfo sqlInfo) {
        handlerExpression(orderByElement.getExpression(), sqlInfo);
    }

    private void handlerGroupByElement(GroupByElement groupByElement, SqlInfo sqlInfo) {
        List<Expression> expressionList = groupByElement.getGroupByExpressions();
        if (CollectionUtils.isNotEmpty(expressionList)) {
            expressionList.forEach(expression -> {
                handlerExpression(expression, sqlInfo);
            });
        }
    }

    private void nops() {
    }

    private Statement parseSingleSql(String sql) throws JSQLParserException {
        return CCJSqlParserUtil.parse(sql);
    }

    private List<Statement> parseMultiSql1(String sqls) throws JSQLParserException {
        return CCJSqlParserUtil.parseStatements(sqls).getStatements();
    }

    private List<Statement> parseMultiSql2(String sqls) throws ParseException {
        CCJSqlParser ccjSqlParser = new CCJSqlParser(sqls);
        Statements statements = ccjSqlParser.Statements();
        return statements.getStatements();
    }
}
