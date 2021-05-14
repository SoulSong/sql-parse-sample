package com.shf.sql.helper;

import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.util.SourceStringReader;

import java.util.HashSet;

import static com.shf.sql.helper.Constants.STAR;

/**
 * description :
 * 整个解析过程是深度优先遍历
 *
 * @author songhaifeng
 * @date 2021/5/12 19:26
 */
@Slf4j
public class CalciteHelper implements SqlExtractHelper, SqlChecker {
    private static final SqlParser.Config CONFIG = SqlParser.config()
            .withParserFactory(SqlParserImpl.FACTORY)
            .withQuoting(Quoting.DOUBLE_QUOTE)
            .withUnquotedCasing(Casing.TO_LOWER)
            .withQuotedCasing(Casing.UNCHANGED)
            .withConformance(SqlConformanceEnum.MYSQL_5)
            .withLex(Lex.MYSQL);

    private CalciteHelper() {
    }

    public static CalciteHelper getInstance() {
        return CalciteHelperHolder.calciteHelper;
    }

    private static class CalciteHelperHolder {
        private static CalciteHelper calciteHelper = new CalciteHelper();
    }

    @Override
    public boolean withLimit(String sql) throws Exception {
        return withLimit(sql, CONFIG);
    }

    public boolean withLimit(String sql, SqlParser.Config config) throws Exception {
        SqlParser sqlParser = SqlParser.create(new SourceStringReader(sql), config);
        SqlNode sqlNode = sqlParser.parseQuery();
        SqlKind kind = sqlNode.getKind();
        switch (kind) {
            case SELECT:
                return false;
            case ORDER_BY:
                SqlOrderBy sqlOrderBy = (SqlOrderBy) sqlNode;
                if (sqlOrderBy.fetch == null) {
                    return false;
                }
                log.debug("{}", ((SqlNumericLiteral) sqlOrderBy.fetch).bigDecimalValue());
                return true;
            default:
        }
        return false;
    }

    @Override
    public void extractSqlInfo(String sql, SqlInfo sqlInfo) throws Exception {
        extractSqlInfo(sql, CONFIG, sqlInfo);
    }

    public void extractSqlInfo(String sql, SqlParser.Config config, SqlInfo sqlInfo) throws SqlParseException {
        SqlParser sqlParser = SqlParser.create(new SourceStringReader(sql), config);
        SqlNode sqlNode = sqlParser.parseQuery();
        handlerSQL(sqlNode, sqlInfo);
        log.info(sqlInfo.toString());
    }

    private static void handlerSQL(SqlNode sqlNode, SqlInfo sqlInfo) {
        SqlKind kind = sqlNode.getKind();
        switch (kind) {
            case SELECT:
                handlerSelect(sqlNode, sqlInfo);
                break;
            case UNION:
                ((SqlBasicCall) sqlNode).getOperandList().forEach(node -> {
                    handlerSQL(node, sqlInfo);
                });
                break;
            case JOIN:
                handlerFrom(sqlNode, sqlInfo);
                break;
            case ORDER_BY:
                handlerOrderBy(sqlNode, sqlInfo);
                break;
            case AS:
                handlerFrom(sqlNode, sqlInfo);
                break;
            case IDENTIFIER:
                handlerFrom(sqlNode, sqlInfo);
                break;
            default:
                break;
        }
    }

    private static void handlerOrderBy(SqlNode node, SqlInfo sqlInfo) {
        SqlOrderBy sqlOrderBy = (SqlOrderBy) node;
        SqlNode query = sqlOrderBy.query;
        handlerSQL(query, sqlInfo);
        SqlNodeList orderList = sqlOrderBy.orderList;
        handlerField(orderList, sqlInfo);
    }

    private static void handlerSelect(SqlNode select, SqlInfo sqlInfo) {
        SqlSelect sqlSelect = (SqlSelect) select;

        handlerFrom(sqlSelect.getFrom(), sqlInfo);

        SqlNodeList selectList = sqlSelect.getSelectList();
        selectList.getList().forEach(list -> {
            handlerField(list, sqlInfo);
        });

        if (sqlSelect.hasWhere()) {
            handlerField(sqlSelect.getWhere(), sqlInfo);
        }

        if (sqlSelect.hasOrderBy()) {
            handlerField(sqlSelect.getOrderList(), sqlInfo);
        }

        SqlNodeList group = sqlSelect.getGroup();
        if (group != null) {
            group.forEach(groupField -> {
                handlerField(groupField, sqlInfo);
            });
        }
    }

    private static void handlerFrom(SqlNode from, SqlInfo sqlInfo) {
        if (!sqlInfo.isNeedExtractTables()) {
            return;
        }
        SqlKind kind = from.getKind();

        switch (kind) {
            case IDENTIFIER:
                SqlIdentifier sqlIdentifier = (SqlIdentifier) from;
                log.debug("table name --> {}", sqlIdentifier.toString());
                if (TABLE_PREFIX.stream().anyMatch(sqlIdentifier.toString()::startsWith)) {
                    sqlInfo.getTables().add(sqlIdentifier.toString());
                }
                break;
            case AS:
                SqlBasicCall sqlBasicCall = (SqlBasicCall) from;
                SqlNode selectNode = sqlBasicCall.getOperandList().get(0);
                handlerSQL(selectNode, sqlInfo);
                break;
            case JOIN:
                SqlJoin sqlJoin = (SqlJoin) from;
                SqlNode left = sqlJoin.getLeft();
                handlerSQL(left, sqlInfo);
                SqlNode right = sqlJoin.getRight();
                handlerSQL(right, sqlInfo);
                // 当表连接采用类似select * from a,b where a.id = b.cid时，此处condition为null
                SqlNode condition = sqlJoin.getCondition();
                handlerField(condition, sqlInfo);
                break;
            case SELECT:
                handlerSQL(from, sqlInfo);
                break;
            default:
                break;
        }
    }

    private static void handlerField(SqlNode field, SqlInfo sqlInfo) {
        if (field == null || !sqlInfo.isNeedExtractFields()) {
            return;
        }
        SqlKind kind = field.getKind();
        switch (kind) {
            case AS:
                SqlNode[] operands_as = ((SqlBasicCall) field).operands;
                SqlNode left_as = operands_as[0];
                handlerField(left_as, sqlInfo);
                break;
            case IDENTIFIER:
                SqlIdentifier sqlIdentifier = (SqlIdentifier) field;
                String fullFiledName = sqlIdentifier.toString();
                log.debug("field name --> {}", sqlIdentifier.toString());
                if (STAR.equalsIgnoreCase(fullFiledName)) {
                    sqlInfo.setContainsSelectAllColumns(true);
                }
                String[] splitName = fullFiledName.split("\\.");
                if (splitName.length == 2 && TABLE_PREFIX.stream().anyMatch(fullFiledName::startsWith)) {
                    String tableName = splitName[0];
                    String fieldName = splitName[1];
                    sqlInfo.getFields().computeIfAbsent(tableName, tmp -> new HashSet<>()).add(fieldName);
                }
                break;
            default:
                if (field instanceof SqlBasicCall) {
                    SqlNode[] nodes = ((SqlBasicCall) field).operands;
                    for (SqlNode node : nodes) {
                        handlerField(node, sqlInfo);
                    }
                }
                if (field instanceof SqlNodeList) {
                    ((SqlNodeList) field).getList().forEach(node -> {
                        handlerField(node, sqlInfo);
                    });
                }
                break;
        }
    }

}
