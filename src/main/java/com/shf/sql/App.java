package com.shf.sql;

import com.alibaba.druid.DbType;
import com.shf.sql.helper.SqlChecker;
import com.shf.sql.helper.SqlExtractHelper;
import com.shf.sql.helper.SqlInfo;
import com.shf.sql.helper.calcite.CalciteHelper;
import com.shf.sql.helper.druid.DruidHelper;
import com.shf.sql.helper.druid.FormatHelper;
import com.shf.sql.helper.sqlparse.SqlParseHelper;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class App {
    public static void main(String[] args) throws Exception {
        log.info("***************************calciteExtractTest***************************");
        CalciteHelper calciteHelper = CalciteHelper.getInstance();
        sqlExtractHelperTest(calciteHelper);

        log.info("***************************calciteCheckTest***************************");
        sqlCheckerTest(calciteHelper);

        log.info("***************************sqlParseExtractTest***************************");
        SqlParseHelper sqlParseHelper = SqlParseHelper.getInstance();
        sqlExtractHelperTest(sqlParseHelper);

        log.info("***************************druidExtractTest***************************");
        DruidHelper druidHelper = DruidHelper.getInstance();
        sqlExtractHelperTest(druidHelper);

        log.info("***************************druidCheckTest***************************");
        sqlCheckerTest(druidHelper);

        log.info("***************************sqlFormatterTest***************************");
        sqlFormatterTest(druidHelper);
    }

    private static void sqlExtractHelperTest(SqlExtractHelper sqlExtractHelper) throws Exception {
        String sql = "SELECT dwd_ans_rt.name FROM(SELECT distinct dwd_patent_ans_relation_rt.ans_id FROM (SELECT dwd_patent_cite_relation_rt.t_pdoc_id FROM `dwd_patent_cite_relation_rt` WHERE dwd_patent_cite_relation_rt.pdoc_id=157513407 and dwd_patent_cite_relation_rt.aaa='111')T1 INNER JOIN dwd_patent_ans_relation_rt T2 ON T1.t_pdoc_id = dwd_patent_ans_relation_rt.pdoc_id AND dwd_patent_ans_relation_rt.type = 'current_assignee')T1 INNER JOIN dwd_ans_rt T2 ON T1.ans_id = dwd_ans_rt.ans_id";
        log.info(sqlExtractHelper.extractSqlInfo(sql, SqlInfo.builder().sql(sql).needExtractFields(true).needExtractTables(true).build()).toString());

        sql = "SELECT distinct dwd_ans_rt.name FROM(SELECT dwd_patent_cite_relation_rt.t_pdoc_id FROM dwd_patent_cite_relation_rt WHERE dwd_patent_cite_relation_rt.pdoc_id=157513407) T1 INNER JOIN dwd_patent_ans_relation_rt T2 ON T1.t_pdoc_id = dwd_patent_ans_relation_rt.pdoc_id AND dwd_patent_ans_relation_rt.type = 'current_assignee' INNER JOIN dwd_ans_rt T3 ON dwd_patent_ans_relation_rt.ans_id = dwd_ans_rt.ans_id";
        log.info(sqlExtractHelper.extractSqlInfo(sql, SqlInfo.builder().sql(sql).needExtractFields(true).needExtractTables(true).build()).toString());

        sql = "SELECT max_patents_ancs_id.ancs_id, max_patents_ancs_id.p_docid_num, max_patents_country.country, max_patents_country.county_count, max_patents_country.rank_in_country FROM (SELECT dwd_patent_ans_relation_rt.ans_id  ancs_id, COUNT(DISTINCT tmp_10w.pdoc_id) AS p_docid_num FROM tmp_10w, dwd_patent_ans_relation_rt WHERE tmp_10w.pdoc_id = dwd_patent_ans_relation_rt.pdoc_id and type = 'current_assignee'GROUP BY dwd_patent_ans_relation_rt.ans_id ORDER BY p_docid_num DESC LIMIT 1, 100) max_patents_ancs_id, (SELECT dwd_patent_ans_relation_rt.ans_id                                                                    ancs_id, dwd_patent_publication_rt.country                                                                    country, count(tmp_10w.pdoc_id)                                                                               county_count, row_number() over ( PARTITION BY dwd_patent_ans_relation_rt.ans_id ORDER BY count(tmp_10w.pdoc_id) DESC ) rank_in_country FROM tmp_10w, dwd_patent_ans_relation_rt, dwd_patent_publication_rt WHERE tmp_10w.pdoc_id = dwd_patent_ans_relation_rt.pdoc_id AND dwd_patent_ans_relation_rt.pdoc_id = dwd_patent_publication_rt.pdoc_id and type = 'current_assignee' GROUP BY dwd_patent_ans_relation_rt.ans_id, dwd_patent_publication_rt.country) max_patents_country WHERE max_patents_ancs_id.ancs_id = max_patents_country.ancs_id AND max_patents_country.rank_in_country <= 100 ORDER BY max_patents_ancs_id.p_docid_num DESC, max_patents_country.rank_in_country";
        log.info(sqlExtractHelper.extractSqlInfo(sql, SqlInfo.builder().sql(sql).needExtractFields(true).needExtractTables(true).build()).toString());

        sql = "SELECT * FROM (SELECT x.one_layer_name, x.two_layer_name, COUNT(DISTINCT dwd_patent_ans_relation_rt.pdoc_id) AS patent_count FROM (SELECT dwd_patent_ans_relation_rt.ans_id AS one_layer_ans_id, dwd_patent_ans_relation_rt.`name` AS one_layer_name, dwd_patent_ans_relation_rt.ans_id AS two_layer_ans_id, dwd_patent_ans_relation_rt.`name` AS two_layer_name FROM (SELECT dwd_patent_cite_relation_rt.t_pdoc_id FROM dwd_patent_ans_relation_rt t1, dwd_patent_cite_relation_rt t2 WHERE dwd_patent_ans_relation_rt.pdoc_id = dwd_patent_cite_relation_rt.pdoc_id AND dwd_patent_ans_relation_rt.ans_id = 'A'AND dwd_patent_ans_relation_rt.type = 'current_assignee') a, dwd_patent_cite_relation_rt b, dwd_patent_ans_relation_rt c, dwd_patent_cite_relation_rt d, dwd_patent_ans_relation_rt e, (SELECT dwd_patent_cite_relation_rt.pdoc_id, dwd_patent_ans_relation_rt.ans_id FROM dwd_patent_ans_relation_rt t1, dwd_patent_cite_relation_rt t2 WHERE dwd_patent_ans_relation_rt.pdoc_id = dwd_patent_cite_relation_rt.t_pdoc_id AND dwd_patent_ans_relation_rt.ans_id = 'G' AND dwd_patent_ans_relation_rt.type = 'current_assignee') f WHERE a.t_pdoc_id = dwd_patent_cite_relation_rt.pdoc_id AND dwd_patent_cite_relation_rt.pdoc_id = dwd_patent_ans_relation_rt.pdoc_id AND dwd_patent_ans_relation_rt.ans_id <> 'A' AND dwd_patent_ans_relation_rt.ans_id <> 'G' AND dwd_patent_ans_relation_rt.type = 'current_assignee' AND dwd_patent_cite_relation_rt.t_pdoc_id = dwd_patent_cite_relation_rt.pdoc_id AND dwd_patent_cite_relation_rt.pdoc_id = dwd_patent_ans_relation_rt.pdoc_id AND dwd_patent_cite_relation_rt.pdoc_id = f.pdoc_id) x, dwd_patent_ans_relation_rt y, dwd_ans_rt z WHERE x.two_layer_ans_id = dwd_patent_ans_relation_rt.ans_id AND dwd_patent_ans_relation_rt.ans_id = dwd_ans_rt.ans_id AND dwd_ans_rt.lang = 'cn' GROUP BY x.two_layer_ans_id, x.two_layer_ans_id) k ORDER BY k.patent_count DESC LIMIT 5";
        log.info(sqlExtractHelper.extractSqlInfo(sql, SqlInfo.builder().sql(sql).needExtractFields(true).needExtractTables(true).build()).toString());
    }

    private static void sqlCheckerTest(SqlChecker sqlChecker) throws Exception {
        String sql = "select * from `111ruhe`  limit 5,20";
        log.info("{}", sqlChecker.withLimit(sql));
        log.info("{}", sqlChecker.containsSelectAllColumns(sql));

        sql = "SELECT distinct dwd_ans_rt.name FROM(SELECT dwd_patent_cite_relation_rt.t_pdoc_id FROM dwd_patent_cite_relation_rt WHERE dwd_patent_cite_relation_rt.pdoc_id=157513407) T1 INNER JOIN dwd_patent_ans_relation_rt T2 ON T1.t_pdoc_id = dwd_patent_ans_relation_rt.pdoc_id AND dwd_patent_ans_relation_rt.type = 'current_assignee' INNER JOIN dwd_ans_rt T3 ON dwd_patent_ans_relation_rt.ans_id = dwd_ans_rt.ans_id";
        log.info("{}", sqlChecker.withLimit(sql));
        log.info("{}", sqlChecker.containsSelectAllColumns(sql));

        sql = "SELECT max_patents_ancs_id.ancs_id, max_patents_ancs_id.p_docid_num, max_patents_country.country, max_patents_country.county_count, max_patents_country.rank_in_country FROM (SELECT dwd_patent_ans_relation_rt.ans_id  ancs_id, COUNT(DISTINCT tmp_10w.pdoc_id) AS p_docid_num FROM tmp_10w, dwd_patent_ans_relation_rt WHERE tmp_10w.pdoc_id = dwd_patent_ans_relation_rt.pdoc_id and type = 'current_assignee'GROUP BY dwd_patent_ans_relation_rt.ans_id ORDER BY p_docid_num DESC LIMIT 1, 100) max_patents_ancs_id, (SELECT dwd_patent_ans_relation_rt.ans_id                                                                    ancs_id, dwd_patent_publication_rt.country                                                                    country, count(tmp_10w.pdoc_id)                                                                               county_count, row_number() over ( PARTITION BY dwd_patent_ans_relation_rt.ans_id ORDER BY count(tmp_10w.pdoc_id) DESC ) rank_in_country FROM tmp_10w, dwd_patent_ans_relation_rt, dwd_patent_publication_rt WHERE tmp_10w.pdoc_id = dwd_patent_ans_relation_rt.pdoc_id AND dwd_patent_ans_relation_rt.pdoc_id = dwd_patent_publication_rt.pdoc_id and type = 'current_assignee' GROUP BY dwd_patent_ans_relation_rt.ans_id, dwd_patent_publication_rt.country) max_patents_country WHERE max_patents_ancs_id.ancs_id = max_patents_country.ancs_id AND max_patents_country.rank_in_country <= 100 ORDER BY max_patents_ancs_id.p_docid_num DESC, max_patents_country.rank_in_country";
        log.info("{}", sqlChecker.withLimit(sql));
        log.info("{}", sqlChecker.containsSelectAllColumns(sql));

        sql = "SELECT * FROM (SELECT x.one_layer_name, x.two_layer_name, COUNT(DISTINCT dwd_patent_ans_relation_rt.pdoc_id) AS patent_count FROM (SELECT dwd_patent_ans_relation_rt.ans_id AS one_layer_ans_id, dwd_patent_ans_relation_rt.`name` AS one_layer_name, dwd_patent_ans_relation_rt.ans_id AS two_layer_ans_id, dwd_patent_ans_relation_rt.`name` AS two_layer_name FROM (SELECT dwd_patent_cite_relation_rt.t_pdoc_id FROM dwd_patent_ans_relation_rt t1, dwd_patent_cite_relation_rt t2 WHERE dwd_patent_ans_relation_rt.pdoc_id = dwd_patent_cite_relation_rt.pdoc_id AND dwd_patent_ans_relation_rt.ans_id = 'A'AND dwd_patent_ans_relation_rt.type = 'current_assignee') a, dwd_patent_cite_relation_rt b, dwd_patent_ans_relation_rt c, dwd_patent_cite_relation_rt d, dwd_patent_ans_relation_rt e, (SELECT dwd_patent_cite_relation_rt.pdoc_id, dwd_patent_ans_relation_rt.ans_id FROM dwd_patent_ans_relation_rt t1, dwd_patent_cite_relation_rt t2 WHERE dwd_patent_ans_relation_rt.pdoc_id = dwd_patent_cite_relation_rt.t_pdoc_id AND dwd_patent_ans_relation_rt.ans_id = 'G' AND dwd_patent_ans_relation_rt.type = 'current_assignee') f WHERE a.t_pdoc_id = dwd_patent_cite_relation_rt.pdoc_id AND dwd_patent_cite_relation_rt.pdoc_id = dwd_patent_ans_relation_rt.pdoc_id AND dwd_patent_ans_relation_rt.ans_id <> 'A' AND dwd_patent_ans_relation_rt.ans_id <> 'G' AND dwd_patent_ans_relation_rt.type = 'current_assignee' AND dwd_patent_cite_relation_rt.t_pdoc_id = dwd_patent_cite_relation_rt.pdoc_id AND dwd_patent_cite_relation_rt.pdoc_id = dwd_patent_ans_relation_rt.pdoc_id AND dwd_patent_cite_relation_rt.pdoc_id = f.pdoc_id) x, dwd_patent_ans_relation_rt y, dwd_ans_rt z WHERE x.two_layer_ans_id = dwd_patent_ans_relation_rt.ans_id AND dwd_patent_ans_relation_rt.ans_id = dwd_ans_rt.ans_id AND dwd_ans_rt.lang = 'cn' GROUP BY x.two_layer_ans_id, x.two_layer_ans_id) k ORDER BY k.patent_count DESC LIMIT 5";
        log.info("{}", sqlChecker.withLimit(sql));
        log.info("{}", sqlChecker.containsSelectAllColumns(sql));
    }

    private static void sqlFormatterTest(FormatHelper formatHelper) {
        String sql = "SELEcT     dwd_patent_publication_rt.patent_type,count(patent_type) as total_count from dwd_patent_publication_rt  where pdoc_id in (${ids}) group by patent_type limit 5";
        log.info(formatHelper.formatParameters(sql, DbType.mysql));
    }


}
