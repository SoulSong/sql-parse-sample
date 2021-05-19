package com.shf.sql;

import com.shf.sql.helper.CalciteHelper;
import com.shf.sql.helper.SqlFormatter;
import com.shf.sql.helper.SqlInfo;
import com.shf.sql.helper.SqlParseHelper;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;

@Slf4j
public class App {
    public static void main(String[] args) throws Exception {
        log.info("***************************calciteExtractTest***************************");
        calciteExtractTest();

        log.info("***************************calciteCheckTest***************************");
        calciteCheckTest();

        log.info("***************************sqlParseExtractTest***************************");
        sqlParseExtractTest();

        log.info("***************************sqlFormatterTest***************************");
        sqlFormatterTest();
    }

    private static void calciteExtractTest() throws Exception {
        CalciteHelper calciteHelper = CalciteHelper.getInstance();

        String sql = "SELECT dwd_ans_rt.name FROM(SELECT distinct dwd_patent_ans_relation_rt.ans_id FROM (SELECT dwd_patent_cite_relation_rt.t_pdoc_id FROM dwd_patent_cite_relation_rt WHERE dwd_patent_cite_relation_rt.pdoc_id=157513407)T1 INNER JOIN dwd_patent_ans_relation_rt T2 ON T1.t_pdoc_id = dwd_patent_ans_relation_rt.pdoc_id AND dwd_patent_ans_relation_rt.type = 'current_assignee')T1 INNER JOIN dwd_ans_rt T2 ON T1.ans_id = dwd_ans_rt.ans_id";
        calciteHelper.extractSqlInfo(sql, SqlInfo.builder().sql(sql).needExtractFields(true).needExtractTables(true).build());

        sql = "SELECT distinct dwd_ans_rt.name FROM(SELECT dwd_patent_cite_relation_rt.t_pdoc_id FROM dwd_patent_cite_relation_rt WHERE dwd_patent_cite_relation_rt.pdoc_id=157513407) T1 INNER JOIN dwd_patent_ans_relation_rt T2 ON T1.t_pdoc_id = dwd_patent_ans_relation_rt.pdoc_id AND dwd_patent_ans_relation_rt.type = 'current_assignee' INNER JOIN dwd_ans_rt T3 ON dwd_patent_ans_relation_rt.ans_id = dwd_ans_rt.ans_id";
        calciteHelper.extractSqlInfo(sql, SqlInfo.builder().sql(sql).needExtractFields(true).needExtractTables(true).build());

        sql = "SELECT max_patents_ancs_id.ancs_id, max_patents_ancs_id.p_docid_num, max_patents_country.country, max_patents_country.county_count, max_patents_country.rank_in_country FROM (SELECT dwd_patent_ans_relation_rt.ans_id  ancs_id, COUNT(DISTINCT tmp_10w.pdoc_id) AS p_docid_num FROM tmp_10w, dwd_patent_ans_relation_rt WHERE tmp_10w.pdoc_id = dwd_patent_ans_relation_rt.pdoc_id and type = 'current_assignee'GROUP BY dwd_patent_ans_relation_rt.ans_id ORDER BY p_docid_num DESC LIMIT 1, 100) max_patents_ancs_id, (SELECT dwd_patent_ans_relation_rt.ans_id                                                                    ancs_id, dwd_patent_publication_rt.country                                                                    country, count(tmp_10w.pdoc_id)                                                                               county_count, row_number() over ( PARTITION BY dwd_patent_ans_relation_rt.ans_id ORDER BY count(tmp_10w.pdoc_id) DESC ) rank_in_country FROM tmp_10w, dwd_patent_ans_relation_rt, dwd_patent_publication_rt WHERE tmp_10w.pdoc_id = dwd_patent_ans_relation_rt.pdoc_id AND dwd_patent_ans_relation_rt.pdoc_id = dwd_patent_publication_rt.pdoc_id and type = 'current_assignee' GROUP BY dwd_patent_ans_relation_rt.ans_id, dwd_patent_publication_rt.country) max_patents_country WHERE max_patents_ancs_id.ancs_id = max_patents_country.ancs_id AND max_patents_country.rank_in_country <= 100 ORDER BY max_patents_ancs_id.p_docid_num DESC, max_patents_country.rank_in_country";
        calciteHelper.extractSqlInfo(sql, SqlInfo.builder().sql(sql).needExtractFields(true).needExtractTables(true).build());

        sql = "SELECT * FROM (SELECT x.one_layer_name, x.two_layer_name, COUNT(DISTINCT dwd_patent_ans_relation_rt.pdoc_id) AS patent_count FROM (SELECT dwd_patent_ans_relation_rt.ans_id AS one_layer_ans_id, dwd_patent_ans_relation_rt.`name` AS one_layer_name, dwd_patent_ans_relation_rt.ans_id AS two_layer_ans_id, dwd_patent_ans_relation_rt.`name` AS two_layer_name FROM (SELECT dwd_patent_cite_relation_rt.t_pdoc_id FROM dwd_patent_ans_relation_rt t1, dwd_patent_cite_relation_rt t2 WHERE dwd_patent_ans_relation_rt.pdoc_id = dwd_patent_cite_relation_rt.pdoc_id AND dwd_patent_ans_relation_rt.ans_id = 'A'AND dwd_patent_ans_relation_rt.type = 'current_assignee') a, dwd_patent_cite_relation_rt b, dwd_patent_ans_relation_rt c, dwd_patent_cite_relation_rt d, dwd_patent_ans_relation_rt e, (SELECT dwd_patent_cite_relation_rt.pdoc_id, dwd_patent_ans_relation_rt.ans_id FROM dwd_patent_ans_relation_rt t1, dwd_patent_cite_relation_rt t2 WHERE dwd_patent_ans_relation_rt.pdoc_id = dwd_patent_cite_relation_rt.t_pdoc_id AND dwd_patent_ans_relation_rt.ans_id = 'G' AND dwd_patent_ans_relation_rt.type = 'current_assignee') f WHERE a.t_pdoc_id = dwd_patent_cite_relation_rt.pdoc_id AND dwd_patent_cite_relation_rt.pdoc_id = dwd_patent_ans_relation_rt.pdoc_id AND dwd_patent_ans_relation_rt.ans_id <> 'A' AND dwd_patent_ans_relation_rt.ans_id <> 'G' AND dwd_patent_ans_relation_rt.type = 'current_assignee' AND dwd_patent_cite_relation_rt.t_pdoc_id = dwd_patent_cite_relation_rt.pdoc_id AND dwd_patent_cite_relation_rt.pdoc_id = dwd_patent_ans_relation_rt.pdoc_id AND dwd_patent_cite_relation_rt.pdoc_id = f.pdoc_id) x, dwd_patent_ans_relation_rt y, dwd_ans_rt z WHERE x.two_layer_ans_id = dwd_patent_ans_relation_rt.ans_id AND dwd_patent_ans_relation_rt.ans_id = dwd_ans_rt.ans_id AND dwd_ans_rt.lang = 'cn' GROUP BY x.two_layer_ans_id, x.two_layer_ans_id) k ORDER BY k.patent_count DESC LIMIT 5";
        calciteHelper.extractSqlInfo(sql, SqlInfo.builder().sql(sql).needExtractFields(true).needExtractTables(true).build());
    }

    private static void calciteCheckTest() throws Exception {
        CalciteHelper calciteHelper = CalciteHelper.getInstance();

        String sql = "select * from aa limit 5,20";
        log.info("{}", calciteHelper.withLimit(sql));
        log.info("{}", calciteHelper.containsSelectAllColumns(sql));

        sql = "SELECT distinct dwd_ans_rt.name FROM(SELECT dwd_patent_cite_relation_rt.t_pdoc_id FROM dwd_patent_cite_relation_rt WHERE dwd_patent_cite_relation_rt.pdoc_id=157513407) T1 INNER JOIN dwd_patent_ans_relation_rt T2 ON T1.t_pdoc_id = dwd_patent_ans_relation_rt.pdoc_id AND dwd_patent_ans_relation_rt.type = 'current_assignee' INNER JOIN dwd_ans_rt T3 ON dwd_patent_ans_relation_rt.ans_id = dwd_ans_rt.ans_id";
        log.info("{}", calciteHelper.withLimit(sql));
        log.info("{}", calciteHelper.containsSelectAllColumns(sql));

        sql = "SELECT max_patents_ancs_id.ancs_id, max_patents_ancs_id.p_docid_num, max_patents_country.country, max_patents_country.county_count, max_patents_country.rank_in_country FROM (SELECT dwd_patent_ans_relation_rt.ans_id  ancs_id, COUNT(DISTINCT tmp_10w.pdoc_id) AS p_docid_num FROM tmp_10w, dwd_patent_ans_relation_rt WHERE tmp_10w.pdoc_id = dwd_patent_ans_relation_rt.pdoc_id and type = 'current_assignee'GROUP BY dwd_patent_ans_relation_rt.ans_id ORDER BY p_docid_num DESC LIMIT 1, 100) max_patents_ancs_id, (SELECT dwd_patent_ans_relation_rt.ans_id                                                                    ancs_id, dwd_patent_publication_rt.country                                                                    country, count(tmp_10w.pdoc_id)                                                                               county_count, row_number() over ( PARTITION BY dwd_patent_ans_relation_rt.ans_id ORDER BY count(tmp_10w.pdoc_id) DESC ) rank_in_country FROM tmp_10w, dwd_patent_ans_relation_rt, dwd_patent_publication_rt WHERE tmp_10w.pdoc_id = dwd_patent_ans_relation_rt.pdoc_id AND dwd_patent_ans_relation_rt.pdoc_id = dwd_patent_publication_rt.pdoc_id and type = 'current_assignee' GROUP BY dwd_patent_ans_relation_rt.ans_id, dwd_patent_publication_rt.country) max_patents_country WHERE max_patents_ancs_id.ancs_id = max_patents_country.ancs_id AND max_patents_country.rank_in_country <= 100 ORDER BY max_patents_ancs_id.p_docid_num DESC, max_patents_country.rank_in_country";
        log.info("{}", calciteHelper.withLimit(sql));
        log.info("{}", calciteHelper.containsSelectAllColumns(sql));

        sql = "SELECT * FROM (SELECT x.one_layer_name, x.two_layer_name, COUNT(DISTINCT dwd_patent_ans_relation_rt.pdoc_id) AS patent_count FROM (SELECT dwd_patent_ans_relation_rt.ans_id AS one_layer_ans_id, dwd_patent_ans_relation_rt.`name` AS one_layer_name, dwd_patent_ans_relation_rt.ans_id AS two_layer_ans_id, dwd_patent_ans_relation_rt.`name` AS two_layer_name FROM (SELECT dwd_patent_cite_relation_rt.t_pdoc_id FROM dwd_patent_ans_relation_rt t1, dwd_patent_cite_relation_rt t2 WHERE dwd_patent_ans_relation_rt.pdoc_id = dwd_patent_cite_relation_rt.pdoc_id AND dwd_patent_ans_relation_rt.ans_id = 'A'AND dwd_patent_ans_relation_rt.type = 'current_assignee') a, dwd_patent_cite_relation_rt b, dwd_patent_ans_relation_rt c, dwd_patent_cite_relation_rt d, dwd_patent_ans_relation_rt e, (SELECT dwd_patent_cite_relation_rt.pdoc_id, dwd_patent_ans_relation_rt.ans_id FROM dwd_patent_ans_relation_rt t1, dwd_patent_cite_relation_rt t2 WHERE dwd_patent_ans_relation_rt.pdoc_id = dwd_patent_cite_relation_rt.t_pdoc_id AND dwd_patent_ans_relation_rt.ans_id = 'G' AND dwd_patent_ans_relation_rt.type = 'current_assignee') f WHERE a.t_pdoc_id = dwd_patent_cite_relation_rt.pdoc_id AND dwd_patent_cite_relation_rt.pdoc_id = dwd_patent_ans_relation_rt.pdoc_id AND dwd_patent_ans_relation_rt.ans_id <> 'A' AND dwd_patent_ans_relation_rt.ans_id <> 'G' AND dwd_patent_ans_relation_rt.type = 'current_assignee' AND dwd_patent_cite_relation_rt.t_pdoc_id = dwd_patent_cite_relation_rt.pdoc_id AND dwd_patent_cite_relation_rt.pdoc_id = dwd_patent_ans_relation_rt.pdoc_id AND dwd_patent_cite_relation_rt.pdoc_id = f.pdoc_id) x, dwd_patent_ans_relation_rt y, dwd_ans_rt z WHERE x.two_layer_ans_id = dwd_patent_ans_relation_rt.ans_id AND dwd_patent_ans_relation_rt.ans_id = dwd_ans_rt.ans_id AND dwd_ans_rt.lang = 'cn' GROUP BY x.two_layer_ans_id, x.two_layer_ans_id) k ORDER BY k.patent_count DESC LIMIT 5";
        log.info("{}", calciteHelper.withLimit(sql));
        log.info("{}", calciteHelper.containsSelectAllColumns(sql));
    }

    private static void sqlParseExtractTest() throws Exception {
        SqlParseHelper sqlParseHelper = SqlParseHelper.getInstance();

        String sql = "SELECT dwd_ans_rt.name FROM(SELECT distinct dwd_patent_ans_relation_rt.ans_id FROM (SELECT dwd_patent_cite_relation_rt.t_pdoc_id FROM dwd_patent_cite_relation_rt WHERE dwd_patent_cite_relation_rt.pdoc_id=157513407)T1 INNER JOIN dwd_patent_ans_relation_rt T2 ON T1.t_pdoc_id = dwd_patent_ans_relation_rt.pdoc_id AND dwd_patent_ans_relation_rt.type = 'current_assignee')T1 INNER JOIN dwd_ans_rt T2 ON T1.ans_id = dwd_ans_rt.ans_id";
        sqlParseHelper.extractSqlInfo(sql, SqlInfo.builder().sql(sql).needExtractFields(true).needExtractTables(true).build());

        sql = "SELECT distinct dwd_ans_rt.name FROM(SELECT dwd_patent_cite_relation_rt.t_pdoc_id FROM dwd_patent_cite_relation_rt WHERE dwd_patent_cite_relation_rt.pdoc_id=157513407) T1 INNER JOIN dwd_patent_ans_relation_rt T2 ON T1.t_pdoc_id = dwd_patent_ans_relation_rt.pdoc_id AND dwd_patent_ans_relation_rt.type = 'current_assignee' INNER JOIN dwd_ans_rt T3 ON dwd_patent_ans_relation_rt.ans_id = dwd_ans_rt.ans_id";
        sqlParseHelper.extractSqlInfo(sql, SqlInfo.builder().sql(sql).needExtractFields(true).needExtractTables(true).build());

        sql = "SELECT max_patents_ancs_id.ancs_id, max_patents_ancs_id.p_docid_num, max_patents_country.country, max_patents_country.county_count, max_patents_country.rank_in_country FROM (SELECT dwd_patent_ans_relation_rt.ans_id  ancs_id, COUNT(DISTINCT tmp_10w.pdoc_id) AS p_docid_num FROM tmp_10w, dwd_patent_ans_relation_rt WHERE tmp_10w.pdoc_id = dwd_patent_ans_relation_rt.pdoc_id and type = 'current_assignee'GROUP BY dwd_patent_ans_relation_rt.ans_id ORDER BY p_docid_num DESC LIMIT 1, 100) max_patents_ancs_id, (SELECT dwd_patent_ans_relation_rt.ans_id                                                                    ancs_id, dwd_patent_publication_rt.country                                                                    country, count(tmp_10w.pdoc_id)                                                                               county_count, row_number() over ( PARTITION BY dwd_patent_ans_relation_rt.ans_id ORDER BY count(tmp_10w.pdoc_id) DESC ) rank_in_country FROM tmp_10w, dwd_patent_ans_relation_rt, dwd_patent_publication_rt WHERE tmp_10w.pdoc_id = dwd_patent_ans_relation_rt.pdoc_id AND dwd_patent_ans_relation_rt.pdoc_id = dwd_patent_publication_rt.pdoc_id and type = 'current_assignee' GROUP BY dwd_patent_ans_relation_rt.ans_id, dwd_patent_publication_rt.country) max_patents_country WHERE max_patents_ancs_id.ancs_id = max_patents_country.ancs_id AND max_patents_country.rank_in_country <= 100 ORDER BY max_patents_ancs_id.p_docid_num DESC, max_patents_country.rank_in_country";
        sqlParseHelper.extractSqlInfo(sql, SqlInfo.builder().sql(sql).needExtractFields(true).needExtractTables(true).build());

        sql = "SELECT * FROM (SELECT x.one_layer_name, x.two_layer_name, COUNT(DISTINCT dwd_patent_ans_relation_rt.pdoc_id) AS patent_count FROM (SELECT dwd_patent_ans_relation_rt.ans_id AS one_layer_ans_id, dwd_patent_ans_relation_rt.`name` AS one_layer_name, dwd_patent_ans_relation_rt.ans_id AS two_layer_ans_id, dwd_patent_ans_relation_rt.`name` AS two_layer_name FROM (SELECT dwd_patent_cite_relation_rt.t_pdoc_id FROM dwd_patent_ans_relation_rt t1, dwd_patent_cite_relation_rt t2 WHERE dwd_patent_ans_relation_rt.pdoc_id = dwd_patent_cite_relation_rt.pdoc_id AND dwd_patent_ans_relation_rt.ans_id = 'A'AND dwd_patent_ans_relation_rt.type = 'current_assignee') a, dwd_patent_cite_relation_rt b, dwd_patent_ans_relation_rt c, dwd_patent_cite_relation_rt d, dwd_patent_ans_relation_rt e, (SELECT dwd_patent_cite_relation_rt.pdoc_id, dwd_patent_ans_relation_rt.ans_id FROM dwd_patent_ans_relation_rt t1, dwd_patent_cite_relation_rt t2 WHERE dwd_patent_ans_relation_rt.pdoc_id = dwd_patent_cite_relation_rt.t_pdoc_id AND dwd_patent_ans_relation_rt.ans_id = 'G' AND dwd_patent_ans_relation_rt.type = 'current_assignee') f WHERE a.t_pdoc_id = dwd_patent_cite_relation_rt.pdoc_id AND dwd_patent_cite_relation_rt.pdoc_id = dwd_patent_ans_relation_rt.pdoc_id AND dwd_patent_ans_relation_rt.ans_id <> 'A' AND dwd_patent_ans_relation_rt.ans_id <> 'G' AND dwd_patent_ans_relation_rt.type = 'current_assignee' AND dwd_patent_cite_relation_rt.t_pdoc_id = dwd_patent_cite_relation_rt.pdoc_id AND dwd_patent_cite_relation_rt.pdoc_id = dwd_patent_ans_relation_rt.pdoc_id AND dwd_patent_cite_relation_rt.pdoc_id = f.pdoc_id) x, dwd_patent_ans_relation_rt y, dwd_ans_rt z WHERE x.two_layer_ans_id = dwd_patent_ans_relation_rt.ans_id AND dwd_patent_ans_relation_rt.ans_id = dwd_ans_rt.ans_id AND dwd_ans_rt.lang = 'cn' GROUP BY x.two_layer_ans_id, x.two_layer_ans_id) k ORDER BY k.patent_count DESC LIMIT 5";
        sqlParseHelper.extractSqlInfo(sql, SqlInfo.builder().sql(sql).needExtractFields(true).needExtractTables(true).build());
    }

    private static void sqlFormatterTest() {
        String sql = "select     dwd_patent_publication_rt.patent_type,count(patent_type) as total_count from dwd_patent_publication_rt  where pdoc_id in (${ids}) group by patent_type limit 5";
        log.info(SqlFormatter.trimSpecialChars(SqlFormatter.combineBlank(sql), Arrays.asList("${", "}")));
    }

}
