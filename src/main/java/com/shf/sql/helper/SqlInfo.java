package com.shf.sql.helper;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import static com.shf.sql.helper.Constants.BR;
import static com.shf.sql.helper.Constants.RIGHT_ARROWS;
import static com.shf.sql.helper.Constants.TAB;

/**
 * description :
 *
 * @author songhaifeng
 * @date 2021/5/13 19:17
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class SqlInfo {
    private String sql;
    private boolean needExtractTables = true;
    private boolean needExtractFields = true;
    /**
     * Note: 只有在needExtractFields为true时才会进行allColumn判断
     */
    private boolean containsSelectAllColumns = false;
    private final Set<String> tables = new HashSet<>();
    private final HashMap<String, Set<String>> fields = new HashMap<>();

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(BR).append("sql :").append(BR).append(TAB).append(sql).append(BR);
        sb.append("tables:").append(BR);
        if (CollectionUtils.isNotEmpty(tables)) {
            tables.forEach(table -> sb.append(TAB).append(table).append(BR));
        }
        sb.append("fields:").append(BR);
        if (MapUtils.isNotEmpty(fields)) {
            fields.forEach((table, fieldList) -> {
                fieldList.forEach(field -> sb.append(TAB).append(table).append(RIGHT_ARROWS).append(field).append(BR));
            });
        }
        sb.append("containsSelectAllColumns:").append(BR).append(TAB).append(isContainsSelectAllColumns()).append(BR);
        return sb.toString();
    }
}
