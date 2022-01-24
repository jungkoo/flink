package org.apache.flink.connector.jdbc.dialect.oracle;

import org.apache.flink.connector.jdbc.converter.JdbcRowConverter;
import org.apache.flink.connector.jdbc.dialect.AbstractDialect;
import org.apache.flink.connector.jdbc.internal.converter.OracleRowConverter;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/** JDBC dialect for Oracle. */
public class OracleDialect extends AbstractDialect {

    private static final long serialVersionUID = 1L;

    // Define MAX/MIN precision of TIMESTAMP type according to Oracle docs:
    // https://www.oracletutorial.com/oracle-basics/oracle-timestamp/
    private static final int MAX_TIMESTAMP_PRECISION = 6;
    private static final int MIN_TIMESTAMP_PRECISION = 1;

    // Define MAX/MIN precision of DECIMAL type according to Oracle docs:
    // https://docs.oracle.com/cd/B19306_01/olap.102/b14346/dml_datatypes002.htm
    private static final int MAX_DECIMAL_PRECISION = 38;
    private static final int MIN_DECIMAL_PRECISION = 1;

    @Override
    public String dialectName() {
        return "Oracle";
    }

    @Override
    public JdbcRowConverter getRowConverter(RowType rowType) {
        return new OracleRowConverter(rowType);
    }

    @Override
    public String getLimitClause(long limit) {
        return "ROWNUM <= " + limit;
    }

    @Override
    public Optional<String> defaultDriverName() {
        return Optional.of("oracle.jdbc.driver.OracleDriver");
    }

    @Override
    public String quoteIdentifier(String identifier) {
        return identifier;
    }

    @Override
    public Optional<String> getUpsertStatement(
            String tableName, String[] fieldNames, String[] uniqueKeyFields) {
        final String onKey =
                Arrays.stream(uniqueKeyFields)
                        .map(f -> f + " = :" + f)
                        .collect(Collectors.joining(" AND "));
        return Optional.of(
                "MERGE INTO "
                        + quoteIdentifier(tableName)
                        + " USING dual ON ("
                        + onKey
                        + ")"
                        + " WHEN MATCHED THEN "
                        + whenMatchThenUpdateClause(fieldNames, uniqueKeyFields)
                        + " WHEN NOT MATCHED THEN "
                        + whenNotMatchedThenInsertClause(fieldNames));
    }

    private String whenMatchThenUpdateClause(String[] fieldNames, String[] uniqueKeyFields) {
        final Set<String> keyField = new HashSet(Arrays.asList(uniqueKeyFields));
        final String updateSetClause =
                Arrays.stream(fieldNames)
                        .filter(f -> !keyField.contains(f))
                        .map(f -> String.format("%s = :%s", quoteIdentifier(f), f))
                        .collect(Collectors.joining(", "));

        return "UPDATE SET " + updateSetClause;
    }

    private String whenNotMatchedThenInsertClause(String[] fieldNames) {
        final String columns =
                Arrays.stream(fieldNames)
                        .map(this::quoteIdentifier)
                        .collect(Collectors.joining(", "));
        final String placeholders =
                Arrays.stream(fieldNames).map(f -> ":" + f).collect(Collectors.joining(", "));
        return "INSERT " + "(" + columns + ")" + " VALUES (" + placeholders + ")";
    }

    @Override
    public Optional<Range> decimalPrecisionRange() {
        return Optional.of(Range.of(MIN_DECIMAL_PRECISION, MAX_DECIMAL_PRECISION));
    }

    @Override
    public Optional<Range> timestampPrecisionRange() {
        return Optional.of(Range.of(MIN_TIMESTAMP_PRECISION, MAX_TIMESTAMP_PRECISION));
    }

    @Override
    public Set<LogicalTypeRoot> supportedTypes() {
        return EnumSet.of(
                LogicalTypeRoot.CHAR,
                LogicalTypeRoot.VARCHAR,
                LogicalTypeRoot.BOOLEAN,
                LogicalTypeRoot.VARBINARY,
                LogicalTypeRoot.DECIMAL,
                LogicalTypeRoot.TINYINT,
                LogicalTypeRoot.SMALLINT,
                LogicalTypeRoot.INTEGER,
                LogicalTypeRoot.BIGINT,
                LogicalTypeRoot.FLOAT,
                LogicalTypeRoot.DOUBLE,
                LogicalTypeRoot.DATE,
                LogicalTypeRoot.TIME_WITHOUT_TIME_ZONE,
                LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE);
    }
}
