package org.apache.flink.connector.jdbc.dialect.oracle;

import org.apache.flink.connector.jdbc.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.dialect.JdbcDialectFactory;

/** Factory for {@link OracleDialect}. */
public class OracleDialectFactory implements JdbcDialectFactory {
    @Override
    public boolean acceptsURL(String url) {
        return url.startsWith("jdbc:oracle:");
    }

    @Override
    public JdbcDialect create() {
        return new OracleDialect();
    }
}
