package com.injaeleewrites.showcase.pg.utility;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import java.beans.PropertyVetoException;

/**
 * @author eksor
 * @since 20150427
 */
public class DBConnectionBuilder {
    public static abstract class Builder<T extends Builder, F> {
        protected String driver;
        protected String jdbcURL;
        protected String username;
        protected String password;

        public T withDriver(String driver) {
            this.driver = driver;
            return (T)this;
        }

        public T withJDBCURL(String jdbcURL) {
            this.jdbcURL = jdbcURL;
            return (T)this;
        }

        public T withUsername(String username) {
            this.username = username;
            return (T)this;
        }

        public T withPassword(String password) {
            this.password = password;
            return (T)this;
        }

        public abstract F build();
    }

    public static class BoneCPBuilder extends Builder<BoneCPBuilder, ComboPooledDataSource> {
        protected int minConnectionPoolSize;
        protected int maxConnectionPoolSize;
        protected int acquireIncrementSize;

        public static BoneCPBuilder initialize() {
            return new BoneCPBuilder();
        }

        public BoneCPBuilder withMinConnectionPoolSize(int minSize) {
            this.minConnectionPoolSize = minSize;
            return this;
        }

        public BoneCPBuilder withMaxConnectionPoolSize(int maxSize) {
            this.maxConnectionPoolSize = maxSize;
            return this;
        }

        public BoneCPBuilder withAcquireIncrement(int incrementSize) {
            this.acquireIncrementSize = incrementSize;
            return this;
        }

        @Override
        public ComboPooledDataSource build() {
            ComboPooledDataSource ds = new ComboPooledDataSource();
            try {
                ds.setDriverClass(super.driver);
                ds.setJdbcUrl(super.jdbcURL);
                ds.setUser(super.username);
                ds.setPassword(super.password);
            } catch (PropertyVetoException e) {
                e.printStackTrace();
            }
            if (acquireIncrementSize != 0) ds.setAcquireIncrement(acquireIncrementSize);
            if (minConnectionPoolSize != 0) ds.setMinPoolSize(minConnectionPoolSize);
            if (maxConnectionPoolSize != 0) ds.setMaxPoolSize(maxConnectionPoolSize);
            return ds;
        }
    }
}
