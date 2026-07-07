package com.notification.config;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.orm.jpa.vendor.HibernateJpaVendorAdapter;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import javax.sql.DataSource;
import java.util.Properties;

/**
 * ==========================================================
 * Component: DataSourceConfig
 * Package: com.notification.config
 * Service: ms-transaction-notification
 * Author: Sanjeev Kumar
 *
 * <p><b>Description:</b></p>
 * <p>
 * Consolidated Spring configuration class that defines:
 * <ul>
 *   <li>{@link DataSource} bean using HikariCP</li>
 *   <li>{@link LocalContainerEntityManagerFactoryBean} for JPA persistence</li>
 *   <li>{@link JpaTransactionManager} for transaction management</li>
 * </ul>
 * </p>
 *
 * <p><b>Purpose:</b></p>
 * <ul>
 *   <li>Centralizes persistence layer configuration in one file.</li>
 *   <li>Ensures database connection, entity scanning, and transaction
 *       management are consistently wired.</li>
 *   <li>Provides explicit control over JPA and Hibernate settings.</li>
 * </ul>
 *
 * <p><b>Developer Notes:</b></p>
 * <ul>
 *   <li>Entities must be placed under <code>com.notification.entity</code>.</li>
 *   <li>Repositories must be scanned via <code>@EnableJpaRepositories</code>
 *       (can be added here or in a separate config).</li>
 *   <li>Adjust Hibernate properties (dialect, ddl-auto) as per environment.</li>
 * </ul>
 * ==========================================================
 */
@Configuration
@EnableTransactionManagement
public class DataSourceConfig {

    private final DbProperties dbProperties;

    public DataSourceConfig(DbProperties dbProperties) {
        this.dbProperties = dbProperties;
    }

    /**
     * Defines the {@link DataSource} bean using HikariCP.
     */
    @Bean
    public DataSource dataSource() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(dbProperties.getUrl());
        config.setUsername(dbProperties.getUsername());
        config.setPassword(dbProperties.getPassword());
        config.setDriverClassName(dbProperties.getDriverClassName());

        config.setMaximumPoolSize(dbProperties.getPool().getMaxSize());
        config.setMinimumIdle(dbProperties.getPool().getMinIdle());
        config.setIdleTimeout(dbProperties.getPool().getIdleTimeout());

        return new HikariDataSource(config);
    }

    /**
     * Defines the JPA EntityManagerFactory bean.
     */
    @Bean
    public LocalContainerEntityManagerFactoryBean entityManagerFactory(DataSource dataSource) {
        LocalContainerEntityManagerFactoryBean emf = new LocalContainerEntityManagerFactoryBean();
        emf.setDataSource(dataSource);
        emf.setPackagesToScan("com.notification.entity"); // adjust to your entity package
        emf.setJpaVendorAdapter(new HibernateJpaVendorAdapter());

        Properties props = new Properties();
        props.setProperty("hibernate.hbm2ddl.auto", "update"); // or validate/create-drop
        props.setProperty("hibernate.dialect", "org.hibernate.dialect.PostgreSQLDialect");
        props.setProperty("hibernate.show_sql", "true");
        emf.setJpaProperties(props);

        return emf;
    }

    /**
     * Defines the JPA TransactionManager bean.
     */
    @Bean
    public JpaTransactionManager transactionManager(LocalContainerEntityManagerFactoryBean emf) {
        return new JpaTransactionManager(emf.getObject());
    }
}
