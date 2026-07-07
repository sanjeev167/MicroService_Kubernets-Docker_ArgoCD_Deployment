package com.orchestrator.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import lombok.Data;

/**
 * ==========================================================
 * Component: DbProperties
 * Package: com.orchestrator.config
 * Service: ms-transaction-orchestrator
 * Author: Sanjeev Kumar
 *
 * <p><b>Description:</b></p>
 * <p>
 * Strongly typed configuration class for database connection
 * properties. Binds externalized values from YAML/Properties
 * files into Java fields using Spring Boot’s
 * {@code @ConfigurationProperties}.
 * </p>
 *
 * <p><b>Purpose:</b></p>
 * <ul>
 *   <li>Encapsulates database connection details such as
 *       URL, username, password, and driver class name.</li>
 *   <li>Provides a nested {@code Pool} class for connection
 *       pool configuration (max size, min idle, idle timeout).</li>
 *   <li>Ensures type safety and clean separation of config
 *       from business logic.</li>
 * </ul>
 *
 * <p><b>How It Works:</b></p>
 * <ol>
 *   <li><code>application-db.yml</code> defines:
 *     <pre>
 *       db:
 *         datasource:
 *           url: jdbc:postgresql://localhost:5432/mydb
 *           username: user
 *           password: pass
 *           driverClassName: org.postgresql.Driver
 *           pool:
 *             maxSize: 20
 *             minIdle: 5
 *             idleTimeout: 30000
 *     </pre>
 *   </li>
 *   <li>{@code DbProperties} is annotated with:
 *     <pre>
 *       @ConfigurationProperties(prefix = "db.datasource")
 *     </pre>
 *   </li>
 *   <li>Spring Boot automatically maps YAML values to
 *       {@code DbProperties} fields and exposes it as a bean.</li>
 * </ol>
 *
 * <p><b>Why This Class Is Needed:</b></p>
 * <ul>
 *   <li>Centralizes database configuration in one place.</li>
 *   <li>Supports environment‑specific overrides without code changes.</li>
 *   <li>Provides type safety compared to raw property lookups.</li>
 * </ul>
 *
 * <p><b>Design Principles:</b></p>
 * <ul>
 *   <li><b>Separation of Concerns:</b> Configuration isolated from logic.</li>
 *   <li><b>Externalized Configuration:</b> Values defined outside code.</li>
 *   <li><b>Type Safety:</b> Properties mapped to typed fields.</li>
 * </ul>
 *
 * <p><b>Developer Notes:</b></p>
 * <ul>
 *   <li>Ensure {@code application-db.yml} is included in the config import.</li>
 *   <li>Ensure {@code ConfigBinding} registers this class via
 *       {@code @EnableConfigurationProperties(DbProperties.class)}.</li>
 *   <li>Use Lombok’s {@code @Data} for getters/setters.</li>
 * </ul>
 *
 * <p><b>Alternative Approach:</b></p>
 * <p>
 * If annotated with {@code @Component}, this class can be
 * discovered automatically. However, explicit registration
 * via {@code @EnableConfigurationProperties} is preferred
 * for clean architecture.
 * </p>
 * ==========================================================
 */
@Component
@ConfigurationProperties(prefix = "db.datasource")
@Data
public class DbProperties {

    private String url;
    private String username;
    private String password;
    private String driverClassName;

    private Pool pool = new Pool();

    /**
     * Nested configuration class for connection pool settings.
     * Provides strongly typed fields for pool size, idle count,
     * and timeout values.
     */
    @Data
    public static class Pool {
        private int maxSize;
        private int minIdle;
        private long idleTimeout;
    }
}
