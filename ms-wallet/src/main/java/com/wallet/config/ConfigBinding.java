package com.wallet.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * ==========================================================
 * Component: ConfigBinding
 * Package: com.wallet.config   
 * Service: ms-transaction-wallet
 * Author: Sanjeev Kumar
 *
 * <p><b>Description:</b></p>
 * <p>
 * Configuration class that enables binding of externalized
 * properties (YAML/Properties) to strongly typed Java classes.
 * </p>
 *
 * <p><b>Purpose:</b></p>
 * <ul>
 *   <li>Activates Spring Boot’s configuration binding for {@code DbProperties}.</li>
 *   <li>Ensures values defined in external configuration files
 *       (e.g., <code>application-db.yml</code>) are automatically mapped
 *       to the {@code DbProperties} Java object.</li>
 * </ul>
 *
 * <p><b>How It Works:</b></p>
 * <ol>
 *   <li><code>application-db.yml</code> defines:
 *     <pre>
 *       db:
 *         datasource:
 *           url: ...
 *           username: ...
 *           password: ...
 *     </pre>
 *   </li>
 *   <li>{@code DbProperties} is annotated with:
 *     <pre>
 *       @ConfigurationProperties(prefix = "db.datasource")
 *     </pre>
 *   </li>
 *   <li>{@code ConfigBinding} registers {@code DbProperties} with the
 *       ApplicationContext via:
 *     <pre>
 *       @EnableConfigurationProperties(DbProperties.class)
 *     </pre>
 *   </li>
 *   <li>Spring Boot then:
 *     <ul>
 *       <li>Reads YAML values</li>
 *       <li>Maps them to {@code DbProperties} fields</li>
 *       <li>Exposes {@code DbProperties} as a bean</li>
 *     </ul>
 *   </li>
 * </ol>
 *
 * <p><b>Why This Class Is Needed:</b></p>
 * <ul>
 *   <li>Without it, {@code DbProperties} may not be registered as a bean.</li>
 *   <li>Configuration values may not be bound, leading to null values.</li>
 *   <li>DataSource setup may fail.</li>
 * </ul>
 * <ul>
 *   <li>With it, strongly typed configuration is enabled.</li>
 *   <li>Separation of config and business logic is maintained.</li>
 *   <li>External configs can change without code changes.</li>
 * </ul>
 *
 * <p><b>Design Principles:</b></p>
 * <ul>
 *   <li><b>Separation of Concerns:</b> Binding isolated from business logic.</li>
 *   <li><b>Externalized Configuration:</b> Environment values kept outside code.</li>
 *   <li><b>Type Safety:</b> Properties mapped to strongly typed classes.</li>
 * </ul>
 *
 * <p><b>Developer Notes:</b></p>
 * <ul>
 *   <li>Ensure {@code DbProperties} is annotated with {@code @ConfigurationProperties}.</li>
 *   <li>Ensure YAML is loaded via <code>spring.config.import</code>.</li>
 *   <li>Ensure this package is included in component scan.</li>
 *   <li>This class contains no logic — it only enables binding.</li>
 * </ul>
 *
 * <p><b>Alternative Approach:</b></p>
 * <p>
 * If {@code DbProperties} is annotated with {@code @Component},
 * this class is optional. However, using
 * {@code @EnableConfigurationProperties} is:
 * </p>
 * <ul>
 *   <li>✔ More explicit</li>
 *   <li>✔ Preferred in clean architecture</li>
 * </ul>
 * ==========================================================
 */
@Configuration
@EnableConfigurationProperties(DbProperties.class)
public class ConfigBinding {
}
