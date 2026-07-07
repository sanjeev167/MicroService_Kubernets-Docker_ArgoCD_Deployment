# STS Infrastructure

## MSIMPL - Microservices Platform

This directory contains the infrastructure required to develop and test the **MSIMPL (Microservices Platform)** using **Spring Tool Suite (STS)**.

In this development model, Spring Boot microservices run directly from STS, while shared infrastructure components execute inside Docker containers.

---

# Directory Structure

```text
sts/
├── infrastructure/
│   ├── docker-compose.yml
│   └── env/
│       ├── kafka.env
│       └── postgres.env
│
├── observability/
│   ├── docker-compose.yml
│   ├── grafana/
│   ├── loki/
│   ├── prometheus/
│   └── promtail/
│
├── docs/
│   └── STS-Environment-Guide.md
│
└── README.md
```

---

# Infrastructure Components

The STS infrastructure is divided into two independent stacks.

## Infrastructure Stack

Location:

```text
infrastructure/
```

Provides the shared services required by Spring Boot applications running from STS.

Current services include:

* Apache Kafka (KRaft Mode)
* Kafka UI

Configuration is externalized using dedicated environment files located in the `env` directory.

Start the infrastructure stack:

```bash
docker compose up -d
```

---

## Observability Stack

Location:

```text
observability/
```

Provides centralized monitoring and logging services.

Current services include:

* Prometheus
* Grafana
* Loki
* Promtail

This stack can be started independently whenever monitoring or log analysis is required.

---

# Application Runtime

Spring Boot microservices are **not** executed inside Docker during local development.

Applications are started directly from **Spring Tool Suite (STS)**, providing:

* Interactive debugging
* Breakpoint support
* Faster development cycle
* Immediate code changes
* Simplified troubleshooting

---

# Database

PostgreSQL runs directly on the Windows host machine and is accessed by Spring Boot applications running from STS.

Database configuration remains externalized through application configuration files.

---

# Configuration Strategy

Infrastructure configuration is centralized using dedicated environment files.

Example:

```text
env/
├── kafka.env
└── postgres.env
```

This approach provides:

* Centralized configuration management
* Cleaner Docker Compose files
* Environment-specific configuration
* Simplified maintenance
* Easy migration to Kubernetes ConfigMaps and Secrets

---

# Development Workflow

```text
Spring Boot Code
        │
        ▼
Run from STS
        │
        ▼
Connect to Docker Infrastructure
        │
        ▼
Verify Using Kafka UI
        │
        ▼
(Optional)
Start Observability Stack
```

---

# Future Deployment

The local STS environment follows the same configuration strategy that will be used in future deployment environments.

Planned deployment targets include:

* Docker
* Kubernetes
* AWS EKS

Because infrastructure configuration is externalized, the same application artifact can be deployed across environments without modifying application code.

---

# Documentation

For complete setup instructions, architecture details, configuration strategy, troubleshooting, and development workflow, refer to:

```text
docs/STS-Environment-Guide.md
```

---

# Design Principles

The STS infrastructure follows these principles:

* Infrastructure separated from application code
* Externalized configuration
* Environment-independent application code
* Reusable deployment strategy
* Same application artifact across environments
* No application rebuild between environments
* Kubernetes-ready configuration

---

# Version

| Property    | Value                           |
| ----------- | ------------------------------- |
| Project     | MSIMPL - Microservices Platform |
| Environment | Spring Tool Suite (STS)         |
| Version     | 1.0.0                           |
| Author      | Sanjeev Kumar                   |
