# STS Environment Guide

## MSIMPL - Microservices Platform

**Version:** 1.0.0
**Author:** Sanjeev Kumar
**Created On:** 03-Jul-2026
**Last Modified:** 03-Jul-2026

---

# 1. Introduction

This document explains how to set up and use the **Spring Tool Suite (STS)** development environment for the **MSIMPL (Microservices Platform)** project.

The objective of this environment is to allow developers to run Spring Boot microservices directly from STS while running only the required infrastructure services inside Docker.

This approach provides:

* Fast development cycle
* Easy debugging
* Simple configuration management
* Environment-independent application code
* Smooth transition to Kubernetes and cloud deployments

---

# 2. Development Architecture

```
                 Spring Tool Suite (STS)
        +--------------------------------------+
        |                                      |
        |  ms-auth-server                      |
        |  ms-user                             |
        |  ms-wallet                           |
        |  ms-notification                     |
        |  ms-transaction-orchestrator         |
        |  ...                                |
        +-------------------+------------------+
                            |
                            |
        ----------------------------------------------
                            |
         +------------------+------------------+
         |                                     |
         |                                     |
         ▼                                     ▼
+----------------------+          +----------------------+
| PostgreSQL           |          | Apache Kafka         |
| Windows Host         |          | Docker Container     |
+----------------------+          +----------------------+
                                              |
                                              ▼
                                      +------------------+
                                      | Kafka UI         |
                                      | Docker Container |
                                      +------------------+
```

---

# 3. Infrastructure Strategy

The local development environment is divided into two independent parts.

## Spring Boot Applications

All Spring Boot microservices run directly from **Spring Tool Suite (STS)**.

Examples:

* ms-auth-server
* ms-user
* ms-wallet
* ms-notification
* ms-transaction-orchestrator

Running applications from STS provides:

* Interactive debugging
* Breakpoints
* Live log monitoring
* Faster code changes
* No Docker image rebuild during development

---

## Docker Infrastructure

Only shared infrastructure components run inside Docker.

Current infrastructure includes:

| Service      | Runtime |
| ------------ | ------- |
| Apache Kafka | Docker  |
| Kafka UI     | Docker  |

---

## Windows Host Services

Some services execute directly on the Windows host machine.

Current service:

| Service    | Runtime      |
| ---------- | ------------ |
| PostgreSQL | Windows Host |

---

## Separate Observability Stack

Observability components are maintained independently from the application infrastructure.

They include:

* Prometheus
* Grafana
* Loki
* Promtail

These components are started using the dedicated **Observability Docker Compose** configuration.

---

# 4. Directory Structure

```
infra/
└── sts/
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

# 5. Configuration Strategy

Infrastructure configuration is externalized into dedicated environment files.

Example:

```
env/
    kafka.env
    postgres.env
```

Benefits include:

* Centralized configuration
* Easier maintenance
* Environment-specific values
* Cleaner Docker Compose files
* Easier migration to Kubernetes ConfigMaps and Secrets

---

# 6. Docker Network

All Docker containers communicate using a shared Docker bridge network.

```
infra_msimpl-network
```

Spring Boot applications running from STS connect to Docker services using published host ports.

Example:

| Service  | Host            |
| -------- | --------------- |
| Kafka    | localhost:29092 |
| Kafka UI | localhost:8085  |

---

# 7. Starting the Environment

Navigate to:

```
infra/sts/infrastructure
```

Start the infrastructure.

```bash
docker compose up -d
```

Verify running containers.

```bash
docker ps
```

Expected containers:

* kafka
* kafka-ui

---

# 8. Starting PostgreSQL

PostgreSQL runs directly on the Windows host machine.

Verify:

* PostgreSQL service is running.
* Database exists.
* Username and password are correct.

Example:

```
Database : wallet_db
Username : postgres
Password : postgres
Port     : 5432
```

---

# 9. Starting Spring Boot Services

Open the project in Spring Tool Suite.

Start the required microservices.

Example order:

1. Config Server
2. Discovery Server
3. Gateway Server
4. Auth Server
5. User Service
6. Wallet Service
7. Notification Service
8. Transaction Orchestrator

---

# 10. Kafka Connectivity

Applications running from STS connect using:

```
localhost:29092
```

Docker containers communicate internally using:

```
kafka:9092
```

This separation allows both Docker containers and STS applications to communicate with the same Kafka broker.

---

# 11. Kafka UI

Kafka UI is available at:

```
http://localhost:8085
```

Use Kafka UI to:

* View topics
* Monitor consumers
* Browse messages
* Verify broker health

---

# 12. Development Workflow

```
Modify Code
      │
      ▼
Run From STS
      │
      ▼
Connect To Kafka
      │
      ▼
Process Messages
      │
      ▼
Verify Using Kafka UI
```

No Docker image rebuild is required during normal application development.

---

# 13. Migration to Kubernetes

The application configuration has been designed to support future Kubernetes deployment without changing application code.

The deployment workflow will be:

```
Spring Boot Project
        │
        ▼
Build JAR
        │
        ▼
Build Docker Image
        │
        ▼
Push Image
        │
        ▼
Deploy To Kubernetes
```

Environment-specific values will be supplied through Kubernetes ConfigMaps and Secrets.

---

# 14. Design Principles

The STS environment follows the following principles:

* Infrastructure separated from application code
* Environment-specific infrastructure configuration
* Externalized configuration
* Environment-independent application code
* Reusable deployment strategy
* Same application artifact across environments
* No application rebuild between environments
* Kubernetes-ready configuration

---

# 15. Troubleshooting

## Kafka not reachable

Verify:

* Docker containers are running.
* Kafka container is healthy.
* Port **29092** is available.
* Application uses **localhost:29092**.

---

## Kafka UI unavailable

Verify:

* Kafka UI container is running.
* Port **8085** is available.
* Kafka broker is healthy.

---

## Database connection failure

Verify:

* PostgreSQL service is running.
* Database exists.
* Username and password are correct.
* Port **5432** is available.

---

## Docker Network Missing

Verify that the shared Docker network exists.

```bash
docker network ls
```

Expected network:

```
infra_msimpl-network
```

---

# 16. Summary

The STS development environment is designed to provide a fast, developer-friendly workflow by combining Spring Tool Suite with Docker-based infrastructure services.

This architecture enables developers to:

* Run Spring Boot applications directly from STS
* Use Docker for shared infrastructure
* Maintain environment-independent application code
* Simplify local development
* Prepare applications for Kubernetes deployment without changing application logic

This approach establishes a consistent development workflow that can be extended to Docker, Kubernetes, and AWS deployments while preserving the same application artifact and configuration strategy.
