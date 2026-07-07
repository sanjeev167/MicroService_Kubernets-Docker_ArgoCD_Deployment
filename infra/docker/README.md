# Docker Infrastructure

## MSIMPL - Microservices Platform

This directory contains the complete Docker deployment environment for the **MSIMPL (Microservices Platform)**.

Unlike the STS environment, all Spring Boot microservices execute inside Docker containers along with the shared infrastructure and observability platform.

The Docker environment provides a production-like runtime that serves as the foundation for future Kubernetes deployment.

---

# Directory Structure

```text
docker/
├── infrastructure/
│   ├── docker-compose.yml
│   └── env/
│       ├── kafka.env
│       └── postgres.env
│
├── applications/
│   ├── docker-compose.yml
│   └── env/
│       ├── auth.env
│       ├── user.env
│       ├── wallet.env
│       ├── notification.env
│       └── orchestrator.env
│
├── observability/
│   ├── docker-compose.yml
│   ├── grafana/
│   ├── loki/
│   ├── prometheus/
│   └── promtail/
│
├── docs/
│   └── Docker-Environment-Guide.md
│
└── README.md
```

---

# Docker Environment Architecture

The Docker environment is divided into three independent stacks.

```
Docker Environment

        +--------------------------------------+
        |         Infrastructure               |
        |--------------------------------------|
        | Kafka                                |
        | Kafka UI                             |
        +----------------+---------------------+
                         |
                         |
        +----------------+---------------------+
        |         Applications                |
        |--------------------------------------|
        | ms-auth-server                      |
        | ms-user                             |
        | ms-wallet                           |
        | ms-notification                     |
        | ms-transaction-orchestrator         |
        +----------------+---------------------+
                         |
                         |
        +----------------+---------------------+
        |       Observability                 |
        |--------------------------------------|
        | Prometheus                          |
        | Grafana                             |
        | Loki                                |
        | Promtail                            |
        +--------------------------------------+
```

---

# Infrastructure Stack

Location

```text
infrastructure/
```

Provides the shared platform services required by the application containers.

Current services include

* Apache Kafka (KRaft)
* Kafka UI

Configuration is externalized using

```text
env/
```

Start Infrastructure

```bash
docker compose up -d
```

---

# Application Stack

Location

```text
applications/
```

Provides all Dockerized Spring Boot microservices.

Current services include

* ms-auth-server
* ms-user
* ms-wallet
* ms-notification
* ms-transaction-orchestrator

Each service uses

* Docker Image
* Docker Container
* Environment Variables
* Shared Docker Network

Application configuration is externalized using dedicated environment files.

Example

```text
env/
├── wallet.env
├── notification.env
├── user.env
└── orchestrator.env
```

Start Applications

```bash
docker compose up -d
```

---

# Observability Stack

Location

```text
observability/
```

Provides centralized monitoring and logging.

Current services include

* Prometheus
* Grafana
* Loki
* Promtail

The observability stack monitors every application container running inside Docker.

Metrics Flow

```text
Spring Boot Containers
        │
        ▼
/actuator/prometheus
        │
        ▼
Prometheus
        │
        ▼
Grafana
```

Logs Flow

```text
Spring Boot Containers
        │
        ▼
/app/logs
        │
        ▼
Host logs
        │
        ▼
Promtail
        │
        ▼
Loki
        │
        ▼
Grafana
```

Start Observability

```bash
docker compose up -d
```

---

# Docker Network

Every Docker container communicates using the shared network

```text
infra_msimpl-network
```

Examples

| Service | Address |
|----------|----------|
| Kafka | kafka:9092 |
| Wallet | ms-wallet:8080 |
| Notification | ms-notification:8081 |
| Transaction Orchestrator | ms-transaction-orchestrator:8082 |
| Prometheus | prometheus:9090 |
| Loki | loki:3100 |
| Grafana | grafana:3000 |

---

# Logging Strategy

Every application writes logs inside

```text
/app/logs
```

Docker bind mount

```text
../../../logs:/app/logs
```

Promtail mounts the same directory

```text
../../../logs:/var/log/app
```

This provides centralized log collection without modifying application code.

---

# Configuration Strategy

Configuration is externalized using dedicated environment files.

Examples

```text
infrastructure/env/
applications/env/
```

Benefits

* Centralized configuration
* Cleaner Docker Compose files
* Environment-specific values
* No image rebuild
* Kubernetes-ready configuration
* Easy migration to ConfigMaps and Secrets

---

# Docker Deployment Workflow

```text
Modify Code
      │
      ▼
Build JAR
      │
      ▼
Build Docker Image
      │
      ▼
Start Infrastructure
      │
      ▼
Start Applications
      │
      ▼
Start Observability
      │
      ▼
Verify Logs & Metrics
```

---

# Future Deployment

The Docker environment is the direct foundation for Kubernetes deployment.

Deployment pipeline

```text
Spring Boot
      │
      ▼
Build JAR
      │
      ▼
Docker Image
      │
      ▼
Docker Container
      │
      ▼
Kubernetes
      │
      ▼
AWS EKS
```

The same Docker image and configuration strategy will be reused without changing application code.

---

# Documentation

Complete Docker setup, architecture, deployment workflow, troubleshooting and best practices are documented in

```text
docs/Docker-Environment-Guide.md
```

---

# Design Principles

The Docker environment follows these principles

* Containerized deployment
* Immutable Docker images
* Shared Docker network
* Centralized logging
* Centralized metrics
* Externalized configuration
* Environment-independent application code
* Production-like deployment
* Same Docker image for Kubernetes
* Kubernetes-ready architecture

---

# Version

| Property | Value |
|-----------|-------|
| Project | MSIMPL - Microservices Platform |
| Environment | Docker |
| Version | 1.0.0 |
| Author | Sanjeev Kumar |