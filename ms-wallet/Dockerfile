# =============================================================================
# File Name   : Dockerfile
# Module      : Docker Image Configuration
# Service     : MS Wallet Service
# Project     : MSIMPL - Microservices Platform
# Author      : Sanjeev Kumar
# Created On  : 03-Jul-2026
# Last Updated: 03-Jul-2026
# =============================================================================

# =============================================================================
# VERSION HISTORY
# =============================================================================
#
# Version | Date        | Author         | Description
# --------|-------------|----------------|------------------------------------------
# 1.0.0   | 03-Jul-2026 | Sanjeev Kumar  | Initial Docker image configuration.
#
# =============================================================================
# PURPOSE
# =============================================================================
#
# Builds the Docker image for the MS Wallet Service.
#
# The Docker image contains only the executable Spring Boot
# application and its Java runtime.
#
# Environment-specific configuration is NOT embedded into
# the image.
#
# Configuration is supplied externally through:
#
# • Environment Variables
# • Docker Compose
# • Kubernetes ConfigMaps
# • Kubernetes Secrets
#
# This allows the same Docker image to be deployed across
# multiple environments without rebuilding.
#
# =============================================================================
# IMAGE ARCHITECTURE
# =============================================================================
#
#               Spring Boot JAR
#                      │
#                      ▼
#          Eclipse Temurin JRE 17
#                      │
#                      ▼
#              MS Wallet Image
#                      │
#        ┌─────────────┼─────────────┐
#        │             │             │
#        ▼             ▼             ▼
#      STS         Kubernetes     AWS EKS
#   (Reference)       Docker        Docker
#
# =============================================================================
# DESIGN PRINCIPLES
# =============================================================================
#
# ✔ Lightweight runtime image
# ✔ Environment-independent image
# ✔ One Docker image for all environments
# ✔ Externalized configuration
# ✔ Immutable deployment artifact
# ✔ Kubernetes Ready
# ✔ AWS Ready
#
# =============================================================================
# MSIMPL DEPLOYMENT STANDARD
# =============================================================================
#
# ✔ Same application code
# ✔ Same executable JAR
# ✔ Same Docker image
# ✔ STS Ready
# ✔ Docker Ready
# ✔ Kubernetes Ready
# ✔ AWS EKS Ready
#
# Only environment variables change between deployments.
#
# =============================================================================

# -----------------------------------------------------------------------------
# Base Runtime Image
# -----------------------------------------------------------------------------
#
# Eclipse Temurin OpenJDK 17 Runtime.
#
# Provides only the Java Runtime Environment (JRE),
# resulting in a smaller production image than a full JDK.
#
FROM eclipse-temurin:17-jre

# -----------------------------------------------------------------------------
# Application Working Directory
# -----------------------------------------------------------------------------

WORKDIR /app

# -----------------------------------------------------------------------------
# Spring Boot Executable JAR
# -----------------------------------------------------------------------------
#
# The build argument allows different JAR versions to be used
# without modifying this Dockerfile.
#
ARG JAR_FILE=target/ms-wallet-0.0.1-SNAPSHOT.jar

COPY ${JAR_FILE} app.jar

# -----------------------------------------------------------------------------
# Application Port
# -----------------------------------------------------------------------------
#
# Documents the default application port.
#
# The actual runtime port may be overridden using:
#
# SERVER_PORT
#
EXPOSE 8080

# -----------------------------------------------------------------------------
# Application Startup
# -----------------------------------------------------------------------------
#
# Starts the Spring Boot application.
#
ENTRYPOINT ["java","-jar","app.jar"]