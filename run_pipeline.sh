#!/bin/bash

set -e  # Exit immediately if a command fails

echo "Cleaning previous Docker setup..."

# Stop and remove containers + volumes
docker-compose down -v

# --------------------------------------

echo "Building Docker images from scratch..."
docker-compose build --no-cache

# --------------------------------------

echo "Starting PostgreSQL..."
docker-compose up -d postgres

echo "Waiting for PostgreSQL to be ready..."
sleep 10

# --------------------------------------

echo "Initializing Airflow (DB + Admin user)..."
docker-compose run airflow-init

# --------------------------------------

echo "Starting Airflow services..."
docker-compose up -d airflow-webserver airflow-scheduler

echo "Waiting for Airflow to start..."
sleep 15

# --------------------------------------

echo "Checking running containers..."
docker ps