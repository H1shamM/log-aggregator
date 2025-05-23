# Log Aggregator System
[![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)](https://python.org)  
[![AWS](https://img.shields.io/badge/AWS-S3-orange.svg)](https://aws.amazon.com)

![Architecture](docs/architecture.png)

## Features
- 📨 **RabbitMQ** message queue with durable queues and DLQ  
- ⚡ **Parallel batch processing** with ThreadPoolExecutor  
- 🔁 **Automatic retries** and dead-letter queue for failures  
- ☁️ **AWS S3** integration with fallback local storage on credential errors  
- 🚀 **Kubernetes KEDA** autoscaling based on queue length  

## Prerequisites
- Docker & Docker Compose  
- Python 3.9+  
- `kubectl` & a local Kubernetes cluster (minikube or Docker Desktop)  
- AWS CLI configured (`aws configure`) for local S3 testing  

## Local Development (Docker Compose)
```bash
# 1. Start RabbitMQ, producer, and consumer
docker-compose up -d

# 2. Send sample logs
docker-compose exec producer python producer.py

# 3. Start the consumer
docker-compose exec consumer python consumer.py
