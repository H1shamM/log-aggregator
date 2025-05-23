# Log Aggregator System
[![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)](https://python.org)
[![AWS](https://img.shields.io/badge/AWS-S3-orange.svg)](https://aws.amazon.com)

![Architecture](docs/architecture.png)

## Features
- 📨 RabbitMQ message queue
- ⚡ Parallel batch processing
- 🔁 Automatic retries + DLQ
- ☁️ AWS S3 integration

## Quick Start
```bash
docker-compose up -d
python src/producer/producer.py