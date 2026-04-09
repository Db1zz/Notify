# Notify

![2026-04-09 08 50 12](https://github.com/user-attachments/assets/19c52c62-7a91-4157-934c-17ce536eac21)

## What is it?
is a Rust notification service built for running as separate, independently scalable components.

It is split into three runtime roles:

- **Producer** — accepts notification data and publishes it to Kafka.
- **Consumer** — reads notification events from Kafka and processes them.
- **Load Balancer** — routes traffic to available nodes.

The project is designed to be simple to scale by running multiple nodes across one or more machines.

## Features

- **Kafka-based** notification pipeline
- **Cassandra-backed** storage
- **Separate producer, consumer, and load balancer** services
- **Async** runtime built on Tokio
- **Structured logging** with `tracing`
- **YAML-based** configuration
- **Docker Compose** setup for local development

## Requirements

- Rust / Cargo
- Docker
- Docker Compose

## Configuration

Notify reads configuration from `./config.yaml` by default.

### Example configuration:


```yaml

consumer:

  topic: "user-notifs"

  brokers: "localhost:9092"

  notifications_to_send_database_addr: "127.0.0.1:9042"

  blocked_notifications_database_addr: "127.0.0.1:9042"

  clients_node_addr: "127.0.0.1:6969"

  metrics_receiver_addr: "0.0.0.0:6979"


load_balancer:

  load_balancer_addr: "0.0.0.0:8989"

  load_balancer_metrics_addr: "0.0.0.0:6979"


producer:

  producer_addr: "0.0.0.0:8992"

  topic: "user-notifs"

  brokers: "localhost:9092"

``` 

## Configuration notes

- **brokers** should point to your Kafka broker.
- **notifications_to_send_database_addr** and **blocked_notifications_database_addr** should point to Cassandra.
- **clients_node_addr** is used by the consumer-side node communication.
- **metrics_receiver_addr** and **load_balancer_metrics_addr** are the metrics endpoints.
- **producer_addr** is the address the producer service binds to.
- **load_balancer_addr** is the address the load balancer binds to.

## Local development

### Start the infrastructure first:

```bash
docker compose up -d
```

### Then run one of the services:
```bash

cargo run -- producer
cargo run -- consumer
cargo run -- load-balancer
```

### What each service does

- **Producer**

    The producer starts the notification entrypoint and sends messages to Kafka.

- **Consumer**

    The consumer listens to the configured Kafka topic and handles notification processing.

- **Load Balancer**

    The load balancer accepts incoming traffic and distributes requests across nodes.

## Ports

### The default config uses these ports:

**9092** — Kafka

**9042** — Cassandra

**6969** — client node communication

**6979** — metrics

**8989** — load balancer

**8992** — producer

## TODO List
- Authentication
