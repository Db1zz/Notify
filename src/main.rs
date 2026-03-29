pub mod models;
pub mod error;
pub mod repository;
pub mod consumer;
pub mod manager;
pub mod service;
pub mod metrics;

mod kafka_consumer_service;
mod kafka_producer_service;

use std::sync::Arc;
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "Notify", about = "Notification Service for Kafka")]
struct Cli {
    #[command(subcommand)]
    command: Service
}

#[derive(Subcommand)]
enum Service {
    Producer,
    Consumer,
    // LoadBalancer
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    let topic = Arc::<String>::new("user-notifs".to_string());
    let brokers = Arc::<String>::new("localhost:9092".to_string());

    let producer_addr = Arc::<String>::new("localhost:9069".to_string());

    match cli.command {
        Service::Consumer => {
            kafka_consumer_service::start(&brokers, &topic).await;
        }

        Service::Producer => {
            kafka_producer_service::start(&producer_addr, &brokers, &topic).await;
        }
    }
}