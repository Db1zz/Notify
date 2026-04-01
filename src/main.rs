pub mod models;
pub mod error;
pub mod repository;
pub mod consumer;
pub mod manager;
pub mod service;
pub mod metrics;
mod app;

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
    LoadBalancer
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    let topic = Arc::<String>::new("user-notifs".to_string());
    let brokers = Arc::<String>::new("localhost:9092".to_string());

    let producer_addr = Arc::<String>::new("localhost:9069".to_string());

    let lb_addr = "0.0.0.0:8989".to_owned();
    let lb_metrics_addr = "0.0.0.0:6979".to_owned();

    match cli.command {
        Service::Consumer => {
            app::consumer::start(&brokers, &topic).await;
        }

        Service::Producer => {
            app::producer::start(&producer_addr, &brokers, &topic).await;
        }

        Service::LoadBalancer => {
            app::load_balancer::start(lb_addr, lb_metrics_addr).await;
        }
    }
}