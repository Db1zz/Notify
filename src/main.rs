pub mod models;
pub mod error;
pub mod repository;
pub mod consumer;
pub mod manager;
pub mod service;
pub mod metrics;
pub mod config;

mod app;

use core::panic;
use std::fs;

use clap::{Parser, Subcommand};
use crate::config::parser;

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

fn get_config_string(path: Option<String>) -> String {
    let config_path = match path {
        Some(path) => path,
        None => "./config.yaml".to_owned()
    };

    let config_raw_data = match fs::read_to_string(config_path.clone()) {
        Ok(data) => data,
        Err(e) => {
            panic!("Error: Unable to read '{}' file\nReason: {}", config_path, e);
        }
    };

    config_raw_data
}

#[tokio::main]
async fn main() {
    // Get config path from a CLI or use default path...
    let config_raw_data = get_config_string(None);
    let result = parser::parse(config_raw_data);
    let config = match result {
        Ok(c) => c,
        Err(e) => {
            panic!("Failed to parse config {}", e);
        }
    };

    let cli = Cli::parse();

    tracing_subscriber::fmt()
            .with_target(true) // - shows which module/service the log came from
            .with_thread_ids(true)
            .with_level(true)
            .init();

    match cli.command {
        Service::Consumer => {
            app::consumer::start(config.consumer.unwrap()).await;
        }

        Service::Producer => {
            app::producer::start(config.producer.unwrap()).await;
        }

        Service::LoadBalancer => {
            app::load_balancer::start(config.load_balancer.unwrap()).await;
        }
    }
}