use serde::{Deserialize, Serialize};
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Config {
    pub consumer: ConsumerConfig,
    pub load_balancer: LoadBalancerConfig,
    pub producer: ProducerConfig,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ConsumerConfig {
	pub topic: String,
	pub brokers: String,
	pub notifications_to_send_database_addr: String,
	pub blocked_notifications_database_addr: String,
	pub clients_node_addr: String,
	pub metrics_receiver_addr: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LoadBalancerConfig {
	pub load_balancer_addr: String,
	pub load_balancer_metrics_addr: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ProducerConfig {
	pub producer_addr: String,
	pub topic: String,
	pub brokers: String,
}