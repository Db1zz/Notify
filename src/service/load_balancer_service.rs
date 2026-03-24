/*
    Purpose of the service:
        Load balancing (LB) services distribute
        the load between consumers and producers
        by selecting the least-loaded node.
        Distribution is carried out by sending node addresses
        to the client.

    Technical requirements:
        1. The load balancer MUST be aware of (consumers and producers)
        2. The load balancer is SINGLE-THREADED (Why? For simplicity. The service itself is not used as often as others;
            more notifications pass through the notification manager than new connections coming to this service.)
        3. Single connection for clients. The task is only to send addresses. If a user wants to receive
            new addresses, they can do so by connecting to the load balancer again.
        4. The load balancer must receive information about node load
*/

struct LoadBalancer {
}

impl LoadBalancer {
	pub fn new() -> Self {
		Self {

		}
	}

	async fn update_load_status() {

	}

	async fn get_least_loaded_node() {

	}

	pub async fn start() {

	}
}