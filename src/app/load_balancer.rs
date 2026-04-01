use crate::service::load_balancer_service::LoadBalancer;

pub async fn start(lb_addr: String, lb_metrics_addr: String) {
	let mut lb = LoadBalancer::new(lb_addr, lb_metrics_addr).await;
	lb.start().await;
}