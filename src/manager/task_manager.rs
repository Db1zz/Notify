use std::sync::Arc;

use crossbeam_deque::{Injector, Stealer, Worker};

pub struct TaskManager<Task, HandlerParams, HandlerResult> {
	max_workers: i32,

	injector: Arc<Injector<Task>>,
    stealers: Arc<Vec<Stealer<Task>>>,

	handler: fn(HandlerParams, Task) -> HandlerResult,
	handler_params: HandlerParams,
}

impl<Task, HandlerParams, HandlerResult> TaskManager<Task, HandlerParams, HandlerResult>
where
    Task: Send + 'static,
    HandlerParams: Clone + Copy + Send + 'static,
	HandlerResult: 'static,
{
	pub fn new(max_workers: i32, handler: fn(HandlerParams, Task) -> HandlerResult, handler_params: HandlerParams) -> Self {
		Self {
			max_workers,
			injector: Arc::new(Injector::new()),
			stealers: Arc::new(Vec::new()),

			handler,
			handler_params,
		}
	}

	fn create_workers_and_stealers(&self) -> (Vec<Worker<Task>>, Vec<Stealer<Task>>) {
		let mut workers = Vec::new();
		let mut stealers = Vec::new();

		for _ in 0..self.max_workers {
			let worker = Worker::<Task>::new_fifo();
			stealers.push(worker.stealer());
			workers.push(worker);
		}

		(workers, stealers)
	}

	fn spawn_workers(&self) {
		let (mut workers, mut stealers) = self.create_workers_and_stealers();

		for worker in workers {
			let stealers = self.stealers.clone();
			let injector = self.injector.clone();

			let handler_params = self.handler_params.clone();
			let handler = self.handler.clone();
			std::thread::spawn(move || {
				loop {
					if let Some(task) = worker.pop() {
						(handler)(handler_params, task);
						continue;
					}

					if let Some(task) = injector.steal().success() {
						(handler)(handler_params, task);
						continue;
					}

					for stealer in &*stealers {
						if let Some(task) = stealer.steal().success() {
						(handler)(handler_params, task);
							break;
						}
					}
				}
			});
		}
	}

	pub fn start(&self) {
		self.spawn_workers();
	}
}