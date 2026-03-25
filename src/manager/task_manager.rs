use std::{sync::{Arc, Mutex, atomic::{AtomicBool, Ordering}}, usize};

use crossbeam_deque::{Injector, Stealer, Worker};
use std::collections::VecDeque;

pub struct TaskManager<Task, HandlerParams, HandlerResult> {
	max_workers: usize,

	injector: Arc<Injector<Task>>,
    stealers: Option<Arc<Vec<Stealer<Task>>>>,

	handler: fn(HandlerParams, Task) -> HandlerResult,
	handler_params: HandlerParams,

	join_handles: Vec<std::thread::JoinHandle<()>>,
	threads: Vec<std::thread::Thread>,
	afk_threads: Arc<Mutex<VecDeque<std::thread::Thread>>>,

	is_exiting: Arc<AtomicBool>,
}

impl<Task, HandlerParams, HandlerResult> TaskManager<Task, HandlerParams, HandlerResult>
where
    Task: Send + 'static,
    HandlerParams: Clone + Copy + Send + 'static,
	HandlerResult: 'static,
{
	pub fn new(max_workers: usize, handler: fn(HandlerParams, Task) -> HandlerResult, handler_params: HandlerParams) -> Self {
		Self {
			max_workers,
			injector: Arc::new(Injector::new()),
			stealers: None,

			handler,
			handler_params,
			join_handles: Vec::new(),
			threads: Vec::new(),
			afk_threads: Arc::new(Mutex::new(VecDeque::new())),

			is_exiting: Arc::new(AtomicBool::new(false)),
		}
	}

    fn create_workers_and_stealers(&self) -> (Vec<Worker<Task>>, Vec<Stealer<Task>>) {
        let mut workers = Vec::with_capacity(self.max_workers);
        let mut stealers = Vec::with_capacity(self.max_workers);

        for _ in 0..self.max_workers {
            let worker = Worker::new_fifo();
            stealers.push(worker.stealer());
            workers.push(worker);
        }

        (workers, stealers)
    }

	fn spawn_workers(&mut self) {
		let (workers, stealers) = self.create_workers_and_stealers();
		self.stealers = Some(Arc::new(stealers));

		for worker in workers {
			let stealers = self.stealers.clone();
			let injector = self.injector.clone();

			let handler_params = self.handler_params.clone();
			let handler = self.handler.clone();

			let afk_threads = self.afk_threads.clone();
			let is_exiting = self.is_exiting.clone();

			let join_handle = std::thread::spawn(move || {
				loop {
					if let Some(task) = worker.pop() {
						(handler)(handler_params, task);
						continue;
					}

					if let Some(task) = injector.steal().success() {
						(handler)(handler_params, task);
						continue;
					}

					if let Some(stealers) = &stealers {
						for stealer in stealers.iter() {
							if let Some(task) = stealer.steal().success() {
							(handler)(handler_params, task);
								break;
							}
						}
						continue;
					}

					if is_exiting.load(Ordering::Relaxed) {
						break;
					}

					{
						let mut queue = afk_threads.lock().unwrap();
						queue.push_back(std::thread::current());
					}
					std::thread::park();
				}
			});

			self.threads.push(join_handle.thread().clone());
			self.join_handles.push(join_handle);
		}
	}

	pub fn start(&mut self) {
		self.spawn_workers();
	}

	pub fn submit(&mut self, task: Task) {
		if let Some(thread) = self.afk_threads.lock().unwrap().pop_front() {
			thread.unpark();
		}
		self.injector.push(task);
	}
}

impl<Task, HandlerParams, HandlerResult> Drop
	for TaskManager<Task, HandlerParams, HandlerResult> 
{
	fn drop(&mut self) {
		self.is_exiting.store(true, Ordering::Relaxed);

		{
			let mut queue = self.afk_threads.lock().unwrap();
			while let Some(thread) = queue.pop_front() {
				thread.unpark();
			}
		}

		for handle in self.join_handles.drain(..) {
			let _ = handle.join();
		}
	}
}