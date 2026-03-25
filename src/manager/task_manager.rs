use std::{sync::{Arc, Mutex, atomic::{AtomicBool, Ordering}}, usize};

use crossbeam_deque::{Injector, Stealer, Worker};
use std::collections::VecDeque;

pub struct TaskManager<Task: TaskManagerTask> {
	max_workers: usize,

	injector: Arc<Injector<Task>>,
    stealers: Option<Arc<Vec<Stealer<Task>>>>,

	join_handles: Vec<std::thread::JoinHandle<()>>,
	threads: Vec<std::thread::Thread>,
	afk_threads: Arc<Mutex<VecDeque<std::thread::Thread>>>,

	is_exiting: Arc<AtomicBool>,
	is_started: bool,
}

impl<Task: TaskManagerTask> TaskManager<Task>
where
    Task: TaskManagerTask + Send + 'static,
{
	pub fn new(max_workers: usize) -> Self {
		Self {
			max_workers,
			injector: Arc::new(Injector::new()),
			stealers: None,

			join_handles: Vec::new(),
			threads: Vec::new(),
			afk_threads: Arc::new(Mutex::new(VecDeque::new())),

			is_exiting: Arc::new(AtomicBool::new(false)),
			is_started: false,
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

	fn worker_routine(context: TaskManagerWorkerContext<Task>)
	{
		loop {
			if let Some(task) = context.worker.pop() {
				task.handle();
				continue;
			}

			if let Some(task) = context.injector.steal().success() {
				task.handle();
				continue;
			}

			if let Some(stealers) = &context.stealers {
				for stealer in stealers.iter() {
					if let Some(task) = stealer.steal().success() {
						task.handle();
						break;
					}
				}
				continue;
			}

			if context.is_exiting.load(Ordering::Relaxed) {
				break;
			}

			{
				let mut queue = context.afk_threads.lock().unwrap();
				queue.push_back(std::thread::current());
			}
			std::thread::park();
		}

	}

	fn spawn_workers(&mut self) {
		let (workers, stealers) = self.create_workers_and_stealers();
		self.stealers = Some(Arc::new(stealers));

		for worker in workers {
			let worker_context = TaskManagerWorkerContext::new(
				self.stealers.clone(),
				self.injector.clone(),
				self.afk_threads.clone(),
				self.is_exiting.clone(),
				worker
			);

			let join_handle = std::thread::spawn(move || {
				Self::worker_routine(worker_context);
			});

			self.threads.push(join_handle.thread().clone());
			self.join_handles.push(join_handle);
		}
	}

	pub fn start(&mut self) {
		if self.is_started == true {
			return;
		}

		self.spawn_workers();
	}

	pub fn submit(&mut self, task: Task) {
		if let Some(thread) = self.afk_threads.lock().unwrap().pop_front() {
			thread.unpark();
		}
		self.injector.push(task);
	}
}

impl<Task> Drop for TaskManager<Task> 
where
	Task: TaskManagerTask
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

		self.is_started = false;
	}
}

struct TaskManagerWorkerContext<Task> {
	stealers: Option<Arc<Vec<Stealer<Task>>>>,
	injector: Arc<Injector<Task>>,
	afk_threads: Arc<Mutex<VecDeque<std::thread::Thread>>>,
	is_exiting: Arc<AtomicBool>,
	worker: Worker<Task>
}

impl<Task> TaskManagerWorkerContext<Task> {
	pub fn new(
		stealers: Option<Arc<Vec<Stealer<Task>>>>,
		injector: Arc<Injector<Task>>,
		afk_threads: Arc<Mutex<VecDeque<std::thread::Thread>>>,
		is_exiting: Arc<AtomicBool>,
		worker: Worker<Task>) -> Self
	{
		Self {
			stealers,
			injector,
			afk_threads,
			is_exiting,
			worker
		}
	}
}

pub trait TaskManagerTask {
	fn handle(self);
}