use atomic_waker::AtomicWaker;
use concurrent_queue::{ConcurrentQueue, PopError, PushError};
use pin_project_lite::pin_project;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};

/// A ConcurrentQueue with a Waker attached. Allows rapid
/// implementation of fast channels where at least one side may not be
/// cloned. Caution: Last Write wins and you are asking for trouble if
/// there are races registering Wakers. See `atomic_waker` for details.
#[derive(Debug)]
pub struct WakerQueue<T> {
    queue: ConcurrentQueue<T>,
    waker: AtomicWaker,
}

impl<T: 'static + Send> WakerQueue<T> {

    /// Create a WakerQueue with the given capacity.
    pub fn bounded(size: usize) -> WakerQueue<T> {
        WakerQueue {
            queue: ConcurrentQueue::bounded(size),
            waker: AtomicWaker::new(),
        }
    }

    /// Create a WakerQueue which will dynamically resize as required.
    pub fn unbounded() -> WakerQueue<T> {
        WakerQueue {
            queue: ConcurrentQueue::unbounded(),
            waker: AtomicWaker::new(),
        }
    }

    /// true if the WakerQueue has no items.
    pub fn is_empty(&self) -> bool { self.queue.is_empty() }

    /// true if the WakerQueue has no spare capacity.
    pub fn is_full(&self) -> bool { self.queue.is_full() }

    /// Count of items in the WakerQueue
    pub fn len(&self) -> usize { self.queue.len() }

    /// Returns the maximum capacity of the queue. None for unbounded queues.
    pub fn capacity(&self) -> Option<usize> { self.queue.capacity() }

    /// Closes the queue so that no more items can be pushed. Pops are allowed.
    pub fn close(&self) { self.queue.close(); }

    /// true if the queue is closed
    pub fn is_closed(&self) -> bool { self.queue.is_closed() }

    /// Attempt to push an item into the queue. Will fail if the queue is full or closed.
    pub fn try_push(&self, value: T) -> Result<(), PushError<T>> { self.queue.push(value) }

    /// Attempts a push. If successful and `wake` is true, wakes the
    /// last registered Waker.
    pub fn try_push_wake(&self, value: T, wake: bool) -> Result<(), PushError<T>> {
        let ret = self.try_push(value);
        self.wake_if(ret.is_ok() && wake);
        ret
    }

    /// Attempts a push. If successful and the queue was previously
    /// empty, wakes the last registered Waker.
    pub fn try_push_wake_empty(&self, value: T) -> Result<(), PushError<T>> {
        self.try_push_wake(value, self.is_empty())
    }
    
    /// Attempts a push. If successful and `wake` is true, wakes the
    /// last registered Waker.
    pub fn try_push_wake_full(&self, value: T) -> Result<(), PushError<T>> {
        self.try_push_wake(value, self.is_full())
    }
    
    /// Attempts to pop an item from the queue. Will fail if the queue is empty.
    pub fn try_pop(&self) -> Result<T, PopError> { self.queue.pop() }

    
    /// Attempts a pop. If successful and `wake` is true, wakes the
    /// last registered Waker.
    pub fn try_pop_wake(&self, wake: bool) -> Result<T, PopError> {
        let ret = self.try_pop();
        self.wake_if(ret.is_ok() && wake);
        ret
    }

    /// Attempts a pop. If successful and the queue was previously
    /// empty, wakes the last registered Waker.
    pub fn try_pop_wake_empty(&self) -> Result<T, PopError> { self.try_pop_wake(self.is_empty()) }

    /// Attempts a pop. If successful and the queue was previously
    /// full, wakes the last registered Waker.
    pub fn try_pop_wake_full(&self) -> Result<T, PopError> { self.try_pop_wake(self.is_full()) }

    /// Returns a future which pushes into a WakerQueue
    pub fn push<'a, F>(&'a self, value: T, wake_if: F) -> Push<'a, T, F>
    where F: Fn(&'a WakerQueue<T>) -> bool {
        Push::new(self, value, wake_if)
    }

    /// Returns a future which pops from a WakerQueue
    pub fn pop<'a, F>(&'a self, wake_if: F) -> Pop<'a, T, F>
    where F: Fn(&'a WakerQueue<T>) -> bool {
        Pop::new(self, wake_if)
    }

    /// Registers a waker with the WakerQueue.
    pub fn register(&self, waker: &Waker) { self.waker.register(waker); }

    /// Wakes the last registered Waker, if any.
    pub fn wake(&self) { self.waker.wake(); }

    /// Wakes the last registered Waker, if any, if wake is true.
    pub fn wake_if(&self, wake: bool) { if wake { self.wake(); } }

    /// Attempts to pop. If it fails because the queue is empty, it
    /// registers the Waker from the provided context.
    pub fn poll_pop(&self, ctx: &Context) -> Result<T, PopError> {
        match self.try_pop() {
            Err(PopError::Empty) => {
                self.register(ctx.waker());
                self.try_pop() // save us from a race.
            }
            other => other,
        }
    }

    /// Attempts to push. If it fails because the queue is full, it
    /// registers the Waker from the provided context.
    pub fn poll_push(&self, value: T, ctx: &Context) -> Result<(), PushError<T>> {
        match self.try_push(value) {
            Err(PushError::Full(value)) => {
                self.register(ctx.waker());
                self.try_push(value) // save us from a race.
            }
            other => other,
        }
    }

}

unsafe impl<T: 'static + Send> Send for WakerQueue<T> {}
unsafe impl<T: 'static + Send> Sync for WakerQueue<T> {}

pin_project! {
    /// Future for `WakerQueue.push(value, wake_if)`
    pub struct Push<'a, T, F> {
        queue: &'a WakerQueue<T>,
        value: Option<T>,
        wake_if: F,
    }
}

impl<'a, T, F> Push<'a, T, F>
where T: 'static + Send, F: Fn(&'a WakerQueue<T>) -> bool{
    fn new(queue: &'a WakerQueue<T>, value: T, wake_if: F) -> Push<'a, T, F> {
        Push { queue, value: Some(value), wake_if }
    }
}

impl<'a, T, F> Future for Push<'a, T, F>
where T: 'static + Send, F: Fn(&'a WakerQueue<T>) -> bool{
    type Output = Result<(), PushError<T>>;

    fn poll(self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Self::Output> {
        let this = self.project();
        let value = this.value.take().expect("Do not poll futures after completion.");
        let wake_if = (this.wake_if)(&this.queue);
        match this.queue.poll_push(value, ctx) {
            Ok(()) => {
                if wake_if { this.queue.wake(); }
                Poll::Ready(Ok(()))
            }
            Err(PushError::Closed(value)) =>
                Poll::Ready(Err(PushError::Closed(value))),
            Err(PushError::Full(value)) => {
                *this.value = Some(value);
                Poll::Pending
            }
        }
    }
}

pin_project! {
    /// Future for `WakerQueue.pop(wake_if)`
    pub struct Pop<'a, T, F> {
        queue: &'a WakerQueue<T>,
        wake_if: F,
    }
}

impl<'a, T, F> Pop<'a, T, F>
where T: 'static + Send, F: Fn(&'a WakerQueue<T>) -> bool{
    fn new(queue: &'a WakerQueue<T>, wake_if: F) -> Pop<'a, T, F> {
        Pop { queue, wake_if }
    }
}

impl<'a, T, F> Future for Pop<'a, T, F>
where T: 'static + Send, F: Fn(&'a WakerQueue<T>) -> bool {
    type Output = Result<T, PopError>;

    fn poll(self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Result<T, PopError>> {
        let this = self.project();
        let wake_if = (this.wake_if)(&this.queue);
        match this.queue.poll_pop(ctx) {
            Ok(val) => {
                if wake_if { this.queue.wake(); }
                Poll::Ready(Ok(val))
            }
            Err(PopError::Closed) => Poll::Ready(Err(PopError::Closed)),
            Err(PopError::Empty) => Poll::Pending,
        }
    }
}
