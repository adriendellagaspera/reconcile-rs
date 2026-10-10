//! Platform adapter for tasks, monotonic timers and cancellation.
//!
//! Native targets use Tokio. Browser tasks run on the current worker's event loop;
//! timer closures stay in worker-local storage, never inside a Send future.
#![allow(missing_docs)]

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub use tokio::{join, pin, select, spawn, sync, task, time};

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub use browser::*;

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
mod browser {
    pub use task::spawn;
    pub use tokio::{join, pin, select, sync};

    pub mod task {
        use futures_util::future::{AbortHandle, Abortable};
        use std::future::Future;
        use std::pin::Pin;
        use std::task::{Context, Poll};
        use tokio::sync::oneshot;

        pub struct JoinHandle<T> {
            abort: AbortHandle,
            result: oneshot::Receiver<T>,
        }

        impl<T> JoinHandle<T> {
            pub fn abort(&self) {
                self.abort.abort();
            }
        }

        impl<T> Future for JoinHandle<T> {
            type Output = Result<T, oneshot::error::RecvError>;
            fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
                Pin::new(&mut self.result).poll(cx)
            }
        }

        pub fn spawn<F: Future + 'static>(future: F) -> JoinHandle<F::Output> {
            let (abort, registration) = AbortHandle::new_pair();
            let (sender, result) = oneshot::channel();
            wasm_bindgen_futures::spawn_local(async move {
                if let Ok(value) = Abortable::new(future, registration).await {
                    let _ = sender.send(value);
                }
            });
            JoinHandle { abort, result }
        }

        pub async fn yield_now() {
            super::time::sleep(std::time::Duration::ZERO).await;
        }
    }

    pub mod time {
        use gloo_timers::callback::Timeout;
        use std::cell::RefCell;
        use std::collections::HashMap;
        use std::future::Future;
        use std::pin::Pin;
        use std::task::{Context, Poll};
        use tokio::sync::oneshot;
        pub use web_time::{Duration, Instant};

        thread_local! {
            static TIMERS: RefCell<(u64, HashMap<u64, Timeout>)> = RefCell::new((0, HashMap::new()));
        }

        pub struct Sleep {
            deadline: Instant,
            id: u64,
            fired: oneshot::Receiver<()>,
        }

        fn arm(deadline: Instant) -> (u64, oneshot::Receiver<()>) {
            let delay = deadline.saturating_duration_since(Instant::now());
            // Round upward so sub-millisecond delays cannot create an early wakeup spin.
            let millis = delay.as_micros().div_ceil(1000).min(i32::MAX as u128) as u32;
            let (sender, receiver) = oneshot::channel();
            let id = TIMERS.with(|timers| {
                let mut timers = timers.borrow_mut();
                timers.0 = timers
                    .0
                    .checked_add(1)
                    .expect("browser timer ids exhausted");
                let id = timers.0;
                timers.1.insert(
                    id,
                    Timeout::new(millis, move || {
                        TIMERS.with(|timers| timers.borrow_mut().1.remove(&id));
                        let _ = sender.send(());
                    }),
                );
                id
            });
            (id, receiver)
        }

        pub fn sleep(duration: Duration) -> Sleep {
            sleep_until(Instant::now() + duration)
        }

        pub fn sleep_until(deadline: Instant) -> Sleep {
            let (id, fired) = arm(deadline);
            Sleep {
                deadline,
                id,
                fired,
            }
        }

        impl Future for Sleep {
            type Output = ();
            fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
                match Pin::new(&mut self.fired).poll(cx) {
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(_) if Instant::now() >= self.deadline => Poll::Ready(()),
                    Poll::Ready(_) => {
                        let (id, fired) = arm(self.deadline);
                        self.id = id;
                        self.fired = fired;
                        Pin::new(&mut self.fired).poll(cx).map(|_| ())
                    }
                }
            }
        }

        impl Drop for Sleep {
            fn drop(&mut self) {
                TIMERS.with(|timers| timers.borrow_mut().1.remove(&self.id));
            }
        }

        pub mod error {
            #[derive(Debug)]
            pub struct Elapsed;
            impl std::fmt::Display for Elapsed {
                fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    f.write_str("deadline has elapsed")
                }
            }
            impl std::error::Error for Elapsed {}
        }

        pub async fn timeout<F: Future>(
            duration: Duration,
            future: F,
        ) -> Result<F::Output, error::Elapsed> {
            tokio::select! {
                biased;
                result = future => Ok(result),
                _ = sleep(duration) => Err(error::Elapsed),
            }
        }

        pub enum MissedTickBehavior {
            Skip,
        }
        pub struct Interval {
            period: Duration,
            next: Instant,
        }

        pub fn interval(period: Duration) -> Interval {
            assert!(!period.is_zero(), "interval period must be nonzero");
            Interval {
                period,
                next: Instant::now(),
            }
        }

        impl Interval {
            pub fn set_missed_tick_behavior(&mut self, _: MissedTickBehavior) {}

            pub async fn tick(&mut self) -> Instant {
                let deadline = self.next;
                if deadline > Instant::now() {
                    sleep_until(deadline).await;
                }
                let now = Instant::now();
                let remainder =
                    now.saturating_duration_since(deadline).as_nanos() % self.period.as_nanos();
                let until_next = self.period - Duration::from_nanos(remainder as u64);
                self.next = now + until_next;
                deadline
            }
        }
    }
}
