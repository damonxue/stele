//! Sequential invocation of registered setup and event handlers.
//!
//! The caller supplies already ordered invocations and a context for each one.
//! This module does not schedule events or provide transactional rollback.

use crate::{DuplicateHandler, HandlerRegistry};
use std::{error::Error, fmt, future::Future, pin::Pin};

/// A handler future may borrow its context and, for events, its payload.
pub type HandlerFuture<'a, E> = Pin<Box<dyn Future<Output = Result<(), E>> + 'a>>;

type SetupHandler<C, E> = dyn for<'a> Fn(&'a mut C) -> HandlerFuture<'a, E>;
type EventHandler<C, T, E> = dyn for<'a> Fn(&'a mut C, &'a T) -> HandlerFuture<'a, E>;

enum Handler<C, T, E> {
    Setup(Box<SetupHandler<C, E>>),
    Event(Box<EventHandler<C, T, E>>),
}

/// One setup callback and its caller-provided context.
pub struct SetupInvocation<'a, C> {
    pub name: &'a str,
    pub context: &'a mut C,
}

/// One named event callback, its payload, and its caller-provided context.
pub struct EventInvocation<'a, C, T> {
    pub name: &'a str,
    pub context: &'a mut C,
    pub event: &'a T,
}

/// A dispatch failure. Handler errors are preserved as the original value.
#[derive(Debug, PartialEq, Eq)]
pub enum InvocationError<E> {
    UnknownHandler(String),
    WrongHandlerKind(String),
    Handler { name: String, source: E },
}

impl<E: fmt::Display> fmt::Display for InvocationError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownHandler(name) => write!(f, "no indexing function registered for '{name}'"),
            Self::WrongHandlerKind(name) => {
                write!(f, "indexing function has the wrong kind for '{name}'")
            }
            Self::Handler { name, source } => {
                write!(f, "indexing function '{name}' failed: {source}")
            }
        }
    }
}

impl<E: Error + 'static> Error for InvocationError<E> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Handler { source, .. } => Some(source),
            _ => None,
        }
    }
}

/// A single namespace of setup and event callbacks, keyed by their full names.
///
/// For example, callers can register a setup callback as `Token:setup`.
/// Registering an existing name fails without replacing the original callback.
pub struct IndexingHandlers<C, T, E> {
    handlers: HandlerRegistry<Handler<C, T, E>>,
}

impl<C, T, E> Default for IndexingHandlers<C, T, E> {
    fn default() -> Self {
        Self {
            handlers: HandlerRegistry::default(),
        }
    }
}

impl<C, T, E> IndexingHandlers<C, T, E> {
    pub fn register_setup<F>(&mut self, name: String, handler: F) -> Result<(), DuplicateHandler>
    where
        F: for<'a> Fn(&'a mut C) -> HandlerFuture<'a, E> + 'static,
    {
        self.handlers
            .register(name, Handler::Setup(Box::new(handler)))
    }

    pub fn register_event<F>(&mut self, name: String, handler: F) -> Result<(), DuplicateHandler>
    where
        F: for<'a> Fn(&'a mut C, &'a T) -> HandlerFuture<'a, E> + 'static,
    {
        self.handlers
            .register(name, Handler::Event(Box::new(handler)))
    }

    /// Await each setup callback in input order, stopping at the first failure.
    pub async fn process_setup<'a>(
        &self,
        invocations: impl IntoIterator<Item = SetupInvocation<'a, C>>,
    ) -> Result<(), InvocationError<E>>
    where
        C: 'a,
    {
        for invocation in invocations {
            let handler = self
                .handlers
                .get(invocation.name)
                .ok_or_else(|| InvocationError::UnknownHandler(invocation.name.to_owned()))?;
            let Handler::Setup(handler) = handler else {
                return Err(InvocationError::WrongHandlerKind(
                    invocation.name.to_owned(),
                ));
            };
            handler(invocation.context)
                .await
                .map_err(|source| InvocationError::Handler {
                    name: invocation.name.to_owned(),
                    source,
                })?;
        }
        Ok(())
    }

    /// Await each event callback in input order, stopping at the first failure.
    pub async fn process_events<'a>(
        &self,
        invocations: impl IntoIterator<Item = EventInvocation<'a, C, T>>,
    ) -> Result<(), InvocationError<E>>
    where
        C: 'a,
        T: 'a,
    {
        for invocation in invocations {
            let handler = self
                .handlers
                .get(invocation.name)
                .ok_or_else(|| InvocationError::UnknownHandler(invocation.name.to_owned()))?;
            let Handler::Event(handler) = handler else {
                return Err(InvocationError::WrongHandlerKind(
                    invocation.name.to_owned(),
                ));
            };
            handler(invocation.context, invocation.event)
                .await
                .map_err(|source| InvocationError::Handler {
                    name: invocation.name.to_owned(),
                    source,
                })?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        cell::RefCell,
        rc::Rc,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        task::{Context, Poll, Wake, Waker},
        thread,
    };

    struct ThreadWake(thread::Thread);

    impl Wake for ThreadWake {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.unpark();
        }
    }

    fn block_on<F: Future>(future: F) -> F::Output {
        let waker = Waker::from(Arc::new(ThreadWake(thread::current())));
        let mut context = Context::from_waker(&waker);
        let mut future = std::pin::pin!(future);
        loop {
            match future.as_mut().poll(&mut context) {
                Poll::Ready(output) => return output,
                Poll::Pending => thread::park(),
            }
        }
    }

    struct YieldOnce(bool);

    impl Future for YieldOnce {
        type Output = ();

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            if self.0 {
                Poll::Ready(())
            } else {
                self.0 = true;
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }

    struct WakeFromThread {
        ready: Arc<AtomicBool>,
        started: bool,
    }

    impl Future for WakeFromThread {
        type Output = ();

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            if self.ready.load(Ordering::Acquire) {
                return Poll::Ready(());
            }
            if !self.started {
                self.started = true;
                let ready = self.ready.clone();
                let waker = cx.waker().clone();
                thread::spawn(move || {
                    ready.store(true, Ordering::Release);
                    waker.wake();
                });
            }
            Poll::Pending
        }
    }

    #[derive(Clone)]
    struct TestContext {
        chain: u8,
        trace: Rc<RefCell<Vec<String>>>,
        fail: bool,
    }

    fn setup(context: &mut TestContext) -> HandlerFuture<'_, &'static str> {
        Box::pin(async move {
            context
                .trace
                .borrow_mut()
                .push(format!("setup {}", context.chain));
            YieldOnce(false).await;
            if context.fail {
                return Err("setup failed");
            }
            context
                .trace
                .borrow_mut()
                .push(format!("setup done {}", context.chain));
            Ok(())
        })
    }

    fn event<'a>(context: &'a mut TestContext, event: &'a u8) -> HandlerFuture<'a, &'static str> {
        Box::pin(async move {
            context
                .trace
                .borrow_mut()
                .push(format!("start {} {event}", context.chain));
            YieldOnce(false).await;
            if context.fail {
                return Err("event failed");
            }
            context
                .trace
                .borrow_mut()
                .push(format!("done {} {event}", context.chain));
            Ok(())
        })
    }

    fn contexts() -> (TestContext, TestContext, Rc<RefCell<Vec<String>>>) {
        let trace = Rc::new(RefCell::new(Vec::new()));
        (
            TestContext {
                chain: 1,
                trace: trace.clone(),
                fail: false,
            },
            TestContext {
                chain: 2,
                trace: trace.clone(),
                fail: false,
            },
            trace,
        )
    }

    #[test]
    fn setup_and_events_await_in_input_order_with_each_context() {
        let mut handlers = IndexingHandlers::default();
        handlers
            .register_setup("Token:setup".into(), setup)
            .unwrap();
        handlers
            .register_event("Token:Transfer".into(), event)
            .unwrap();
        handlers
            .register_event("Token:Approval".into(), event)
            .unwrap();
        let (mut first, mut second, trace) = contexts();

        block_on(handlers.process_setup([
            SetupInvocation {
                name: "Token:setup",
                context: &mut first,
            },
            SetupInvocation {
                name: "Token:setup",
                context: &mut second,
            },
        ]))
        .unwrap();
        block_on(handlers.process_events([
            EventInvocation {
                name: "Token:Transfer",
                context: &mut second,
                event: &8,
            },
            EventInvocation {
                name: "Token:Approval",
                context: &mut first,
                event: &9,
            },
        ]))
        .unwrap();

        assert_eq!(
            &*trace.borrow(),
            &[
                "setup 1",
                "setup done 1",
                "setup 2",
                "setup done 2",
                "start 2 8",
                "done 2 8",
                "start 1 9",
                "done 1 9",
            ]
        );
    }

    #[test]
    fn handler_can_resume_after_wakeup_from_another_thread() {
        fn delayed(context: &mut TestContext) -> HandlerFuture<'_, &'static str> {
            Box::pin(async move {
                WakeFromThread {
                    ready: Arc::new(AtomicBool::new(false)),
                    started: false,
                }
                .await;
                context.trace.borrow_mut().push("resumed".into());
                Ok(())
            })
        }

        let mut handlers = IndexingHandlers::<_, u8, _>::default();
        handlers.register_setup("setup".into(), delayed).unwrap();
        let (mut first, _, trace) = contexts();
        block_on(handlers.process_setup([SetupInvocation {
            name: "setup",
            context: &mut first,
        }]))
        .unwrap();
        assert_eq!(&*trace.borrow(), &["resumed"]);
    }

    #[test]
    fn pending_handler_blocks_later_invocations_and_can_be_cancelled() {
        let mut handlers = IndexingHandlers::default();
        handlers.register_event("event".into(), event).unwrap();
        let (mut first, mut second, trace) = contexts();
        let mut future = Box::pin(handlers.process_events([
            EventInvocation {
                name: "event",
                context: &mut first,
                event: &1,
            },
            EventInvocation {
                name: "event",
                context: &mut second,
                event: &2,
            },
        ]));
        let waker = Waker::from(Arc::new(ThreadWake(thread::current())));
        let mut context = Context::from_waker(&waker);
        assert!(future.as_mut().poll(&mut context).is_pending());
        assert_eq!(&*trace.borrow(), &["start 1 1"]);
        drop(future);
        assert_eq!(&*trace.borrow(), &["start 1 1"]);
    }

    #[test]
    fn handler_failure_is_propagated_and_later_events_are_not_started() {
        let mut handlers = IndexingHandlers::default();
        handlers.register_event("event".into(), event).unwrap();
        let (mut first, mut second, trace) = contexts();
        first.fail = true;
        let result = block_on(handlers.process_events([
            EventInvocation {
                name: "event",
                context: &mut first,
                event: &1,
            },
            EventInvocation {
                name: "event",
                context: &mut second,
                event: &2,
            },
        ]));
        assert_eq!(
            result,
            Err(InvocationError::Handler {
                name: "event".into(),
                source: "event failed",
            })
        );
        assert_eq!(&*trace.borrow(), &["start 1 1"]);
    }

    #[test]
    fn completed_effects_remain_after_a_later_failure() {
        let mut handlers = IndexingHandlers::default();
        handlers.register_event("event".into(), event).unwrap();
        let (mut first, mut second, trace) = contexts();
        let mut third = first.clone();
        second.fail = true;
        let future = handlers.process_events([
            EventInvocation {
                name: "event",
                context: &mut first,
                event: &1,
            },
            EventInvocation {
                name: "event",
                context: &mut second,
                event: &2,
            },
            EventInvocation {
                name: "event",
                context: &mut third,
                event: &3,
            },
        ]);
        assert!(trace.borrow().is_empty());
        assert_eq!(
            block_on(future),
            Err(InvocationError::Handler {
                name: "event".into(),
                source: "event failed",
            })
        );
        assert_eq!(&*trace.borrow(), &["start 1 1", "done 1 1", "start 2 2"]);
    }

    #[test]
    fn setup_failure_is_propagated_and_stops_the_batch() {
        let mut handlers = IndexingHandlers::<_, u8, _>::default();
        handlers.register_setup("setup".into(), setup).unwrap();
        let (mut first, mut second, trace) = contexts();
        first.fail = true;
        let result = block_on(handlers.process_setup([
            SetupInvocation {
                name: "setup",
                context: &mut first,
            },
            SetupInvocation {
                name: "setup",
                context: &mut second,
            },
        ]));
        assert_eq!(
            result,
            Err(InvocationError::Handler {
                name: "setup".into(),
                source: "setup failed",
            })
        );
        assert_eq!(&*trace.borrow(), &["setup 1"]);
    }

    #[test]
    fn duplicates_share_a_namespace_and_preserve_the_first_handler() {
        let mut handlers = IndexingHandlers::default();
        handlers.register_setup("same".into(), setup).unwrap();
        assert_eq!(
            handlers.register_setup("same".into(), setup),
            Err(DuplicateHandler("same".into()))
        );
        assert_eq!(
            handlers.register_event("same".into(), event),
            Err(DuplicateHandler("same".into()))
        );
        handlers.register_event("event".into(), event).unwrap();
        assert_eq!(
            handlers.register_event("event".into(), event),
            Err(DuplicateHandler("event".into()))
        );
        assert_eq!(
            handlers.register_setup("event".into(), setup),
            Err(DuplicateHandler("event".into()))
        );
        let (mut first, mut second, trace) = contexts();
        block_on(handlers.process_setup([SetupInvocation {
            name: "same",
            context: &mut first,
        }]))
        .unwrap();
        block_on(handlers.process_events([EventInvocation {
            name: "event",
            context: &mut second,
            event: &7,
        }]))
        .unwrap();
        assert_eq!(
            &*trace.borrow(),
            &["setup 1", "setup done 1", "start 2 7", "done 2 7"]
        );
    }

    #[test]
    fn missing_and_wrong_kind_are_errors_and_stop_dispatch() {
        let mut handlers = IndexingHandlers::default();
        handlers.register_setup("setup".into(), setup).unwrap();
        handlers.register_event("event".into(), event).unwrap();
        let (mut first, mut second, trace) = contexts();
        assert_eq!(
            block_on(handlers.process_events([
                EventInvocation {
                    name: "missing",
                    context: &mut first,
                    event: &1
                },
                EventInvocation {
                    name: "event",
                    context: &mut second,
                    event: &2
                },
            ])),
            Err(InvocationError::UnknownHandler("missing".into()))
        );
        assert_eq!(
            block_on(handlers.process_events([EventInvocation {
                name: "setup",
                context: &mut first,
                event: &1
            },])),
            Err(InvocationError::WrongHandlerKind("setup".into()))
        );
        assert_eq!(
            block_on(handlers.process_setup([SetupInvocation {
                name: "event",
                context: &mut first
            },])),
            Err(InvocationError::WrongHandlerKind("event".into()))
        );
        assert_eq!(
            block_on(handlers.process_setup([SetupInvocation {
                name: "missing",
                context: &mut first
            },])),
            Err(InvocationError::UnknownHandler("missing".into()))
        );
        assert!(trace.borrow().is_empty());
    }

    #[test]
    fn empty_batches_succeed() {
        let handlers = IndexingHandlers::<TestContext, u8, &'static str>::default();
        assert_eq!(block_on(handlers.process_setup([])), Ok(()));
        assert_eq!(block_on(handlers.process_events([])), Ok(()));
    }
}
