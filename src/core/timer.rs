use actix::prelude::*;
use std::time::Duration;

#[derive(Clone)]
pub struct Timer<M>
where
    M: Message + Send + Default + Clone + 'static,
    M::Result: Send,
{
    timeout_message: M,
    handle: Option<SpawnHandle>,
    duration: Option<Duration>,
}

impl<M> Timer<M>
where
    M: Message + Send + Default + Clone + 'static,
    M::Result: Send,
{
    pub fn new() -> Self {
        Self {
            timeout_message: M::default(),
            handle: None,
            duration: None,
        }
    }

    pub fn with_duration(duration: Duration) -> Self {
        Self {
            timeout_message: M::default(),
            handle: None,
            duration: Some(duration),
        }
    }

    pub fn new_s(secs: u64) -> Self {
        Self::with_duration(Duration::from_secs(secs))
    }

    pub fn new_ms(msecs: u64) -> Self {
        Self::with_duration(Duration::from_millis(msecs))
    }

    pub fn start<A>(&mut self, ctx: &mut A::Context, duration: Duration)
    where
        A: Actor<Context=Context<A>>,
        A: Handler<M>,
    {
        self.cancel::<A>(ctx);
        self.duration = Some(duration.clone());
        self.handle = Some(
            ctx.notify_later(self.timeout_message.clone(), duration)
        );
    }

    pub fn cancel<A>(&mut self, ctx: &mut A::Context)
    where
        A: Actor<Context=Context<A>>,
        A: Handler<M>,
    {
        if let Some(h) = self.handle {
            ctx.cancel_future(h);
            self.handle = None;
        }
    }

    pub fn reset<A>(&mut self, ctx: &mut A::Context)
    where
        A: Actor<Context=Context<A>>,
        A: Handler<M>,
    {
        if let Some(d) = self.duration {
            self.start::<A>(ctx, d);
        }
    }
}
