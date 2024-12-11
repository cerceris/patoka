use patoka;
use actix;

use actix::prelude::*;
use patoka::core::timer::Timer;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

#[derive(Clone)]
struct IntervalMessage;

impl Message for IntervalMessage {
    type Result = ();
}

impl Default for IntervalMessage {
    fn default() -> Self {
        IntervalMessage {
        }
    }
}

#[derive(Clone)]
struct TimeoutMessage;

impl Message for TimeoutMessage {
    type Result = ();
}

impl Default for TimeoutMessage {
    fn default() -> Self {
        TimeoutMessage {
        }
    }
}

struct TimeoutActor {
    interval_timer: Timer<IntervalMessage>,
    interval_timer_duration: Duration,
    interval_messages_rx: Arc<AtomicUsize>,
    timeout_timer: Timer<TimeoutMessage>,
    timeout_timer_duration: Duration,
    timeout_messages_rx: Arc<AtomicUsize>,
    life_duration: Duration
}

impl TimeoutActor {
    pub fn new(
        interval_messages_rx: Arc<AtomicUsize>,
        timeout_messages_rx: Arc<AtomicUsize>,
        interval_timer_duration: Duration,
        timeout_timer_duration: Duration,
        life_duration: Duration,
    ) -> Self {
        Self {
            interval_timer: Timer::new(),
            interval_timer_duration,
            interval_messages_rx,
            timeout_timer: Timer::new(),
            timeout_timer_duration,
            timeout_messages_rx,
            life_duration,
        }
    }
}

impl Actor for TimeoutActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        println!("Started TimeoutActor.");

        self.interval_timer.start::<Self>(
            ctx,
            self.interval_timer_duration,
        );

        self.timeout_timer.start::<Self>(
            ctx,
            self.timeout_timer_duration,
        );

        ctx.notify_later(
            StopMessage,
            self.life_duration,
        );
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        println!("Stopped TimeoutActor.");
    }
}

impl Handler<IntervalMessage> for TimeoutActor {
    type Result = ();

    fn handle(
        &mut self,
        _msg: IntervalMessage,
        ctx: &mut Self::Context
    ) -> Self::Result {
        self.interval_messages_rx.fetch_add(1, Ordering::Relaxed);
        println!("Received interval message: {:?}", self.interval_messages_rx);

        self.interval_timer.reset::<Self>(ctx);
        self.timeout_timer.reset::<Self>(ctx);
    }
}

impl Handler<TimeoutMessage> for TimeoutActor {
    type Result = ();

    fn handle(
        &mut self,
        _msg: TimeoutMessage,
        _ctx: &mut Self::Context
    ) -> Self::Result {
        self.timeout_messages_rx.fetch_add(1, Ordering::Relaxed);
        println!("Received timeout message: {:?}", self.timeout_messages_rx);
    }
}

struct StopMessage;

impl Message for StopMessage {
    type Result = ();
}

impl Handler<StopMessage> for TimeoutActor {
    type Result = ();

    fn handle(
        &mut self,
        _msg: StopMessage,
        _ctx: &mut Self::Context
    ) -> Self::Result {
        println!("Received stop message.");
        System::current().stop();
    }
}


#[test]
fn test_interval_timer() {
    let interval_messages_rx = Arc::new(AtomicUsize::new(0));
    let interval_messages_rx_0 = Arc::clone(&interval_messages_rx);

    let timeout_messages_rx = Arc::new(AtomicUsize::new(0));
    let timeout_messages_rx_0 = Arc::clone(&timeout_messages_rx);

    let system = System::new();

    system.block_on(async {
        let actor = TimeoutActor::new(
            interval_messages_rx_0,
            timeout_messages_rx_0,
            Duration::from_millis(500),
            Duration::from_secs(2),
            Duration::from_millis(3200),
        );
        actor.start();
    });

    system.run();

    assert_eq!(interval_messages_rx.load(Ordering::Relaxed), 6);
    assert_eq!(timeout_messages_rx.load(Ordering::Relaxed), 0);
}

#[test]
fn test_timeout_timer() {
    let interval_messages_rx = Arc::new(AtomicUsize::new(0));
    let interval_messages_rx_0 = Arc::clone(&interval_messages_rx);

    let timeout_messages_rx = Arc::new(AtomicUsize::new(0));
    let timeout_messages_rx_0 = Arc::clone(&timeout_messages_rx);

    let system = System::new();

    system.block_on(async {
        let actor = TimeoutActor::new(
            interval_messages_rx_0,
            timeout_messages_rx_0,
            Duration::from_millis(2000),
            Duration::from_secs(1),
            Duration::from_millis(1200),
        );
        actor.start();
    });

    system.run();

    assert_eq!(interval_messages_rx.load(Ordering::Relaxed), 0);
    assert_eq!(timeout_messages_rx.load(Ordering::Relaxed), 1);

}
