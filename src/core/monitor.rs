use actix::prelude::*;
use crate::core::timer::Timer;

/// Used in conjunction with `ReportStatusTimer` to notify
/// `Handler<ReportStatusMessage>` to submit its status report.
#[derive(Clone, Default)]
pub struct ReportStatusMessage {
}

impl Message for ReportStatusMessage {
    type Result = ();
}

pub type ReportStatusTimer = Timer<ReportStatusMessage>;

/// Used in conjunction with `RegularCheckTimer` to notify
/// `Handler<RegularCheckMessage>` to perform a regular check of any kind.
#[derive(Clone, Default)]
pub struct RegularCheckMessage {
}

impl Message for RegularCheckMessage {
    type Result = ();
}

pub type RegularCheckTimer = Timer<RegularCheckMessage>;
