
use actix::prelude::*;
use crate::transport::connector::*;

pub struct WorkerBackendConnectorParameters;

impl ConnectorParameters for WorkerBackendConnectorParameters {
    fn name() -> &'static str {
        "worker_backend_connector"
    }

    fn router() -> &'static str {
        "inproc://router"
    }
}

pub type WorkerBackendConnector = Connector<WorkerBackendConnectorParameters>;

pub fn start() -> Addr<WorkerBackendConnector>
{
    WorkerBackendConnector::from_registry()
}
