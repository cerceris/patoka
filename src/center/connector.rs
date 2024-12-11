use actix::prelude::*;

use crate::transport::connector::*;

pub struct CenterConnectorParameters;

impl ConnectorParameters for CenterConnectorParameters {
    fn name() -> &'static str {
        "center_connector"
    }

    fn router() -> &'static str {
        "inproc://center_router"
    }
}

pub type CenterConnector = Connector<CenterConnectorParameters>;

pub fn start() -> Addr<CenterConnector> {
    CenterConnector::from_registry()
}
