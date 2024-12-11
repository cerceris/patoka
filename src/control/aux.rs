use serde;

use crate::{
    center::send::*,
    worker::{
        task::GenTaskDefinition,
        tracker::*,
    },
};

pub fn update_task_params<P: serde::Serialize>(
    task_definition: &mut GenTaskDefinition<P>,
    params: P,
) {
    task_definition.params = params;

    send_center_task_updated(
        &task_definition.task_uuid,
        &task_definition,
        &task_definition.name,
    );
}
