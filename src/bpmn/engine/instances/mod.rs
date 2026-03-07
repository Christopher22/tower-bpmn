mod instance;

use dashmap::DashMap;
use std::{collections::HashMap, sync::Arc};

pub use instance::{Instance, InstanceId};

use crate::{SharedHistory, TokenId};

/// Public snapshot of a process instance tracked by the runtime.
#[derive(Debug, Clone, serde::Serialize, schemars::JsonSchema)]
pub struct RuntimeInstance {
    /// Instance identifier.
    pub id: InstanceId,
    /// BPMN process name.
    pub process: String,
    /// Current execution status.
    pub status: InstanceStatus,
    #[serde(skip_serializing)]
    #[schemars(skip)]
    #[allow(unused)]
    history: SharedHistory,
    #[serde(skip_serializing)]
    #[schemars(skip)]
    last_tasks: HashMap<TokenId, String>,
}

#[derive(Debug, Clone)]
pub struct Instances(Arc<DashMap<InstanceId, RuntimeInstance>>);

impl Instances {
    /// Create a new instance object.
    pub fn new() -> Self {
        Instances(Arc::new(DashMap::new()))
    }

    /// Returns an iterator over tracked process instances.
    pub fn iter(&self) -> impl Iterator<Item = impl std::ops::Deref<Target = RuntimeInstance>> {
        self.0.iter()
    }

    /// Returns one tracked instance by id.
    pub fn get(&self, id: InstanceId) -> Option<impl std::ops::Deref<Target = RuntimeInstance>> {
        self.0.get(&id)
    }
}

impl super::Observer for Instances {
    fn report_start(&self, instance_id: InstanceId, process: String, history: SharedHistory) {
        self.0.insert(
            instance_id,
            RuntimeInstance {
                id: instance_id,
                process: process.to_string(),
                status: InstanceStatus::Running,
                history,
                last_tasks: HashMap::new(),
            },
        );
    }

    fn report_task_completed(&self, instance_id: InstanceId, token_id: TokenId, task: &str) {
        match self.0.entry(instance_id) {
            dashmap::Entry::Occupied(mut entry) => entry
                .get_mut()
                .last_tasks
                .insert(token_id, task.to_string()),
            dashmap::Entry::Vacant(_) => {
                log::warn!("Received task completion report for unknown instance {instance_id:?}");
                return;
            }
        };
    }

    fn report_end(&self, instance_id: InstanceId) {
        match self.0.entry(instance_id) {
            dashmap::Entry::Occupied(mut entry) => {
                entry.get_mut().status = InstanceStatus::Completed
            }
            dashmap::Entry::Vacant(_) => {
                log::warn!("Received end report for unknown instance {instance_id:?}");
                return;
            }
        };
    }

    fn report_stop(&self, instance_id: InstanceId) {
        match self.0.entry(instance_id) {
            dashmap::Entry::Occupied(mut entry) => entry.get_mut().status = InstanceStatus::Stopped,
            dashmap::Entry::Vacant(_) => {
                log::warn!("Received stop report for unknown instance {instance_id:?}");
                return;
            }
        };
    }
}

/// Status of a runtime-tracked process instance.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum InstanceStatus {
    /// Instance is currently executing.
    Running,
    /// Instance finished successfully.
    Completed,
    /// Instance was explicitly stopped.
    Stopped,
}
