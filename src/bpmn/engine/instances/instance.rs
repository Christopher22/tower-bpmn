use uuid::Uuid;

use crate::{
    ExtendedExecutor, RegisteredProcess, Runtime, SharedHistory, State, Token, Value,
    executor::Executor,
};

use super::super::Observer;

/// The ID of a BPMN process instance.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    schemars::JsonSchema,
)]
pub struct InstanceId(Uuid);

impl InstanceId {
    /// Create a new instance ID.
    pub(crate) fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl std::fmt::Display for InstanceId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::str::FromStr for InstanceId {
    type Err = uuid::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Uuid::parse_str(s).map(Self)
    }
}

/// A instance of a process running in the background.
pub struct Instance<'a, A: ExtendedExecutor> {
    /// The unique identifier of this process instance.
    pub id: InstanceId,
    engine: &'a Runtime<A>,
    process: &'a RegisteredProcess<A>,
    simulation: <A as Executor<crate::petri_net::Marking<State>>>::TaskHandle,
}

impl<'a, A: ExtendedExecutor> std::fmt::Debug for Instance<'a, A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Instance")
            .field("uuid", &self.id)
            .finish_non_exhaustive()
    }
}

impl<'a, A: ExtendedExecutor + 'static> Instance<'a, A> {
    pub(crate) fn new<V: Value>(
        uuid: InstanceId,
        engine: &'a Runtime<A>,
        process_raw: &'a RegisteredProcess<A>,
        input: V,
        shared_history: SharedHistory,
    ) -> Self {
        // Create the process simulation and spawn it in the executor.
        let simulation = engine.executor.spawn_task(
            process_raw
                .instantiate(engine.executor.clone(), input, shared_history)
                .run(),
        );

        Self {
            engine,
            process: process_raw,
            id: uuid,
            simulation,
        }
    }

    /// Wait for the process instance to complete and return the final context. The context can be used to query the final state of the process.
    pub async fn wait_for_completion(self) -> Token {
        let instance_id = self.id;
        let mut marking: crate::petri_net::Marking<State> =
            <A as Executor<crate::petri_net::Marking<State>>>::join(
                &self.engine.executor,
                self.simulation,
            )
            .await
            .expect("process simulation failed to complete");
        match std::mem::take(&mut marking[self.process.end]) {
            State::Completed(token) => {
                self.engine.instances.report_end(instance_id);
                token
            }
            _ => panic!("process completed without token at the end place"),
        }
    }

    /// Stop the process instance and return the context for resuming later.
    pub fn stop(self) {
        self.engine.instances.report_stop(self.id);
        futures::executor::block_on(async {
            <A as Executor<crate::petri_net::Marking<State>>>::stop(
                &self.engine.executor,
                self.simulation,
            )
            .await;
        });
    }
}
