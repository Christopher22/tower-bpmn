use std::sync::Arc;

use parking_lot::RwLock;
use schemars::JsonSchema;

use crate::bpmn::{Value, messages::Participant};

/// Runtime type metadata used to validate step inputs and outputs.
#[derive(Debug, Clone, PartialEq, JsonSchema)]
pub struct Type {
    /// The expected type of the input value.
    pub schema: schemars::Schema,
    #[schemars(skip)]
    type_id: std::any::TypeId,
}

impl Type {
    /// Create a new type for a value.
    pub fn new<V: Value>() -> Self {
        let mut generator = schemars::SchemaGenerator::default();
        Self {
            schema: V::json_schema(&mut generator),
            type_id: std::any::TypeId::of::<V>(),
        }
    }

    /// Check if the external step expects a value of the given type.
    pub fn matches<V: Value>(&self) -> bool {
        self.type_id == std::any::TypeId::of::<V>()
    }
}

/// A step in a process which is waiting for external input.
#[derive(Debug, Clone, PartialEq, JsonSchema)]
pub struct ExternalStepData {
    /// The name of the step.
    pub name: String,
    /// The expected type of input.
    pub input: Type,
    /// The expected sender.
    pub expected_participant: Participant,
}

impl ExternalStepData {
    /// Create a new external step with the given name, schema and expected sender.
    pub fn new<V: Value>(name: impl Into<String>, expected_participant: Participant) -> Self {
        Self {
            name: name.into(),
            input: Type::new::<V>(),
            expected_participant,
        }
    }
}

impl AsRef<str> for ExternalStepData {
    fn as_ref(&self) -> &str {
        self.name.as_str()
    }
}

/// A collection of steps in a process.
#[derive(Debug, Clone)]
pub struct Steps(Vec<Arc<InnerStep>>);

impl Steps {
    /// Try to create a new collection of steps from a list of step names.
    #[cfg(test)]
    pub(crate) fn new(steps: impl Iterator<Item = impl Into<String>>) -> Result<Self, InvalidStep> {
        use crate::bpmn::storage::NoOutput;

        let builder = StepsBuilder::default();
        builder.add_start::<NoOutput>(Participant::Nobody)?;
        for step in steps {
            builder.add::<NoOutput>(step)?;
        }
        builder.add_end(Type::new::<NoOutput>());
        Ok(builder
            .build()
            .expect("StepsBuilder should not fail when all steps are valid"))
    }

    /// Get the start step of the process.
    pub fn start(&self) -> ExternalStep {
        ExternalStep(Step(
            self.0
                .first()
                .expect("Steps must contain a start step")
                .clone(),
        ))
    }

    /// Get the end step of the process.
    pub fn end(&self) -> Step {
        self.0
            .last()
            .expect("Steps must contain an end step")
            .into()
    }

    /// Try to get a step by name. Returns `None` if the step does not exist.
    pub fn get(&self, step: &str) -> Option<Step> {
        self.0.iter().find(|s| s.as_str() == step).map(Step::from)
    }

    /// Get an iterator over all the steps in the process.
    pub fn steps(&self) -> impl Iterator<Item = &str> {
        self.0.iter().map(|s| s.as_str())
    }

    /// Get an iterator over all the external steps in the process.
    /// This does NOT include the start step, even though it is technically an external step.
    pub fn external_steps(&self) -> impl Iterator<Item = &ExternalStepData> {
        self.0.iter().filter_map(|s| match s.as_ref() {
            InnerStep::External(external, _) => Some(external),
            _ => None,
        })
    }
}

impl TryFrom<StepsBuilder> for Steps {
    type Error = UnfinishedBuilder;

    fn try_from(builder: StepsBuilder) -> Result<Self, Self::Error> {
        let steps = builder.0.read();
        if !steps.iter().any(|s| s.as_str() == Step::START)
            || !steps.iter().any(|s| s.as_str() == Step::END)
        {
            return Err(UnfinishedBuilder);
        }

        let mut steps = steps.clone();
        // Sort the steps to ensure a consistent order, with "Start" always first and "End" always last.
        steps.sort_by(|a, b| {
            if a.as_str() == Step::START || b.as_str() == Step::END {
                std::cmp::Ordering::Less
            } else if a.as_str() == Step::END || b.as_str() == Step::START {
                std::cmp::Ordering::Greater
            } else {
                a.as_str().cmp(b.as_str())
            }
        });
        Ok(Steps(steps))
    }
}

impl serde::Serialize for Steps {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.collect_seq(self.0.iter().map(|value| value.as_str()))
    }
}

/// A builder for creating a collection of steps. This is used to ensure that the steps are valid and to prevent race conditions when adding steps from multiple branches.
#[derive(Debug, Clone)]
pub(crate) struct StepsBuilder(Arc<RwLock<Vec<Arc<InnerStep>>>>);

impl Default for StepsBuilder {
    fn default() -> Self {
        StepsBuilder(Arc::new(RwLock::new(vec![])))
    }
}

impl StepsBuilder {
    /// Add a step to the process. The name cannot be empty, and cannot be "Start" or "End". Returns an error if the name is invalid or already exists.
    pub fn add<O: Value>(&self, step: impl Into<String>) -> Result<Step, InvalidStep> {
        self.add_step_internal::<O, _, _>(step.into(), InnerStep::Internal)
    }

    /// Add a step externable callable to the process. The name cannot be empty, and cannot be "Start" or "End". Returns an error if the name is invalid or already exists.
    pub fn add_external<O: Value>(
        &self,
        external_step: ExternalStepData,
    ) -> Result<ExternalStep, InvalidStep> {
        Ok(ExternalStep(self.add_step_internal::<O, _, _>(
            external_step,
            InnerStep::External,
        )?))
    }

    fn add_step_internal<O: Value, V: AsRef<str>, C: FnOnce(V, Type) -> InnerStep>(
        &self,
        step_owned: V,
        callback: C,
    ) -> Result<Step, InvalidStep> {
        let step = step_owned.as_ref();
        if step == Step::START || step == Step::END {
            return Err(InvalidStep::Reserved);
        } else if step.trim().is_empty() {
            return Err(InvalidStep::Empty);
        }

        // Lock the container before checking for existing names to prevent race conditions.
        let mut lock = self.0.write();
        if lock.iter().any(|s| s.as_str() == step) {
            return Err(InvalidStep::Existing);
        }

        let reference = Arc::new(callback(step_owned, Type::new::<O>()));
        lock.push(reference.clone());
        Ok(Step(reference))
    }

    /// Build the steps. This will return an error if not start or end were added.
    pub(super) fn build(self) -> Result<Steps, UnfinishedBuilder> {
        Steps::try_from(self)
    }

    /// Add a start step. This is only called internally.
    pub(super) fn add_start<V: Value>(
        &self,
        owner: Participant,
    ) -> Result<ExternalStep, InvalidStep> {
        let mut lock = self.0.write();

        if lock
            .iter()
            .any(|s| matches!(s.as_ref(), InnerStep::Start(_)))
        {
            return Err(InvalidStep::Existing);
        }

        let reference = Arc::new(InnerStep::Start(ExternalStepData::new::<V>(
            Step::START,
            owner,
        )));
        lock.push(reference.clone());
        Ok(ExternalStep(Step(reference)))
    }

    /// Add an end step. This is only called internally.
    pub(super) fn add_end(&self, output_type: Type) -> Step {
        let mut lock = self.0.write();
        Step(
            lock.iter()
                .find(|s| matches!(s.as_ref(), InnerStep::End(_)))
                .cloned()
                .unwrap_or_else(|| {
                    let reference = Arc::new(InnerStep::End(output_type));
                    lock.push(reference.clone());
                    reference
                }),
        )
    }
}

/// An builder which does not have a start or end yet.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct UnfinishedBuilder;

impl std::fmt::Display for UnfinishedBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("StepsBuilder must contain start and end steps before building")
    }
}

impl std::error::Error for UnfinishedBuilder {}

#[derive(Debug)]
enum InnerStep {
    Start(ExternalStepData),
    End(Type),
    Internal(String, Type),
    External(ExternalStepData, Type),
}

impl InnerStep {
    fn output(&self) -> &Type {
        match self {
            InnerStep::Start(external) => &external.input,
            InnerStep::End(output) => output,
            InnerStep::Internal(_, output) => output,
            InnerStep::External(_, output) => output,
        }
    }
}

impl InnerStep {
    fn as_str(&self) -> &str {
        match self {
            InnerStep::Start(_) => Step::START,
            InnerStep::End(_) => Step::END,
            InnerStep::Internal(s, _) => s.as_str(),
            InnerStep::External(external, _) => external.name.as_str(),
        }
    }
}

impl std::fmt::Display for InnerStep {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A step in a process. This equals only(!) the same step within the same process, even if the name is the same.
#[derive(Debug, Clone)]
pub struct Step(Arc<InnerStep>);

impl Step {
    /// Reserved start name that cannot be used for user-defined steps.
    pub const START: &'static str = "Start";

    /// Reserved end name that cannot be used for user-defined steps.
    pub const END: &'static str = "End";

    /// Check if the step is the start step.
    pub fn is_start(&self) -> bool {
        matches!(*self.0, InnerStep::Start(_))
    }

    /// Check if the step is the end step.
    pub fn is_end(&self) -> bool {
        matches!(*self.0, InnerStep::End(_))
    }

    /// Get the name of the step.
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    /// Get the additional data if this is an external step.
    pub fn external_data(&self) -> Option<&ExternalStepData> {
        match self.0.as_ref() {
            InnerStep::Start(external) => Some(external),
            InnerStep::External(external, _) => Some(external),
            _ => None,
        }
    }

    /// Get the output of the step.
    pub fn output(&self) -> &Type {
        self.0.output()
    }
}

impl From<Arc<InnerStep>> for Step {
    fn from(inner: Arc<InnerStep>) -> Self {
        Step(inner)
    }
}

impl<'a> From<&'a Arc<InnerStep>> for Step {
    fn from(inner: &'a Arc<InnerStep>) -> Self {
        Step(inner.clone())
    }
}

impl AsRef<str> for Step {
    fn as_ref(&self) -> &str {
        self.0.as_str()
    }
}

impl std::fmt::Display for Step {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0.as_str())
    }
}

impl PartialEq for Step {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl<'a> From<&'a Step> for std::borrow::Cow<'a, str> {
    fn from(step: &'a Step) -> Self {
        std::borrow::Cow::Borrowed(step.as_str())
    }
}

impl From<Step> for std::borrow::Cow<'static, str> {
    fn from(step: Step) -> Self {
        std::borrow::Cow::Owned((*step.0).to_string())
    }
}

impl std::hash::Hash for Step {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        Arc::as_ptr(&self.0).hash(state);
    }
}

impl PartialEq<str> for Step {
    fn eq(&self, other: &str) -> bool {
        self.0.as_str() == other
    }
}

impl Eq for Step {}

impl serde::Serialize for Step {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.0.as_str())
    }
}

impl JsonSchema for Step {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "Step".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        concat!(module_path!(), "::Step").into()
    }

    fn json_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({
            "type": "string",
        })
    }
}

/// A step with associated ExternalStepData, guaranteed through the type system. It is easily convertible to a normal Step, but the additional data can be accessed without needing to check.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ExternalStep(Step);

impl From<ExternalStep> for Step {
    fn from(external: ExternalStep) -> Self {
        external.0
    }
}

impl std::ops::Deref for ExternalStep {
    type Target = Step;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AsRef<ExternalStepData> for ExternalStep {
    fn as_ref(&self) -> &ExternalStepData {
        self.0
            .external_data()
            .expect("ExternalStep must have ExternalStepData")
    }
}

/// Errors emitted when building steps.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum InvalidStep {
    /// The name is reserved and cannot be used.
    Reserved,
    /// The name is empty or only whitespace.
    Empty,
    /// The name already exists in the process.
    Existing,
}

impl std::fmt::Display for InvalidStep {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            InvalidStep::Reserved => {
                write!(f, " '{}' and '{}' are reserved", Step::START, Step::END)
            }
            InvalidStep::Empty => write!(f, "The name cannot be empty"),
            InvalidStep::Existing => write!(f, "The name already exists"),
        }
    }
}

impl std::error::Error for InvalidStep {}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize, JsonSchema)]
    struct DummyValue {
        value: String,
    }

    #[test]
    fn test_step_equality() {
        const SAME_NAME: &str = "Task A";

        let (s1, s2) = {
            let step_process_1 = StepsBuilder::default();
            let step_process_2 = StepsBuilder::default();

            assert_ne!(
                step_process_1.add_start::<()>(Participant::Everyone),
                step_process_2.add_start::<()>(Participant::Everyone)
            );
            assert_ne!(
                step_process_1.add_end(Type::new::<()>()),
                step_process_2.add_end(Type::new::<()>())
            );
            assert_ne!(
                step_process_1.add::<()>(SAME_NAME).unwrap(),
                step_process_2.add::<()>(SAME_NAME).unwrap()
            );

            (
                step_process_1.build().unwrap(),
                step_process_2.build().unwrap(),
            )
        };

        assert_ne!(s1.start(), s2.start());
        assert_ne!(s1.end(), s2.end());
        assert_ne!(s1.get(SAME_NAME).unwrap(), s2.get(SAME_NAME).unwrap());

        // However, the same step within the same process should be equal.
        assert_eq!(s1.get(SAME_NAME).unwrap(), s1.get(SAME_NAME).unwrap());
        assert_eq!(s1.start(), s1.start());
        assert_eq!(s1.end(), s1.end());
    }

    #[test]
    fn test_step_diverging_builder() {
        let builder = StepsBuilder::default();
        builder.add_start::<()>(Participant::Everyone).unwrap();
        builder.add_end(Type::new::<()>());
        builder.add::<()>("Task A").unwrap();

        let other_builder = builder.clone();
        builder.add::<()>("Task B").unwrap();
        other_builder.add::<()>("Task C").unwrap();

        let steps = builder.build().unwrap();
        other_builder.add::<()>("Task D").unwrap();

        let steps = steps.steps().collect::<Vec<_>>(); // Should not panic or cause issues with the other builder.
        assert_eq!(steps, vec!["Start", "Task A", "Task B", "Task C", "End"]); // Task c should be included because it was added before building, but Task D should not be included because it was added after building.
    }

    #[test]
    fn test_builder_happy_path() {
        let builder = StepsBuilder::default();
        builder.add_start::<()>(Participant::Everyone).unwrap();
        builder.add_end(Type::new::<()>());
        let step_a = builder
            .add::<()>("Process Data")
            .expect("Should allow adding a valid step");

        let steps = builder
            .build()
            .expect("Build should succeed with Start and End");

        // Verify start and end accessors
        assert_eq!(Step::from(steps.start()), *Step::START);
        assert_eq!(steps.end(), *Step::END);

        // Verify retrieval
        assert_eq!(steps.get("Process Data").unwrap(), step_a);
    }

    #[test]
    fn test_builder_validation_errors() {
        let builder = StepsBuilder::default();

        // 1. Test reserved names
        assert_eq!(
            builder.add::<()>("Start").unwrap_err(),
            InvalidStep::Reserved
        );
        assert_eq!(builder.add::<()>("End").unwrap_err(), InvalidStep::Reserved);

        // 2. Test empty name
        assert_eq!(builder.add::<()>("").unwrap_err(), InvalidStep::Empty);
        assert_eq!(builder.add::<()>("   ").unwrap_err(), InvalidStep::Empty);

        // 3. Test duplicates
        builder.add::<()>("Unique").unwrap();
        assert_eq!(
            builder.add::<()>("Unique").unwrap_err(),
            InvalidStep::Existing
        );
    }

    #[test]
    fn test_build_missing_requirements() {
        // Missing both
        let builder = StepsBuilder::default();
        assert_eq!(builder.build().unwrap_err(), UnfinishedBuilder);

        // Missing End
        let builder = StepsBuilder::default();
        builder.add_start::<()>(Participant::Everyone).unwrap();
        assert_eq!(builder.build().unwrap_err(), UnfinishedBuilder);

        // Missing Start
        let builder = StepsBuilder::default();
        builder.add_end(Type::new::<()>());
        assert_eq!(builder.build().unwrap_err(), UnfinishedBuilder);
    }

    #[test]
    fn test_step_order_logic() {
        let builder = StepsBuilder::default();
        // Add in "wrong" order
        builder.add_end(Type::new::<()>());
        builder.add::<()>("Middle Task").unwrap();
        builder.add::<()>("Middle Task 2").unwrap();
        builder.add_start::<()>(Participant::Everyone).unwrap();

        let steps = builder.build().expect("Build should succeed");
        let collected: Vec<&str> = steps.steps().collect();

        // Ensure Start is first and End is last regardless of addition order
        assert_eq!(collected.first().unwrap(), &Step::START);
        assert_eq!(collected.last().unwrap(), &Step::END);
    }

    #[test]
    fn test_step_iterator() {
        let builder = StepsBuilder::default();
        builder.add_start::<()>(Participant::Everyone).unwrap();
        builder.add::<()>("A").unwrap();
        builder.add::<()>("B").unwrap();
        builder.add_end(Type::new::<()>());

        let steps = builder.build().unwrap();
        let mut iter = steps.steps();

        assert_eq!(iter.next(), Some(Step::START));
        assert_eq!(iter.next(), Some("A"));
        assert_eq!(iter.next(), Some("B"));
        assert_eq!(iter.next(), Some(Step::END));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn test_external_step_creation() {
        let participant = Participant::Entity(crate::bpmn::messages::Entity::new("User1"));
        let step = ExternalStepData::new::<DummyValue>("ExternalTask", participant.clone());

        assert_eq!(step.name, "ExternalTask");
        assert_eq!(step.expected_participant, participant);
        // Schema should not be empty
        let schema_json = serde_json::to_value(&step.input.schema).unwrap();
        assert!(schema_json.is_object());
    }

    #[test]
    fn test_steps_get_missing() {
        let builder = StepsBuilder::default();
        builder.add_start::<()>(Participant::Everyone).unwrap();
        builder.add::<()>("A").unwrap();
        builder.add_end(Type::new::<()>());

        let steps = builder.build().unwrap();

        assert!(steps.get("DoesNotExist").is_none());
    }

    #[test]
    fn test_external_steps_collection() {
        let builder = StepsBuilder::default();
        builder.add_start::<()>(Participant::Everyone).unwrap();

        let participant = Participant::Entity(crate::bpmn::messages::Entity::new("UserX"));
        let ext = ExternalStepData::new::<DummyValue>("External", participant.clone());

        builder.add_external::<DummyValue>(ext.clone()).unwrap();
        builder.add::<()>("Internal").unwrap();
        builder.add_end(Type::new::<()>());

        let steps = builder.build().unwrap();

        let collected: Vec<&ExternalStepData> = steps.external_steps().collect();
        assert_eq!(collected.len(), 1);
        assert_eq!(collected[0], &ext);
    }

    #[test]
    fn test_step_is_start_and_is_end() {
        let builder = StepsBuilder::default();
        let s = builder.add_start::<()>(Participant::Everyone).unwrap();
        let e = builder.add_end(Type::new::<()>());

        assert!(s.is_start());
        assert!(!s.is_end());
        assert!(e.is_end());
        assert!(!e.is_start());
    }

    #[test]
    fn test_step_as_external_none() {
        let builder = StepsBuilder::default();
        builder.add_start::<()>(Participant::Everyone).unwrap();
        builder.add::<()>("Internal").unwrap();
        builder.add_end(Type::new::<()>());
        let steps = builder.build().unwrap();

        assert!(steps.get("Internal").unwrap().external_data().is_none());
    }

    #[test]
    fn test_step_as_external_some() {
        let participant = Participant::Entity(crate::bpmn::messages::Entity::new("User1"));
        let ext = ExternalStepData::new::<DummyValue>("Ext", participant.clone());

        let builder = StepsBuilder::default();
        builder.add_start::<()>(Participant::Everyone).unwrap();
        builder.add_external::<DummyValue>(ext.clone()).unwrap();
        builder.add_end(Type::new::<()>());

        let steps = builder.build().unwrap();
        let step = steps.get("Ext").unwrap();

        assert!(step.external_data().is_some());
        assert_eq!(step.external_data().unwrap(), &ext);
    }

    #[test]
    fn test_steps_serialize() {
        let builder = StepsBuilder::default();
        builder.add_start::<()>(Participant::Everyone).unwrap();
        builder.add::<()>("A").unwrap();
        builder
            .add_external::<DummyValue>(ExternalStepData::new::<DummyValue>(
                "B",
                Participant::Entity(crate::bpmn::messages::Entity::new("User1")),
            ))
            .unwrap();
        builder.add_end(Type::new::<()>());

        let steps = builder.build().unwrap();

        let json = serde_json::to_string(&steps).unwrap();
        assert_eq!(json, r#"["Start","A","B","End"]"#);
    }

    #[test]
    fn test_step_serialize() {
        let builder = StepsBuilder::default();
        builder.add_start::<()>(Participant::Everyone).unwrap();
        builder.add_end(Type::new::<()>());

        let steps = builder.build().unwrap();

        let start_json = serde_json::to_string(&Step::from(steps.start())).unwrap();
        assert_eq!(start_json, r#""Start""#);
    }

    #[test]
    fn test_step_hash() {
        use std::collections::HashSet;

        let builder = StepsBuilder::default();
        builder.add_start::<()>(Participant::Everyone).unwrap();
        builder.add::<()>("A").unwrap();
        builder.add_end(Type::new::<()>());
        let steps = builder.build().unwrap();

        let mut set = HashSet::new();
        set.insert(steps.get("A").unwrap());
        // Same step instance (Arc ptr) must not add a new item
        set.insert(steps.get("A").unwrap());

        assert_eq!(set.len(), 1);
    }

    #[test]
    fn test_try_from_builder_direct() {
        // This exercises TryFrom<StepsBuilder> instead of build()
        let builder = StepsBuilder::default();
        builder.add_start::<()>(Participant::Everyone).unwrap();
        builder.add::<()>("X").unwrap();
        builder.add_end(Type::new::<()>());

        let steps_via_try = Steps::try_from(builder.clone()).unwrap();
        let steps_via_build = builder.build().unwrap();

        assert_eq!(
            steps_via_try.steps().collect::<Vec<_>>(),
            steps_via_build.steps().collect::<Vec<_>>()
        );
    }
}
