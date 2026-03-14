use std::sync::Arc;

use parking_lot::RwLock;

/// A collection of steps in a process.
#[derive(Debug, Clone)]
pub struct Steps(Vec<Arc<String>>);

impl Steps {
    /// Try to create a new collection of steps from a list of step names.
    pub fn new(steps: impl Iterator<Item = impl Into<String>>) -> Result<Self, InvalidStep> {
        let builder = StepsBuilder::default();
        builder.add_start();
        for step in steps {
            builder.add(step)?;
        }
        builder.add_end();
        Ok(builder
            .build()
            .expect("StepsBuilder should not fail when all steps are valid"))
    }

    /// Get the start step of the process.
    pub fn start(&self) -> Step {
        Step(
            self.0
                .first()
                .expect("Steps must contain a start step")
                .clone(),
        )
    }

    /// Get the end step of the process.
    pub fn end(&self) -> Step {
        Step(
            self.0
                .last()
                .expect("Steps must contain an end step")
                .clone(),
        )
    }

    /// Try to get a step by name. Returns `None` if the step does not exist.
    pub fn get(&self, step: &str) -> Option<Step> {
        self.0
            .iter()
            .find(|s| s.as_str() == step)
            .map(|s| Step(s.clone()))
    }

    /// Get an iterator over all the steps in the process.
    pub fn steps(&self) -> impl Iterator<Item = &str> {
        self.0.iter().map(|s| s.as_str())
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
pub struct StepsBuilder(Arc<RwLock<Vec<Arc<String>>>>);

impl Default for StepsBuilder {
    fn default() -> Self {
        StepsBuilder(Arc::new(RwLock::new(vec![])))
    }
}

impl StepsBuilder {
    /// Add a step to the process. The name cannot be empty, and cannot be "Start" or "End". Returns an error if the name is invalid or already exists.
    pub fn add(&self, step: impl Into<String>) -> Result<Step, InvalidStep> {
        let step = step.into();
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

        let reference = Arc::new(step);
        lock.push(reference.clone());
        Ok(Step(reference))
    }

    /// Build the steps. This will return an error if not start or end were added.
    pub(super) fn build(self) -> Result<Steps, UnfinishedBuilder> {
        Steps::try_from(self)
    }

    /// Add a start step. This is only called internally.
    pub(super) fn add_start(&self) -> Step {
        let mut lock = self.0.write();
        Step(
            lock.iter()
                .find(|s| s.as_str() == Step::START)
                .cloned()
                .unwrap_or_else(|| {
                    let reference = Arc::new(Step::START.to_string());
                    lock.push(reference.clone());
                    reference
                }),
        )
    }

    /// Add an end step. This is only called internally.
    pub(super) fn add_end(&self) -> Step {
        let mut lock = self.0.write();
        Step(
            lock.iter()
                .find(|s| s.as_str() == Step::END)
                .cloned()
                .unwrap_or_else(|| {
                    let reference = Arc::new(Step::END.to_string());
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

/// A step in a process. This equals only(!) the same step within the same process, even if the name is the same.
#[derive(Debug, Clone)]
pub struct Step(Arc<String>);

impl Step {
    /// Reserved start name that cannot be used for user-defined steps.
    pub const START: &'static str = "Start";

    /// Reserved end name that cannot be used for user-defined steps.
    pub const END: &'static str = "End";

    /// Check if the step is the start step.
    pub fn is_start(&self) -> bool {
        self.0.as_str() == Self::START
    }

    /// Check if the step is the end step.
    pub fn is_end(&self) -> bool {
        self.0.as_str() == Self::END
    }

    /// Get the name of the step.
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl AsRef<str> for Step {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for Step {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl PartialEq for Step {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
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
        serializer.serialize_str(&self.0)
    }
}

impl schemars::JsonSchema for Step {
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

/// Errors emitted when building steps.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum InvalidStep {
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

    #[test]
    fn test_step_equality() {
        const SAME_NAME: &str = "Task A";

        let (s1, s2) = {
            let step_process_1 = StepsBuilder::default();
            let step_process_2 = StepsBuilder::default();

            assert_ne!(step_process_1.add_start(), step_process_2.add_start());
            assert_ne!(step_process_1.add_end(), step_process_2.add_end());
            assert_ne!(
                step_process_1.add(SAME_NAME).unwrap(),
                step_process_2.add(SAME_NAME).unwrap()
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
        builder.add_start();
        builder.add_end();
        builder.add("Task A").unwrap();

        let other_builder = builder.clone();
        builder.add("Task B").unwrap();
        other_builder.add("Task C").unwrap();

        let steps = builder.build().unwrap();
        other_builder.add("Task D").unwrap();

        let steps = steps.steps().collect::<Vec<_>>(); // Should not panic or cause issues with the other builder.
        assert_eq!(steps, vec!["Start", "Task A", "Task B", "Task C", "End"]); // Task c should be included because it was added before building, but Task D should not be included because it was added after building.
    }

    #[test]
    fn test_builder_happy_path() {
        let builder = StepsBuilder::default();
        builder.add_start();
        builder.add_end();
        let step_a = builder
            .add("Process Data")
            .expect("Should allow adding a valid step");

        let steps = builder
            .build()
            .expect("Build should succeed with Start and End");

        // Verify start and end accessors
        assert_eq!(steps.start(), *Step::START);
        assert_eq!(steps.end(), *Step::END);

        // Verify retrieval
        assert_eq!(steps.get("Process Data").unwrap(), step_a);
    }

    #[test]
    fn test_builder_validation_errors() {
        let builder = StepsBuilder::default();

        // 1. Test reserved names
        assert_eq!(builder.add("Start").unwrap_err(), InvalidStep::Reserved);
        assert_eq!(builder.add("End").unwrap_err(), InvalidStep::Reserved);

        // 2. Test empty name
        assert_eq!(builder.add("").unwrap_err(), InvalidStep::Empty);
        assert_eq!(builder.add("   ").unwrap_err(), InvalidStep::Empty);

        // 3. Test duplicates
        builder.add("Unique").unwrap();
        assert_eq!(builder.add("Unique").unwrap_err(), InvalidStep::Existing);
    }

    #[test]
    fn test_build_missing_requirements() {
        // Missing both
        let builder = StepsBuilder::default();
        assert_eq!(builder.build().unwrap_err(), UnfinishedBuilder);

        // Missing End
        let builder = StepsBuilder::default();
        builder.add_start();
        assert_eq!(builder.build().unwrap_err(), UnfinishedBuilder);

        // Missing Start
        let builder = StepsBuilder::default();
        builder.add_end();
        assert_eq!(builder.build().unwrap_err(), UnfinishedBuilder);
    }

    #[test]
    fn test_step_order_logic() {
        let builder = StepsBuilder::default();
        // Add in "wrong" order
        builder.add_end();
        builder.add("Middle Task").unwrap();
        builder.add("Middle Task 2").unwrap();
        builder.add_start();

        let steps = builder.build().expect("Build should succeed");
        let collected: Vec<&str> = steps.steps().collect();

        // Ensure Start is first and End is last regardless of addition order
        assert_eq!(collected.first().unwrap(), &Step::START);
        assert_eq!(collected.last().unwrap(), &Step::END);
    }

    #[test]
    fn test_step_iterator() {
        let builder = StepsBuilder::default();
        builder.add_start();
        builder.add("A").unwrap();
        builder.add("B").unwrap();
        builder.add_end();

        let steps = builder.build().unwrap();
        let mut iter = steps.steps();

        assert_eq!(iter.next(), Some(Step::START));
        assert_eq!(iter.next(), Some("A"));
        assert_eq!(iter.next(), Some("B"));
        assert_eq!(iter.next(), Some(Step::END));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn test_internal_add_is_idempotent() {
        let builder = StepsBuilder::default();
        let s1 = builder.add_start();
        let s2 = builder.add_start();
        builder.add_end();

        // add_start should return the same Arc if called twice
        assert_eq!(s1, s2);

        let steps = builder.build().unwrap();
        // Should only contain one "Start"
        let start_count = steps.steps().filter(|&s| s == Step::START).count();
        assert_eq!(start_count, 1);
    }
}
