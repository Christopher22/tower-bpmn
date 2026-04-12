use std::collections::HashSet;
use std::hash::Hash;
use std::{borrow::Cow, sync::Arc};

#[derive(
    Debug, Clone, PartialEq, Eq, Hash, serde::Deserialize, serde::Serialize, schemars::JsonSchema,
)]
/// A participant in a BPMN process, which can be used for assigning tasks and sending messages to specific entities or roles.
pub enum Participant {
    /// No specific participant which could be used to deactivate processes.
    Nobody,
    /// A specific entity, such as a department.
    Entity(Cow<'static, str>),
    /// A role, like a chef.
    Role(Cow<'static, str>),
    /// A generic participant representing everyone.
    Everyone,
}

impl Participant {
    /// Creates a participant representing a specific entity.
    pub const fn entity(name: &'static str) -> Self {
        Participant::Entity(Cow::Borrowed(name))
    }

    /// Creates a participant representing a specific role.
    pub const fn role(name: &'static str) -> Self {
        Participant::Role(Cow::Borrowed(name))
    }
}

/// The context a message was created.
#[derive(Debug, Clone)]
pub struct Context(Arc<HashSet<Participant>>);

impl Context {
    /// Creates a context matching the given participant, if it is not 'Nobody'.
    pub fn new_matching(participant: Participant) -> Option<Self> {
        match participant {
            Participant::Nobody => None,
            Participant::Everyone => Some(HashSet::new()),
            participant => Some(HashSet::from([participant])),
        }
        .map(|value| Context(Arc::new(value)))
    }

    /// Checks if the message context matches the expected participant.
    pub fn is_suitable_for(&self, participant: &Participant) -> bool {
        match participant {
            Participant::Nobody => false,
            Participant::Everyone => true,
            _ => self.0.iter().any(|p| p == participant),
        }
    }
}

impl FromIterator<Participant> for Context {
    fn from_iter<T: IntoIterator<Item = Participant>>(iter: T) -> Self {
        Context(Arc::new(
            iter.into_iter()
                .filter(|value| value != &Participant::Nobody && value != &Participant::Everyone)
                .collect(),
        ))
    }
}

impl Default for Context {
    fn default() -> Self {
        Context(Arc::new(HashSet::new()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper to create a context from a slice of participants.
    fn make_context(p: &[Participant]) -> Context {
        p.iter().cloned().collect()
    }

    #[test]
    fn test_participant_constructors() {
        // Test if the const constructors create the expected variants with correct names
        let entity = Participant::entity("DevDept");
        let role = Participant::role("Admin");

        assert_eq!(entity, Participant::Entity(Cow::Borrowed("DevDept")));
        assert_eq!(role, Participant::Role(Cow::Borrowed("Admin")));
    }

    #[test]
    fn test_context_default() {
        let ctx = Context::default();
        let dev_role = Participant::role("Developer");

        // A default context is empty.
        // It should not match a specific role, but still match 'Everyone'.
        assert!(
            !ctx.is_suitable_for(&dev_role),
            "Default context should not match a specific role"
        );
        assert!(
            ctx.is_suitable_for(&Participant::Everyone),
            "Default context should still match 'Everyone'"
        );
        assert!(
            !ctx.is_suitable_for(&Participant::Nobody),
            "Default context should never match 'Nobody'"
        );
    }

    #[test]
    fn test_context_filtering_logic() {
        // The FromIterator implementation should filter out 'Nobody' and 'Everyone'
        // from the internal HashSet because they are handled by logic, not by presence.
        let participants = vec![
            Participant::role("Chef"),
            Participant::Everyone,
            Participant::Nobody,
            Participant::entity("Kitchen"),
        ];

        let ctx: Context = participants.into_iter().collect();

        // Internal set size check: Should only contain "Chef" and "Kitchen"
        assert_eq!(
            ctx.0.len(),
            2,
            "Context should have filtered out Everyone and Nobody from the internal set"
        );

        // Verify suitability
        assert!(ctx.is_suitable_for(&Participant::role("Chef")));
        assert!(ctx.is_suitable_for(&Participant::entity("Kitchen")));
    }

    #[test]
    fn test_suitability_edge_cases() {
        let chef = Participant::role("Chef");
        let waiter = Participant::role("Waiter");
        let ctx = make_context(std::slice::from_ref(&chef));

        // 1. Participant::Everyone: Always returns true regardless of context content
        assert!(
            ctx.is_suitable_for(&Participant::Everyone),
            "Everyone should always be suitable"
        );

        // 2. Participant::Nobody: Always returns false regardless of context content
        assert!(
            !ctx.is_suitable_for(&Participant::Nobody),
            "Nobody should never be suitable"
        );

        // 3. Match found
        assert!(
            ctx.is_suitable_for(&chef),
            "Context containing Chef should be suitable for Chef"
        );

        // 4. Match not found
        assert!(
            !ctx.is_suitable_for(&waiter),
            "Context containing only Chef should not be suitable for Waiter"
        );
    }

    #[test]
    fn test_cow_string_matching() {
        // Test if owned strings and borrowed strings match correctly within the context
        let role_name = "Manager".to_string();
        let ctx = make_context(&[Participant::Role(Cow::Owned(role_name))]);

        // Check against a borrowed version
        assert!(
            ctx.is_suitable_for(&Participant::role("Manager")),
            "Owned strings in context should match borrowed strings in queries"
        );
    }

    #[test]
    fn test_empty_iterator() {
        // Creating a context from an empty iterator should result in an empty set
        let ctx: Context = std::iter::empty::<Participant>().collect();
        assert_eq!(ctx.0.len(), 0);
        assert!(!ctx.is_suitable_for(&Participant::role("Any")));
    }

    #[test]
    fn test_duplicate_participants() {
        // HashSets handle duplicates automatically
        let ctx = make_context(&[Participant::role("Admin"), Participant::role("Admin")]);

        assert_eq!(
            ctx.0.len(),
            1,
            "Duplicate participants should be collapsed in the context"
        );
    }
}
