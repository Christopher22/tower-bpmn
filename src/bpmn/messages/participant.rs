use std::collections::HashSet;
use std::hash::Hash;
use std::{borrow::Cow, sync::Arc};

/// A sppecific entity of a process, such as a specific person.
/// In geenral, that corresponds to the individual responsible.
#[derive(
    Debug, Clone, PartialEq, Eq, Hash, serde::Deserialize, serde::Serialize, schemars::JsonSchema,
)]
#[serde(transparent)]
pub struct Entity(Cow<'static, str>);

impl Entity {
    /// A generic entity representing the system itself, which is used for system-generated messages and tasks.
    pub const SYSTEM: Self = Self::new("SYSTEM");

    /// Creates a new entity with the given name.
    pub const fn new(name: &'static str) -> Self {
        Entity(Cow::Borrowed(name))
    }
}

impl From<&'static str> for Entity {
    fn from(value: &'static str) -> Self {
        Entity(Cow::Borrowed(value))
    }
}

impl From<String> for Entity {
    fn from(value: String) -> Self {
        Entity(Cow::Owned(value))
    }
}

impl std::fmt::Display for Entity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// A specific role which may be fulfilled by different entities, such as 'Chef' or 'Admin'.
#[derive(
    Debug, Clone, PartialEq, Eq, Hash, serde::Deserialize, serde::Serialize, schemars::JsonSchema,
)]
#[serde(transparent)]
pub struct Role(Cow<'static, str>);

impl Role {
    /// Creates a new role with the given name.
    pub const fn new(name: &'static str) -> Self {
        Role(Cow::Borrowed(name))
    }
}

impl std::fmt::Display for Role {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// A participant in a BPMN process, which can be used for assigning tasks and sending messages to specific entities or roles.
#[derive(
    Debug, Clone, PartialEq, Eq, Hash, serde::Deserialize, serde::Serialize, schemars::JsonSchema,
)]
pub enum Participant {
    /// No specific participant which could be used to deactivate processes.
    Nobody,
    /// A specific entity, such as a department.
    Entity(Entity),
    /// A role, like a chef.
    Role(Role),
    /// A generic participant representing everyone.
    Everyone,
}

impl From<Entity> for Participant {
    fn from(value: Entity) -> Self {
        Participant::Entity(value)
    }
}

impl From<Role> for Participant {
    fn from(value: Role) -> Self {
        Participant::Role(value)
    }
}

/// The context a message was created.
#[derive(Debug, Clone)]
pub struct Context(Arc<HashSet<Participant>>);

impl Context {
    /// Creates a context for the system, which is used for system-generated messages and tasks.
    pub fn system() -> Self {
        Context(Arc::new(HashSet::from([Participant::Entity(
            Entity::SYSTEM,
        )])))
    }

    /// Creates a context for a specific entity.
    pub fn for_entity(entity: Entity) -> Self {
        Context(Arc::new(HashSet::from([Participant::Entity(entity)])))
    }

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

    /// Try to extract the responsible entity from the context, if there is exactly one entity.
    pub fn responsible_entity(&self) -> Option<&Entity> {
        let mut entities = self.0.iter().filter_map(|p| match p {
            Participant::Entity(e) => Some(e),
            _ => None,
        });
        let first = entities.next()?;
        if entities.next().is_none() {
            Some(first)
        } else {
            None
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
        let entity = Participant::Entity(Entity::new("DevDept"));
        let role = Participant::Role(Role::new("Admin"));

        assert_eq!(entity, Participant::Entity(Entity::new("DevDept")));
        assert_eq!(role, Participant::Role(Role::new("Admin")));
    }

    #[test]
    fn test_context_default() {
        let ctx = Context::system();
        let dev_role = Participant::Role(Role::new("Developer"));

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
            Participant::Role(Role::new("Chef")),
            Participant::Everyone,
            Participant::Nobody,
            Participant::Entity(Entity::new("Kitchen")),
        ];

        let ctx: Context = participants.into_iter().collect();

        // Internal set size check: Should only contain "Chef" and "Kitchen"
        assert_eq!(
            ctx.0.len(),
            2,
            "Context should have filtered out Everyone and Nobody from the internal set"
        );

        // Verify suitability
        assert!(ctx.is_suitable_for(&Participant::Role(Role::new("Chef"))));
        assert!(ctx.is_suitable_for(&Participant::Entity(Entity::new("Kitchen"))));
    }

    #[test]
    fn test_suitability_edge_cases() {
        let chef = Participant::Role(Role::new("Chef"));
        let waiter = Participant::Role(Role::new("Waiter"));
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
        let ctx = make_context(&[Participant::Role(Role(Cow::Owned(role_name)))]);

        // Check against a borrowed version
        assert!(
            ctx.is_suitable_for(&Participant::Role(Role::new("Manager"))),
            "Owned strings in context should match borrowed strings in queries"
        );
    }

    #[test]
    fn test_empty_iterator() {
        // Creating a context from an empty iterator should result in an empty set
        let ctx: Context = std::iter::empty::<Participant>().collect();
        assert_eq!(ctx.0.len(), 0);
        assert!(!ctx.is_suitable_for(&Participant::Role(Role::new("Any"))));
    }

    #[test]
    fn test_duplicate_participants() {
        // HashSets handle duplicates automatically
        let ctx = make_context(&[
            Participant::Role(Role::new("Admin")),
            Participant::Role(Role::new("Admin")),
        ]);

        assert_eq!(
            ctx.0.len(),
            1,
            "Duplicate participants should be collapsed in the context"
        );
    }
}
