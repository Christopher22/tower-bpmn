use std::{path::Path, sync::Arc};

use dashmap::DashMap;
use parking_lot::Mutex;
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::Value as JsonValue;

use crate::bpmn::{
    InstanceId, ProcessName, RegisteredProcess, Step, Steps, Token, TokenId, Value,
    messages::Entity,
    storage::{ResumableProcess, Storage, StorageBackend, StorageError},
};

/// Errors emitted while opening or preparing SQLite storage.
#[derive(Debug)]
pub enum SqliteError {
    /// Any error returned by the underlying rusqlite driver.
    Sql(rusqlite::Error),
}

impl std::fmt::Display for SqliteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SqliteError::Sql(error) => write!(f, "SQLite error: {error}"),
        }
    }
}

impl std::error::Error for SqliteError {}

impl From<rusqlite::Error> for SqliteError {
    fn from(value: rusqlite::Error) -> Self {
        SqliteError::Sql(value)
    }
}

/// A SQLite based storage backend.
///
/// This backend persists:
/// - registered process definitions and versions,
/// - process steps (including magic `Start` and `End`),
/// - optional JSON schema for each step output,
/// - instance history rows with serialized values.
///
/// The schema is normalized for low write amplification and straightforward
/// migration to a PostgreSQL backend.
#[derive(Debug, Clone)]
pub struct Sqlite {
    connection: Arc<Mutex<Connection>>,
}

impl Sqlite {
    /// Create an in-memory backend.
    pub fn in_memory() -> Result<Self, SqliteError> {
        let connection = Connection::open_in_memory()?;
        Self::from_connection(connection)
    }

    /// Open or create a SQLite database at the given path.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, SqliteError> {
        let connection = Connection::open(path)?;
        Self::from_connection(connection)
    }

    fn from_connection(connection: Connection) -> Result<Self, SqliteError> {
        connection.execute_batch("PRAGMA foreign_keys = ON;")?;
        Self::init_schema(&connection)?;
        Ok(Self {
            connection: Arc::new(Mutex::new(connection)),
        })
    }

    fn init_schema(connection: &Connection) -> rusqlite::Result<()> {
        connection.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS processes (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                version INTEGER NOT NULL,
                UNIQUE(name, version)
            );

            CREATE TABLE IF NOT EXISTS steps (
                id INTEGER PRIMARY KEY,
                process_id INTEGER NOT NULL REFERENCES processes(id) ON DELETE CASCADE,
                step_name TEXT NOT NULL,
                rust_type TEXT,
                value_schema_json TEXT,
                UNIQUE(process_id, step_name)
            );

            CREATE TABLE IF NOT EXISTS instances (
                id TEXT PRIMARY KEY,
                process_id INTEGER NOT NULL REFERENCES processes(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS entities (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL UNIQUE
            );

            CREATE TABLE IF NOT EXISTS finished_steps (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                instance_id TEXT NOT NULL REFERENCES instances(id) ON DELETE CASCADE,
                step_id INTEGER NOT NULL REFERENCES steps(id) ON DELETE RESTRICT,
                token_slot INTEGER NOT NULL,
                value_json TEXT NOT NULL,
                entity_id INTEGER REFERENCES entities(id) ON DELETE RESTRICT,
                CHECK(token_slot >= 0)
            );

            CREATE INDEX IF NOT EXISTS idx_finished_steps_latest_by_token
                ON finished_steps(instance_id, token_slot, id DESC);

            CREATE INDEX IF NOT EXISTS idx_steps_type
                ON steps(process_id, rust_type);

            CREATE INDEX IF NOT EXISTS idx_history_lookup
                ON finished_steps(instance_id, step_id, token_slot, id DESC);

            CREATE INDEX IF NOT EXISTS idx_history_step
                ON finished_steps(instance_id, step_id, id DESC);
            ",
        )
    }

    fn with_connection<T>(&self, op: impl FnOnce(&mut Connection) -> rusqlite::Result<T>) -> T {
        let mut lock = self.connection.lock();
        // Keep all SQLite access serialized through one connection to avoid
        // accidental concurrent writes in test/runtime setups.
        op(&mut lock).expect("SQLite operation failed")
    }
}

impl Default for Sqlite {
    fn default() -> Self {
        Self::in_memory().expect("failed to create in-memory sqlite backend")
    }
}

impl StorageBackend for Sqlite {
    type Storage = SqliteStorage;

    fn query(
        &self,
        process: &RegisteredProcess<Self>,
        step: Step,
        instance_id: InstanceId,
    ) -> Result<Vec<JsonValue>, StorageError> {
        let found_process: Option<(i64, String, u32)> = self.with_connection(|connection| {
            connection
                .query_row(
                    "
                    SELECT p.id, p.name, p.version
                    FROM instances i
                    JOIN processes p ON p.id = i.process_id
                    WHERE i.id = ?1
                    ",
                    params![instance_id.to_string()],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .optional()
        });

        let Some((process_db_id, name, version)) = found_process else {
            return Err(StorageError::NotFound);
        };

        if name != process.meta_data.name.as_ref() || version != process.meta_data.version {
            return Err(StorageError::ProcessMismatch);
        }

        let step_db_id: Option<i64> = self.with_connection(|connection| {
            connection
                .query_row(
                    "SELECT id FROM steps WHERE process_id = ?1 AND step_name = ?2",
                    params![process_db_id, step.as_str()],
                    |row| row.get(0),
                )
                .optional()
        });

        let Some(step_db_id) = step_db_id else {
            return Ok(Vec::new());
        };

        let raw_values: Vec<String> = self.with_connection(|connection| {
            let mut statement = connection.prepare(
                "
                SELECT value_json
                FROM finished_steps
                WHERE instance_id = ?1 AND step_id = ?2
                ORDER BY id ASC
                ",
            )?;
            let rows = statement
                .query_map(params![instance_id.to_string(), step_db_id], |row| {
                    row.get::<_, String>(0)
                })?;

            let mut values = Vec::new();
            for row in rows {
                values.push(row?);
            }

            Ok(values)
        });

        Ok(raw_values
            .into_iter()
            .filter_map(|value| serde_json::from_str::<JsonValue>(&value).ok())
            .collect())
    }

    fn new_instance(
        &self,
        process: &RegisteredProcess<Self>,
        process_id: InstanceId,
    ) -> Self::Storage {
        let process_name = process.meta_data.name.to_string();
        let process_version = process.meta_data.version;

        let step_ids = self.with_connection(|connection| {
			// Register process metadata + steps + concrete instance atomically so
			// the schema cannot end up in a partially initialized state.
			let tx = connection.transaction()?;

			// Upsert the process definition exactly once. Subsequent instances for
			// the same (name, version) reuse the same process row.
			tx.execute(
				"INSERT OR IGNORE INTO processes (name, version) VALUES (?1, ?2)",
				params![process_name, process_version],
			)?;

			let process_db_id: i64 = tx.query_row(
				"SELECT id FROM processes WHERE name = ?1 AND version = ?2",
				params![process.meta_data.name.as_ref(), process.meta_data.version],
				|row| row.get(0),
			)?;

			for step in process.steps.steps() {
				// Persist known process steps once and enrich `value_schema_json`
				// lazily when the first value for this step is written.
				tx.execute(
					"INSERT OR IGNORE INTO steps (process_id, step_name, rust_type, value_schema_json) VALUES (?1, ?2, NULL, NULL)",
					params![process_db_id, step],
				)?;
			}

			tx.execute(
				// One row per runtime instance with a strict FK to its process.
				"INSERT OR IGNORE INTO instances (id, process_id) VALUES (?1, ?2)",
				params![process_id.to_string(), process_db_id],
			)?;

			let step_ids = {
				let mut statement = tx.prepare(
					"SELECT step_name, id FROM steps WHERE process_id = ?1",
				)?;
				let rows = statement.query_map(params![process_db_id], |row| {
					Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
				})?;
				let step_ids = DashMap::new();
				for row in rows {
					let (name, id) = row?;
					step_ids.insert(name, id);
				}
				step_ids
			};

			tx.commit()?;

			Ok(step_ids)
		});

        SqliteStorage {
            connection: self.connection.clone(),
            steps: process.steps.clone(),
            instance_id: process_id,
            token_slots: Arc::new(DashMap::new()),
            step_ids: Arc::new(step_ids),
            next_token_slot: Arc::new(Mutex::new(0)),
        }
    }

    fn resume_instance(
        &self,
        process: &RegisteredProcess<Self>,
        process_id: InstanceId,
    ) -> Result<ResumableProcess<Self>, StorageError> {
        // Resolve the owning process first. This provides both existence and
        // process-mismatch checks in one lookup.
        let found_process: Option<(i64, String, u32)> = self.with_connection(|connection| {
            connection
                .query_row(
                    "
					SELECT p.id, p.name, p.version
					FROM instances i
					JOIN processes p ON p.id = i.process_id
					WHERE i.id = ?1
					",
                    params![process_id.to_string()],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .optional()
        });

        let Some((process_db_id, name, version)) = found_process else {
            return Err(StorageError::NotFound);
        };

        if name != process.meta_data.name.as_ref() || version != process.meta_data.version {
            return Err(StorageError::ProcessMismatch);
        }

        let (step_ids, next_token_slot) = self.with_connection(|connection| {
            // Load step ids for fast runtime writes and pre-seed token slot counter
            // so resumed tokens and future forks do not collide.
            let mut statement =
                connection.prepare("SELECT step_name, id FROM steps WHERE process_id = ?1")?;
            let rows = statement.query_map(params![process_db_id], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
            })?;
            let step_ids = DashMap::new();
            for row in rows {
                let (name, id) = row?;
                step_ids.insert(name, id);
            }

            let max_slot: Option<i64> = connection.query_row(
                "SELECT MAX(token_slot) FROM finished_steps WHERE instance_id = ?1",
                params![process_id.to_string()],
                |row| row.get(0),
            )?;

            Ok((step_ids, max_slot.map_or(0, |slot| slot + 1)))
        });

        let storage = SqliteStorage {
            connection: self.connection.clone(),
            steps: process.steps.clone(),
            instance_id: process_id,
            token_slots: Arc::new(DashMap::new()),
            step_ids: Arc::new(step_ids),
            next_token_slot: Arc::new(Mutex::new(next_token_slot)),
        };

        let rows: Vec<(i64, String)> = storage.with_connection(|connection| {
            let mut statement = connection.prepare(
                "
				WITH latest_per_token AS (
					SELECT token_slot, MAX(id) AS max_id
					FROM finished_steps
					WHERE instance_id = ?1
					GROUP BY token_slot
				)
				-- Reconstruct each active token branch from its last persisted step.
				SELECT f.token_slot, s.step_name
				FROM finished_steps f
				JOIN latest_per_token l ON l.max_id = f.id
				JOIN steps s ON s.id = f.step_id
				",
            )?;
            let rows = statement.query_map(params![process_id.to_string()], |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
            })?;

            let mut resolved = Vec::new();
            for row in rows {
                resolved.push(row?);
            }
            Ok(resolved)
        });

        if rows.is_empty() {
            return Err(StorageError::NotFound);
        }

        let mut current_state = Vec::with_capacity(rows.len());
        for (token_slot, step_name) in rows {
            let Some(step) = process.steps.get(&step_name) else {
                continue;
            };
            let Some(place_id) = process.place_after_step(&step) else {
                continue;
            };

            let token = Token::new(Entity::SYSTEM, storage.clone());
            storage.assign_resumed_slot(token.id(), token_slot);
            current_state.push((place_id, token));
        }

        if current_state.is_empty() {
            return Err(StorageError::NotFound);
        }

        Ok(ResumableProcess {
            id: process_id,
            current_state,
            storage,
        })
    }

    fn unfinished_instances(&self) -> Vec<(ProcessName, InstanceId)> {
        self.with_connection(|connection| {
            let mut statement = connection.prepare(
                "
				WITH latest_per_token AS (
					SELECT instance_id, token_slot, MAX(id) AS max_id
					FROM finished_steps
					GROUP BY instance_id, token_slot
				),
				current_tokens AS (
					-- Determine the currently active step per token branch.
					SELECT l.instance_id, s.step_name
					FROM latest_per_token l
					JOIN finished_steps f ON f.id = l.max_id
					JOIN steps s ON s.id = f.step_id
				)
				-- An instance is unfinished when at least one active branch is not at End.
				SELECT p.name, p.version, i.id
				FROM instances i
				JOIN processes p ON p.id = i.process_id
				LEFT JOIN current_tokens c ON c.instance_id = i.id
				GROUP BY p.name, p.version, i.id
				HAVING COALESCE(SUM(CASE WHEN c.step_name = ?1 THEN 0 ELSE 1 END), 1) > 0
				",
            )?;

            let rows = statement.query_map(params![Step::END], |row| {
                let process_name: String = row.get(0)?;
                let version: u32 = row.get(1)?;
                let instance_id: String = row.get(2)?;
                Ok((process_name, version, instance_id))
            })?;

            let mut unfinished = Vec::new();
            for row in rows {
                let (name, version, instance_id) = row?;
                let process_name = format!("{name}-{version}").parse().map_err(|_| {
                    rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Text,
                        "invalid process name in database".into(),
                    )
                })?;
                let instance_id = instance_id.parse().map_err(|_| {
                    rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Text,
                        "invalid instance id in database".into(),
                    )
                })?;
                unfinished.push((process_name, instance_id));
            }

            Ok(unfinished)
        })
    }
}

/// Storage for one process instance backed by SQLite tables.
#[derive(Clone)]
pub struct SqliteStorage {
    connection: Arc<Mutex<Connection>>,
    steps: Steps,
    instance_id: InstanceId,
    /// Mapping from runtime `TokenId` to the compact integer slot stored in
    /// `finished_steps.token_slot`.  All clones of the same `SqliteStorage`
    /// share this map so slot assignments remain consistent within one process
    /// instance while staying completely isolated from other instances (each
    /// [`StorageBackend::new_instance`] / [`StorageBackend::resume_instance`]
    /// call creates a fresh `Arc`).
    token_slots: Arc<DashMap<TokenId, i64>>,
    step_ids: Arc<DashMap<String, i64>>,
    next_token_slot: Arc<Mutex<i64>>,
}

impl std::fmt::Debug for SqliteStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqliteStorage")
            .field("instance_id", &self.instance_id)
            .finish_non_exhaustive()
    }
}

impl PartialEq for SqliteStorage {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.connection, &other.connection)
            && Arc::ptr_eq(&self.token_slots, &other.token_slots)
            && self.instance_id == other.instance_id
    }
}

impl Eq for SqliteStorage {}

impl SqliteStorage {
    fn value_type_key<V: Value>() -> String {
        <V as schemars::JsonSchema>::schema_name().into_owned()
    }

    #[cfg(test)]
    /// Create an isolated SQLite-backed storage with a synthetic process/instance.
    pub fn for_test() -> Self {
        let backend = Sqlite::in_memory().expect("failed to create test backend");
        let steps = Steps::new(["a", "b"].into_iter()).expect("valid test steps");
        let process_name = "test".to_string();
        let process_version = 1_u32;
        let instance_id: InstanceId = uuid::Uuid::new_v4()
            .to_string()
            .parse()
            .expect("uuid must parse as instance id");

        let step_ids = backend.with_connection(|connection| {
            connection.execute(
                "INSERT INTO processes (name, version) VALUES (?1, ?2)",
                params![process_name, process_version],
            )?;
            let process_db_id: i64 = connection.query_row(
                "SELECT id FROM processes WHERE name = ?1 AND version = ?2",
                params!["test", 1_u32],
                |row| row.get(0),
            )?;
            for step in steps.steps() {
                connection.execute(
					"INSERT INTO steps (process_id, step_name, value_schema_json) VALUES (?1, ?2, NULL)",
					params![process_db_id, step],
				)?;
            }
            connection.execute(
                "INSERT INTO instances (id, process_id) VALUES (?1, ?2)",
                params![instance_id.to_string(), process_db_id],
            )?;

            let mut statement =
                connection.prepare("SELECT step_name, id FROM steps WHERE process_id = ?1")?;
            let rows = statement.query_map(params![process_db_id], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
            })?;
            let step_ids = DashMap::new();
            for row in rows {
                let (name, id) = row?;
                step_ids.insert(name, id);
            }

            Ok(step_ids)
        });

        Self {
            connection: backend.connection,
            steps,
            instance_id,
            token_slots: Arc::new(DashMap::new()),
            step_ids: Arc::new(step_ids),
            next_token_slot: Arc::new(Mutex::new(0)),
        }
    }

    fn with_connection<T>(&self, op: impl FnOnce(&mut Connection) -> rusqlite::Result<T>) -> T {
        let mut lock = self.connection.lock();
        op(&mut lock).expect("SQLite operation failed")
    }

    fn token_slot_for(&self, token_id: TokenId) -> i64 {
        // `entry().or_insert_with()` holds the DashMap shard lock across the
        // check and the insert, closing the TOCTOU window that would otherwise
        // let two concurrent tasks on the same instance allocate two different
        // slots for the same token and make history rows unsearchable.
        *self.token_slots.entry(token_id).or_insert_with(|| {
            // Token slots are append-only integer identifiers used for compact
            // branch tracking in SQL queries.
            let mut next = self.next_token_slot.lock();
            let slot = *next;
            *next += 1;
            slot
        })
    }

    fn token_slots_for(&self, token_ids: &[TokenId]) -> Vec<i64> {
        // Keep order stable with input token ids to produce deterministic SQL
        // parameter vectors in tests and debug output.
        token_ids
            .iter()
            .filter_map(|token_id| self.token_slots.get(token_id).map(|entry| *entry))
            .collect()
    }

    /// Register the DB slot from a persisted row back onto a freshly allocated
    /// token while reconstructing an instance during [`StorageBackend::resume_instance`].
    fn assign_resumed_slot(&self, token_id: TokenId, slot: i64) {
        self.token_slots.insert(token_id, slot);
        // Advance the counter so that newly forked tokens after resume never
        // collide with the slot numbers that already exist in the database.
        let mut next = self.next_token_slot.lock();
        if *next <= slot {
            *next = slot + 1;
        }
    }

    fn step_db_id(&self, step: &Step) -> i64 {
        self.step_ids
            .get(step.as_str())
            .map(|value| *value)
            .expect("step must be registered in steps table")
    }
}

impl Storage for SqliteStorage {
    fn add<V: Value>(&self, responsible: &Entity, token_id: TokenId, place: Step, value: V) {
        let token_slot = self.token_slot_for(token_id);
        let step_id = self.step_db_id(&place);
        let value_json = serde_json::to_string(&value).expect("value must serialize");
        let schema_type = Self::value_type_key::<V>();
        let schema_json = serde_json::to_string(&schemars::schema_for!(V))
            .expect("schema must serialize to JSON");
        let entity_name = (responsible != &Entity::SYSTEM).then(|| responsible.to_string());

        self.with_connection(|connection| {
            // Write step completion and step metadata enrichment in one transaction
            // to keep type/schema metadata consistent with first data row.
            let tx = connection.transaction()?;

            let entity_id: Option<i64> = if let Some(entity_name) = entity_name.as_deref() {
                tx.execute(
                    "INSERT OR IGNORE INTO entities (name) VALUES (?1)",
                    params![entity_name],
                )?;
                Some(tx.query_row(
                    "SELECT id FROM entities WHERE name = ?1",
                    params![entity_name],
                    |row| row.get(0),
                )?)
            } else {
                None
            };

            tx.execute(
                "
				INSERT INTO finished_steps (
					instance_id,
					step_id,
					token_slot,
					value_json,
					entity_id
				)
				VALUES (?1, ?2, ?3, ?4, ?5)
				",
                params![
                    self.instance_id.to_string(),
                    step_id,
                    token_slot,
                    value_json,
                    entity_id,
                ],
            )?;

            tx.execute(
                "
				UPDATE steps
				SET value_schema_json = COALESCE(value_schema_json, ?1),
					rust_type = COALESCE(rust_type, ?2)
				WHERE id = ?3
				",
                params![schema_json, schema_type, step_id],
            )?;

            tx.commit()?;

            Ok(())
        });
    }

    fn current_places(&self) -> Vec<Step> {
        self.with_connection(|connection| {
            // Compute the latest row per token slot and map back to step names.
            let mut statement = connection.prepare(
                "
				WITH latest_per_token AS (
					SELECT token_slot, MAX(id) AS max_id
					FROM finished_steps
					WHERE instance_id = ?1
					GROUP BY token_slot
				)
				SELECT s.step_name
				FROM latest_per_token l
				JOIN finished_steps f ON f.id = l.max_id
				JOIN steps s ON s.id = f.step_id
				",
            )?;
            let rows = statement.query_map(params![self.instance_id.to_string()], |row| {
                row.get::<_, String>(0)
            })?;

            let mut places = Vec::new();
            for row in rows {
                let step_name = row?;
                if let Some(step) = self.steps.get(&step_name) {
                    places.push(step);
                }
            }
            Ok(places)
        })
    }

    fn get_last<T: Value>(&self, token_ids: &[TokenId]) -> Option<T> {
        let token_slots = self.token_slots_for(token_ids);
        if token_slots.is_empty() {
            return None;
        }
        let schema_type = Self::value_type_key::<T>();

        let placeholders = vec!["?"; token_slots.len()].join(", ");
        let query = format!(
            "
			SELECT f.value_json
			FROM finished_steps f
			JOIN steps s ON s.id = f.step_id
			WHERE f.instance_id = ?1
			  AND s.rust_type = ?2
			  AND f.token_slot IN ({placeholders})
			ORDER BY f.id DESC
			LIMIT 1
			"
        );

        let mut params: Vec<rusqlite::types::Value> =
            vec![self.instance_id.to_string().into(), schema_type.into()];
        params.extend(token_slots.into_iter().map(rusqlite::types::Value::from));

        let raw_json: Option<String> = self.with_connection(|connection| {
            connection
                .query_row(&query, rusqlite::params_from_iter(params), |row| row.get(0))
                .optional()
        });
        raw_json.and_then(|json| serde_json::from_str::<T>(&json).ok())
    }

    fn last_step(&self, token_ids: &[TokenId]) -> Option<Step> {
        let token_slots = self.token_slots_for(token_ids);
        if token_slots.is_empty() {
            return None;
        }

        let placeholders = vec!["?"; token_slots.len()].join(", ");
        let query = format!(
            "
			SELECT s.step_name
			FROM finished_steps f
			JOIN steps s ON s.id = f.step_id
			WHERE f.instance_id = ?1
			  AND f.token_slot IN ({placeholders})
			ORDER BY f.id DESC
			LIMIT 1
			"
        );

        let mut params: Vec<rusqlite::types::Value> = vec![self.instance_id.to_string().into()];
        params.extend(token_slots.into_iter().map(rusqlite::types::Value::from));

        let raw_step: Option<String> = self.with_connection(|connection| {
            // Last finished step across the visible token slots.
            connection
                .query_row(&query, rusqlite::params_from_iter(params), |row| row.get(0))
                .optional()
        });
        raw_step.and_then(|name| self.steps.get(&name))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sqlite_storage_stores_system_entity_as_null() {
        let storage = SqliteStorage::for_test();
        let step = storage.steps.get("a").expect("test step must exist");
        let token_id = Token::new(Entity::SYSTEM, storage.clone()).id();

        storage.add(&Entity::SYSTEM, token_id, step, 13_i32);

        let (entity_id, entity_count): (Option<i64>, i64) = storage.with_connection(|connection| {
            let entity_id = connection.query_row(
                "SELECT entity_id FROM finished_steps WHERE instance_id = ?1",
                params![storage.instance_id.to_string()],
                |row| row.get(0),
            )?;
            let entity_count =
                connection.query_row("SELECT COUNT(*) FROM entities", [], |row| row.get(0))?;
            Ok((entity_id, entity_count))
        });

        assert_eq!(entity_id, None);
        assert_eq!(entity_count, 0);
    }

    #[test]
    fn sqlite_storage_normalizes_non_system_entities_via_foreign_key() {
        let storage = SqliteStorage::for_test();
        let step = storage.steps.get("a").expect("test step must exist");
        let responsible = Entity::new("Alice");
        let first_token_id = Token::new(responsible.clone(), storage.clone()).id();
        let second_token_id = Token::new(responsible.clone(), storage.clone()).id();

        storage.add(&responsible, first_token_id, step.clone(), 13_i32);
        storage.add(&responsible, second_token_id, step, 21_i32);

        let rows: Vec<(Option<i64>, Option<String>)> = storage.with_connection(|connection| {
            let mut statement = connection.prepare(
                "
					SELECT f.entity_id, e.name
					FROM finished_steps f
					LEFT JOIN entities e ON e.id = f.entity_id
					WHERE f.instance_id = ?1
					ORDER BY f.id ASC
					",
            )?;
            let rows = statement.query_map(params![storage.instance_id.to_string()], |row| {
                Ok((
                    row.get::<_, Option<i64>>(0)?,
                    row.get::<_, Option<String>>(1)?,
                ))
            })?;

            let mut collected = Vec::new();
            for row in rows {
                collected.push(row?);
            }
            Ok(collected)
        });
        let entity_count: i64 = storage.with_connection(|connection| {
            connection.query_row("SELECT COUNT(*) FROM entities", [], |row| row.get(0))
        });

        assert_eq!(rows.len(), 2);
        assert!(
            rows.iter()
                .all(|(entity_id, name)| entity_id.is_some() && name.as_deref() == Some("Alice"))
        );
        assert_eq!(rows[0].0, rows[1].0);
        assert_eq!(entity_count, 1);
    }

    // ── tests added by user ──────────────────────────────────────────────────

    use std::{
        sync::{Arc, Barrier},
        thread,
    };

    use crate::bpmn::{MetaData, Process, ProcessBuilder, Runtime, gateways};
    use crate::executor::TokioExecutor;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct SqliteDraftProcess;

    impl Process for SqliteDraftProcess {
        type Input = i32;
        type Output = i32;

        fn metadata(&self) -> &MetaData {
            static META: MetaData = MetaData::new("sqlite-draft", "SQLite backend draft tests");
            &META
        }

        fn define<S: Storage>(
            &self,
            builder: ProcessBuilder<Self, Self::Input, S>,
        ) -> ProcessBuilder<Self, Self::Output, S> {
            builder.then("identity", |_token, value| value)
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct SqliteComplexProcess;

    impl Process for SqliteComplexProcess {
        type Input = i32;
        type Output = [i32; 2];

        fn metadata(&self) -> &MetaData {
            static META: MetaData = MetaData::new("sqlite-complex", "SQLite complex resume tests");
            &META
        }

        fn define<S: Storage>(
            &self,
            builder: ProcessBuilder<Self, Self::Input, S>,
        ) -> ProcessBuilder<Self, Self::Output, S> {
            let [left, right] = builder
                .then("prepare", |_token, value| value)
                .split(gateways::And("parallel".into()));

            ProcessBuilder::join(
                gateways::And("join".into()),
                [
                    left.then("left", |_token, value| value + 1),
                    right.then("right", |_token, value| value + 2),
                ],
            )
        }
    }

    #[test]
    fn sqlite_storage_roundtrip_for_values_and_steps() {
        let steps = Steps::new(["a", "b"].into_iter()).expect("test steps must be valid");
        let storage = SqliteStorage::for_test();
        let root = Token::new(Entity::SYSTEM, storage.clone())
            .set_output(steps.start().into(), 11_i32)
            .set_output(steps.get("a").expect("step a exists"), 22_i32)
            .set_output(steps.get("b").expect("step b exists"), 33_i32);

        assert_eq!(root.get_last::<i32>(), Some(33));
        assert_eq!(
            root.last_step().as_ref().map(Step::as_str),
            Some(steps.get("b").expect("step b exists").as_str())
        );

        let places = storage.current_places();
        assert_eq!(places.len(), 1);
        assert_eq!(
            places[0].as_str(),
            steps.get("b").expect("step b exists").as_str()
        );
    }

    #[test]
    fn sqlite_backend_persists_process_and_steps_on_new_instance() {
        let backend = Sqlite::in_memory().expect("in-memory sqlite should be available");
        let mut runtime = Runtime::new(TokioExecutor, backend.clone());
        runtime
            .register_process(SqliteDraftProcess)
            .expect("process registration must succeed");

        let process_name = ProcessName::from(SqliteDraftProcess.metadata());
        let registered = runtime
            .get_registered_process(&process_name)
            .expect("registered process must exist");
        let instance_id: InstanceId = uuid::Uuid::new_v4()
            .to_string()
            .parse()
            .expect("uuid must parse to instance id");

        let _storage = backend.new_instance(registered, instance_id);

        let (process_count, step_count): (i64, i64) = backend.with_connection(|connection| {
            let process_count = connection.query_row(
                "SELECT COUNT(*) FROM processes WHERE name = ?1 AND version = ?2",
                params![
                    SqliteDraftProcess.metadata().name.as_ref(),
                    SqliteDraftProcess.metadata().version
                ],
                |row| row.get(0),
            )?;
            let step_count = connection.query_row(
                "
				SELECT COUNT(*)
				FROM steps s
				JOIN processes p ON p.id = s.process_id
				WHERE p.name = ?1 AND p.version = ?2
				",
                params![
                    SqliteDraftProcess.metadata().name.as_ref(),
                    SqliteDraftProcess.metadata().version
                ],
                |row| row.get(0),
            )?;
            Ok((process_count, step_count))
        });

        assert_eq!(process_count, 1);
        assert!(step_count >= 3, "Start, identity and End are expected");
    }

    #[test]
    fn sqlite_backend_writes_normalized_history_rows() {
        let backend = Sqlite::in_memory().expect("in-memory sqlite should be available");
        let mut runtime = Runtime::new(TokioExecutor, backend.clone());
        runtime
            .register_process(SqliteDraftProcess)
            .expect("process registration must succeed");

        let process_name = ProcessName::from(SqliteDraftProcess.metadata());
        let registered = runtime
            .get_registered_process(&process_name)
            .expect("registered process must exist");
        let instance_id: InstanceId = uuid::Uuid::new_v4()
            .to_string()
            .parse()
            .expect("uuid must parse to instance id");

        let storage = backend.new_instance(registered, instance_id);
        let identity = registered
            .steps
            .get("identity")
            .expect("identity step must exist");
        let token_id = Token::new(Entity::SYSTEM, storage.clone()).id();
        storage.add(&Entity::SYSTEM, token_id, identity, 13_i32);

        let (instances_count, history_count): (i64, i64) = backend.with_connection(|connection| {
            let instances_count = connection.query_row(
                "SELECT COUNT(*) FROM instances WHERE id = ?1",
                params![instance_id.to_string()],
                |row| row.get(0),
            )?;
            let history_count = connection.query_row(
                "SELECT COUNT(*) FROM finished_steps WHERE instance_id = ?1",
                params![instance_id.to_string()],
                |row| row.get(0),
            )?;
            Ok((instances_count, history_count))
        });

        assert_eq!(instances_count, 1);
        assert_eq!(history_count, 1);
    }

    #[test]
    fn sqlite_backend_query_returns_values_in_insertion_order() {
        let backend = Sqlite::in_memory().expect("in-memory sqlite should be available");
        let mut runtime = Runtime::new(TokioExecutor, backend.clone());
        runtime
            .register_process(SqliteDraftProcess)
            .expect("process registration must succeed");

        let process_name = ProcessName::from(SqliteDraftProcess.metadata());
        let registered = runtime
            .get_registered_process(&process_name)
            .expect("registered process must exist");
        let instance_id: InstanceId = uuid::Uuid::new_v4()
            .to_string()
            .parse()
            .expect("uuid must parse to instance id");

        let storage = backend.new_instance(registered, instance_id);
        let identity = registered
            .steps
            .get("identity")
            .expect("identity step must exist");
        let token = Token::new(Entity::SYSTEM, storage.clone());
        storage.add(&Entity::SYSTEM, token.id(), identity.clone(), 13_i32);
        storage.add(&Entity::SYSTEM, token.id(), identity.clone(), 21_i32);

        let values = backend
            .query(registered, identity, instance_id)
            .expect("query must succeed");

        assert_eq!(values, vec![serde_json::json!(13), serde_json::json!(21)]);
    }

    #[test]
    fn sqlite_backend_query_reports_process_mismatch() {
        let backend = Sqlite::in_memory().expect("in-memory sqlite should be available");
        let mut runtime = Runtime::new(TokioExecutor, backend.clone());
        runtime
            .register_process(SqliteDraftProcess)
            .expect("draft process registration must succeed");
        runtime
            .register_process(SqliteComplexProcess)
            .expect("complex process registration must succeed");

        let draft_name = ProcessName::from(SqliteDraftProcess.metadata());
        let draft = runtime
            .get_registered_process(&draft_name)
            .expect("draft process must exist");
        let draft_instance_id: InstanceId = uuid::Uuid::new_v4()
            .to_string()
            .parse()
            .expect("uuid must parse to instance id");
        let draft_storage = backend.new_instance(draft, draft_instance_id);
        let draft_step = draft
            .steps
            .get("identity")
            .expect("identity step must exist");
        draft_storage.add(
            &Entity::SYSTEM,
            Token::new(Entity::SYSTEM, draft_storage.clone()).id(),
            draft_step,
            11_i32,
        );

        let complex_name = ProcessName::from(SqliteComplexProcess.metadata());
        let complex = runtime
            .get_registered_process(&complex_name)
            .expect("complex process must exist");
        let complex_step = complex
            .steps
            .get("prepare")
            .expect("prepare step must exist");

        let error = backend
            .query(complex, complex_step, draft_instance_id)
            .expect_err("query must fail due to process mismatch");

        assert_eq!(error, StorageError::ProcessMismatch);
    }

    #[test]
    fn sqlite_backend_reports_unfinished_instances_via_end_step_absence() {
        let backend = Sqlite::in_memory().expect("in-memory sqlite should be available");
        let mut runtime = Runtime::new(TokioExecutor, backend.clone());
        runtime
            .register_process(SqliteDraftProcess)
            .expect("process registration must succeed");

        let process_name = ProcessName::from(SqliteDraftProcess.metadata());
        let registered = runtime
            .get_registered_process(&process_name)
            .expect("registered process must exist");
        let instance_id: InstanceId = uuid::Uuid::new_v4()
            .to_string()
            .parse()
            .expect("uuid must parse to instance id");

        let storage = backend.new_instance(registered, instance_id);
        let identity = registered
            .steps
            .get("identity")
            .expect("identity step must exist");
        let token_id = Token::new(Entity::SYSTEM, storage.clone()).id();

        storage.add(&Entity::SYSTEM, token_id, identity, 7_i32);

        let unfinished = backend.unfinished_instances();
        assert_eq!(unfinished.len(), 1);
        assert_eq!(unfinished[0].0.to_string(), process_name.to_string());
        assert_eq!(unfinished[0].1, instance_id);

        storage.add(&Entity::SYSTEM, token_id, registered.steps.end(), 9_i32);
        assert!(backend.unfinished_instances().is_empty());
    }

    #[test]
    fn sqlite_backend_reconstructs_resumable_state_from_current_places() {
        let backend = Sqlite::in_memory().expect("in-memory sqlite should be available");
        let mut runtime = Runtime::new(TokioExecutor, backend.clone());
        runtime
            .register_process(SqliteDraftProcess)
            .expect("process registration must succeed");

        let process_name = ProcessName::from(SqliteDraftProcess.metadata());
        let registered = runtime
            .get_registered_process(&process_name)
            .expect("registered process must exist");
        let instance_id: InstanceId = uuid::Uuid::new_v4()
            .to_string()
            .parse()
            .expect("uuid must parse to instance id");

        let storage = backend.new_instance(registered, instance_id);
        let identity = registered
            .steps
            .get("identity")
            .expect("identity step must exist");
        let token_id = Token::new(Entity::SYSTEM, storage.clone()).id();
        storage.add(&Entity::SYSTEM, token_id, identity.clone(), 42_i32);

        let resumable = backend
            .resume_instance(registered, instance_id)
            .expect("instance must be resumable");

        assert_eq!(resumable.id, instance_id);
        assert_eq!(resumable.current_state.len(), 1);

        let token = resumable.current_state[0].1.clone();
        assert_eq!(token.get_last::<i32>(), Some(42));

        let place = resumable.current_state[0].0;
        let expected_place = registered
            .place_after_step(&identity)
            .expect("place for identity step must be resolvable");
        assert_eq!(place, expected_place);
    }

    #[test]
    fn sqlite_backend_reconstructs_multiple_parallel_tokens() {
        let backend = Sqlite::in_memory().expect("in-memory sqlite should be available");
        let mut runtime = Runtime::new(TokioExecutor, backend.clone());
        runtime
            .register_process(SqliteComplexProcess)
            .expect("process registration must succeed");

        let process_name = ProcessName::from(SqliteComplexProcess.metadata());
        let registered = runtime
            .get_registered_process(&process_name)
            .expect("registered process must exist");
        let instance_id: InstanceId = uuid::Uuid::new_v4()
            .to_string()
            .parse()
            .expect("uuid must parse to instance id");

        let storage = backend.new_instance(registered, instance_id);
        let left_step = registered.steps.get("left").expect("left step must exist");
        let right_step = registered
            .steps
            .get("right")
            .expect("right step must exist");

        let seed = Token::new(Entity::SYSTEM, storage.clone());
        let left = seed.fork().set_output(left_step.clone(), 11_i32);
        let right = seed.fork().set_output(right_step.clone(), 22_i32);

        let resumable = backend
            .resume_instance(registered, instance_id)
            .expect("instance must be resumable");
        assert_eq!(resumable.current_state.len(), 2);

        let mut values = resumable
            .current_state
            .iter()
            .map(|(_, token)| token.get_last::<i32>().expect("token value must exist"))
            .collect::<Vec<_>>();
        values.sort();
        assert_eq!(values, vec![11, 22]);

        let mut place_names = resumable
            .current_state
            .iter()
            .map(|(place, _)| place)
            .map(|place| {
                if *place
                    == registered
                        .place_after_step(&left_step)
                        .expect("left place should resolve")
                {
                    "left"
                } else {
                    "right"
                }
            })
            .collect::<Vec<_>>();
        place_names.sort();
        assert_eq!(place_names, vec!["left", "right"]);

        // Mark only one branch as finished: instance must still be resumable.
        storage.add(&Entity::SYSTEM, left.id(), registered.steps.end(), ());
        assert_eq!(backend.unfinished_instances().len(), 1);

        // Mark second branch as finished: instance disappears from unfinished query.
        storage.add(&Entity::SYSTEM, right.id(), registered.steps.end(), ());
        assert!(backend.unfinished_instances().is_empty());
    }

    #[test]
    fn sqlite_backend_keeps_token_slots_isolated_across_concurrent_processes() {
        let backend = Sqlite::in_memory().expect("in-memory sqlite should be available");
        let mut runtime = Runtime::new(TokioExecutor, backend.clone());
        runtime
            .register_process(SqliteDraftProcess)
            .expect("draft process registration must succeed");
        runtime
            .register_process(SqliteComplexProcess)
            .expect("complex process registration must succeed");

        let draft_name = ProcessName::from(SqliteDraftProcess.metadata());
        let draft = runtime
            .get_registered_process(&draft_name)
            .expect("draft process must exist");
        let draft_instance_id: InstanceId = uuid::Uuid::new_v4()
            .to_string()
            .parse()
            .expect("uuid must parse to instance id");
        let draft_storage = backend.new_instance(draft, draft_instance_id);
        let draft_step = draft
            .steps
            .get("identity")
            .expect("identity step must exist");
        let draft_token_id = Token::new(Entity::SYSTEM, draft_storage.clone()).id();

        let complex_name = ProcessName::from(SqliteComplexProcess.metadata());
        let complex = runtime
            .get_registered_process(&complex_name)
            .expect("complex process must exist");
        let complex_instance_id: InstanceId = uuid::Uuid::new_v4()
            .to_string()
            .parse()
            .expect("uuid must parse to instance id");
        let complex_storage = backend.new_instance(complex, complex_instance_id);
        let complex_step = complex
            .steps
            .get("prepare")
            .expect("prepare step must exist");
        let complex_token_id = Token::new(Entity::SYSTEM, complex_storage.clone()).id();

        let start = Arc::new(Barrier::new(3));

        let draft_start = start.clone();
        let draft_handle = thread::spawn(move || {
            draft_start.wait();
            draft_storage.add(&Entity::SYSTEM, draft_token_id, draft_step, 11_i32);
        });

        let complex_start = start.clone();
        let complex_handle = thread::spawn(move || {
            complex_start.wait();
            complex_storage.add(&Entity::SYSTEM, complex_token_id, complex_step, 22_i32);
        });

        start.wait();
        draft_handle
            .join()
            .expect("draft writer thread must finish");
        complex_handle
            .join()
            .expect("complex writer thread must finish");

        let slot_rows: Vec<(String, i64)> = backend.with_connection(|connection| {
            let mut statement = connection.prepare(
                "
                SELECT instance_id, token_slot
                FROM finished_steps
                WHERE instance_id IN (?1, ?2)
                ORDER BY instance_id, id
                ",
            )?;
            let rows = statement.query_map(
                params![
                    draft_instance_id.to_string(),
                    complex_instance_id.to_string()
                ],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
            )?;

            let mut collected = Vec::new();
            for row in rows {
                collected.push(row?);
            }
            Ok(collected)
        });

        assert_eq!(slot_rows.len(), 2);
        assert!(slot_rows.contains(&(draft_instance_id.to_string(), 0)));
        assert!(slot_rows.contains(&(complex_instance_id.to_string(), 0)));
    }
}
