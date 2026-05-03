use std::{any::TypeId, sync::Arc};

use dashmap::DashMap;
use parking_lot::Mutex;
use postgres::NoTls;
use r2d2::Pool;
use r2d2_postgres::PostgresConnectionManager;
use serde_json::Value as JsonValue;

use crate::bpmn::{
    InstanceId, ProcessName, RegisteredProcess, Step, Steps, Token, TokenId, Value,
    messages::Entity,
    storage::{
        FinishedStep, InstanceDetails, ResumableProcess, Storage, StorageBackend, StorageError,
    },
};

/// Errors emitted while opening or preparing PostgreSQL storage.
#[derive(Debug)]
pub enum PostgresError {
    /// Invalid PostgreSQL connection configuration.
    InvalidConfig(postgres::Error),
    /// Error while building the connection pool.
    PoolBuild(r2d2::Error),
    /// Error while acquiring a pooled connection.
    Pool(r2d2::Error),
    /// Any error returned by the underlying PostgreSQL driver.
    Sql(postgres::Error),
}

impl std::fmt::Display for PostgresError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PostgresError::InvalidConfig(error) => {
                write!(f, "PostgreSQL config error: {error}")
            }
            PostgresError::PoolBuild(error) => write!(f, "PostgreSQL pool build error: {error}"),
            PostgresError::Pool(error) => write!(f, "PostgreSQL pool error: {error}"),
            PostgresError::Sql(error) => write!(f, "PostgreSQL error: {error}"),
        }
    }
}

impl std::error::Error for PostgresError {}

impl From<postgres::Error> for PostgresError {
    fn from(value: postgres::Error) -> Self {
        PostgresError::Sql(value)
    }
}

impl From<r2d2::Error> for PostgresError {
    fn from(value: r2d2::Error) -> Self {
        PostgresError::Pool(value)
    }
}

/// A PostgreSQL based storage backend.
///
/// This backend is optimized for concurrent read/write access by using
/// a pooled set of client connections and indexed history queries.
#[derive(Debug, Clone)]
pub struct Postgres {
    pool: Arc<Pool<PostgresConnectionManager<NoTls>>>,
}

impl Postgres {
    /// Connect to PostgreSQL and initialize the storage schema.
    pub fn connect(database_url: &str) -> Result<Self, PostgresError> {
        Self::connect_with_pool_size(database_url, 16)
    }

    /// Connect to PostgreSQL with a custom pool size.
    pub fn connect_with_pool_size(
        database_url: &str,
        max_pool_size: u32,
    ) -> Result<Self, PostgresError> {
        let config = database_url
            .parse::<postgres::Config>()
            .map_err(PostgresError::InvalidConfig)?;
        let manager = PostgresConnectionManager::new(config, NoTls);
        let pool = Pool::builder()
            .max_size(max_pool_size)
            .build(manager)
            .map_err(PostgresError::PoolBuild)?;

        {
            let mut connection = pool.get().map_err(PostgresError::Pool)?;
            Self::init_schema(&mut connection)?;
        }

        Ok(Self {
            pool: Arc::new(pool),
        })
    }

    fn init_schema(client: &mut postgres::Client) -> Result<(), postgres::Error> {
        client.batch_execute(
            "
            CREATE TABLE IF NOT EXISTS processes (
                id BIGSERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                version INTEGER NOT NULL,
                UNIQUE(name, version)
            );

            CREATE TABLE IF NOT EXISTS steps (
                id BIGSERIAL PRIMARY KEY,
                process_id BIGINT NOT NULL REFERENCES processes(id) ON DELETE CASCADE,
                step_name TEXT NOT NULL,
                rust_type TEXT,
                value_schema_json JSONB,
                UNIQUE(process_id, step_name)
            );

            CREATE TABLE IF NOT EXISTS instances (
                id TEXT PRIMARY KEY,
                process_id BIGINT NOT NULL REFERENCES processes(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS entities (
                id BIGSERIAL PRIMARY KEY,
                name TEXT NOT NULL UNIQUE
            );

            CREATE TABLE IF NOT EXISTS finished_steps (
                id BIGSERIAL PRIMARY KEY,
                instance_id TEXT NOT NULL REFERENCES instances(id) ON DELETE CASCADE,
                step_id BIGINT NOT NULL REFERENCES steps(id) ON DELETE RESTRICT,
                token_slot BIGINT NOT NULL,
                value_json JSONB NOT NULL,
                entity_id BIGINT REFERENCES entities(id) ON DELETE RESTRICT,
                timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
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

    fn with_client<T>(
        &self,
        op: impl FnOnce(&mut postgres::Client) -> Result<T, postgres::Error>,
    ) -> T {
        let mut client = self
            .pool
            .get()
            .expect("failed to acquire PostgreSQL pooled connection");
        op(&mut client).expect("PostgreSQL operation failed")
    }
}

impl StorageBackend for Postgres {
    type Storage = PostgresStorage;

    fn query(
        &self,
        process: &RegisteredProcess<Self>,
        step: Step,
        instance_id: InstanceId,
    ) -> Result<Vec<FinishedStep>, StorageError> {
        let found_process: Option<(i64, String, i32)> = self.with_client(|client| {
            client
                .query_opt(
                    "
                    SELECT p.id, p.name, p.version
                    FROM instances i
                    JOIN processes p ON p.id = i.process_id
                    WHERE i.id = $1
                    ",
                    &[&instance_id.to_string()],
                )
                .map(|opt| opt.map(|row| (row.get(0), row.get(1), row.get(2))))
        });

        let Some((process_db_id, name, version)) = found_process else {
            return Err(StorageError::NotFound);
        };

        if name != process.meta_data.name.as_ref()
            || u32::try_from(version).ok() != Some(process.meta_data.version)
        {
            return Err(StorageError::ProcessMismatch);
        }

        let step_db_id: Option<i64> = self.with_client(|client| {
            client
                .query_opt(
                    "SELECT id FROM steps WHERE process_id = $1 AND step_name = $2",
                    &[&process_db_id, &step.as_str()],
                )
                .map(|opt| opt.map(|row| row.get(0)))
        });

        let Some(step_db_id) = step_db_id else {
            return Ok(Vec::new());
        };

        let rows: Vec<FinishedStep> = self.with_client(|client| {
            let rows = client.query(
                "
                SELECT f.value_json, f.timestamp, e.name
                FROM finished_steps f
                LEFT JOIN entities e ON e.id = f.entity_id
                WHERE f.instance_id = $1 AND f.step_id = $2
                ORDER BY f.id ASC
                ",
                &[&instance_id.to_string(), &step_db_id],
            )?;

            Ok(rows
                .into_iter()
                .map(|row| FinishedStep {
                    output: row.get::<_, JsonValue>(0),
                    timestamp: row.get::<_, chrono::DateTime<chrono::Utc>>(1),
                    responsible: row
                        .get::<_, Option<String>>(2)
                        .map(Entity::from)
                        .unwrap_or(Entity::SYSTEM),
                })
                .collect())
        });

        Ok(rows)
    }

    fn query_all(
        &self,
        process: &RegisteredProcess<Self>,
    ) -> Result<Vec<InstanceDetails>, StorageError> {
        self.with_client(|client| {
            let rows = client.query(
                "
                    WITH target_process AS (
                        SELECT id
                        FROM processes
                        WHERE name = $1 AND version = $2
                    ),
                    latest_per_instance AS (
                        SELECT f.instance_id, MAX(f.id) AS max_id
                        FROM finished_steps f
                        JOIN instances i ON i.id = f.instance_id
                        JOIN target_process p ON p.id = i.process_id
                        GROUP BY f.instance_id
                    )
                    SELECT f.instance_id, s.step_name, f.value_json, f.timestamp, e.name
                    FROM latest_per_instance l
                    JOIN finished_steps f ON f.id = l.max_id
                    JOIN steps s ON s.id = f.step_id
                    LEFT JOIN entities e ON e.id = f.entity_id
                    ORDER BY f.id ASC
                    ",
                &[
                    &process.meta_data.name.as_ref(),
                    &i32::try_from(process.meta_data.version)
                        .expect("process version must fit into PostgreSQL INTEGER"),
                ],
            )?;

            Ok(rows
                .into_iter()
                .map(|row| {
                    let (instance_id_raw, step_name, output, timestamp, entity_name) = (
                        row.get::<_, String>(0),
                        row.get::<_, String>(1),
                        row.get::<_, JsonValue>(2),
                        row.get::<_, chrono::DateTime<chrono::Utc>>(3),
                        row.get::<_, Option<String>>(4),
                    );

                    let Ok(instance_id) = instance_id_raw.parse() else {
                        // Skip rows with invalid instance IDs, which should not happen unless the database was tampered with.
                        return None;
                    };
                    let Some(step) = process.steps.get(&step_name) else {
                        // Skip rows with invalid step names, which should not happen unless the database was tampered with.
                        return None;
                    };

                    InstanceDetails {
                        instance_id,
                        step,
                        data: FinishedStep {
                            timestamp,
                            responsible: entity_name.map(Entity::from).unwrap_or(Entity::SYSTEM),
                            output,
                        },
                    }
                })
                .collect())
        })
    }

    fn new_instance(
        &self,
        process: &RegisteredProcess<Self>,
        process_id: InstanceId,
    ) -> Self::Storage {
        let process_name = process.meta_data.name.to_string();
        let process_version = i32::try_from(process.meta_data.version)
            .expect("process version must fit into PostgreSQL INTEGER");

        let step_ids = self.with_client(|client| {
            let mut tx = client.transaction()?;

            tx.execute(
                "INSERT INTO processes (name, version) VALUES ($1, $2) ON CONFLICT (name, version) DO NOTHING",
                &[&process_name, &process_version],
            )?;

            let process_db_id: i64 = tx
                .query_one(
                    "SELECT id FROM processes WHERE name = $1 AND version = $2",
                    &[&process.meta_data.name.as_ref(), &process_version],
                )?
                .get(0);

            for step in process.steps.steps() {
                tx.execute(
                    "
                    INSERT INTO steps (process_id, step_name, rust_type, value_schema_json)
                    VALUES ($1, $2, NULL, NULL)
                    ON CONFLICT (process_id, step_name) DO NOTHING
                    ",
                    &[&process_db_id, &step],
                )?;
            }

            tx.execute(
                "INSERT INTO instances (id, process_id) VALUES ($1, $2) ON CONFLICT (id) DO NOTHING",
                &[&process_id.to_string(), &process_db_id],
            )?;

            let rows = tx.query(
                "SELECT step_name, id FROM steps WHERE process_id = $1",
                &[&process_db_id],
            )?;
            let step_ids = DashMap::new();
            for row in rows {
                let name: String = row.get(0);
                let id: i64 = row.get(1);
                step_ids.insert(name, id);
            }

            tx.commit()?;

            Ok(step_ids)
        });

        PostgresStorage {
            pool: self.pool.clone(),
            steps: process.steps.clone(),
            instance_id: process_id,
            instance_id_raw: Arc::from(process_id.to_string()),
            token_slots: Arc::new(DashMap::new()),
            step_ids: Arc::new(step_ids),
            next_token_slot: Arc::new(Mutex::new(0)),
            value_schema_cache: Arc::new(DashMap::new()),
            initialized_step_metadata: Arc::new(DashMap::new()),
        }
    }

    fn resume_instance(
        &self,
        process: &RegisteredProcess<Self>,
        process_id: InstanceId,
    ) -> Result<ResumableProcess<Self>, StorageError> {
        let found_process: Option<(i64, String, i32)> = self.with_client(|client| {
            client
                .query_opt(
                    "
                    SELECT p.id, p.name, p.version
                    FROM instances i
                    JOIN processes p ON p.id = i.process_id
                    WHERE i.id = $1
                    ",
                    &[&process_id.to_string()],
                )
                .map(|opt| opt.map(|row| (row.get(0), row.get(1), row.get(2))))
        });

        let Some((process_db_id, name, version)) = found_process else {
            return Err(StorageError::NotFound);
        };

        if name != process.meta_data.name.as_ref()
            || u32::try_from(version).ok() != Some(process.meta_data.version)
        {
            return Err(StorageError::ProcessMismatch);
        }

        let (step_ids, next_token_slot) = self.with_client(|client| {
            let rows = client.query(
                "SELECT step_name, id FROM steps WHERE process_id = $1",
                &[&process_db_id],
            )?;
            let step_ids = DashMap::new();
            for row in rows {
                let name: String = row.get(0);
                let id: i64 = row.get(1);
                step_ids.insert(name, id);
            }

            let max_slot: Option<i64> = client
                .query_one(
                    "SELECT MAX(token_slot) FROM finished_steps WHERE instance_id = $1",
                    &[&process_id.to_string()],
                )?
                .get(0);

            Ok((step_ids, max_slot.map_or(0, |slot| slot + 1)))
        });

        let storage = PostgresStorage {
            pool: self.pool.clone(),
            steps: process.steps.clone(),
            instance_id: process_id,
            instance_id_raw: Arc::from(process_id.to_string()),
            token_slots: Arc::new(DashMap::new()),
            step_ids: Arc::new(step_ids),
            next_token_slot: Arc::new(Mutex::new(next_token_slot)),
            value_schema_cache: Arc::new(DashMap::new()),
            initialized_step_metadata: Arc::new(DashMap::new()),
        };

        let rows: Vec<(i64, String)> = storage.with_client(|client| {
            let rows = client.query(
                "
                WITH latest_per_token AS (
                    SELECT token_slot, MAX(id) AS max_id
                    FROM finished_steps
                    WHERE instance_id = $1
                    GROUP BY token_slot
                )
                SELECT f.token_slot, s.step_name
                FROM finished_steps f
                JOIN latest_per_token l ON l.max_id = f.id
                JOIN steps s ON s.id = f.step_id
                ",
                &[&process_id.to_string()],
            )?;

            Ok(rows
                .into_iter()
                .map(|row| (row.get::<_, i64>(0), row.get::<_, String>(1)))
                .collect())
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
        self.with_client(|client| {
            let rows = client.query(
                "
                WITH latest_per_token AS (
                    SELECT instance_id, token_slot, MAX(id) AS max_id
                    FROM finished_steps
                    GROUP BY instance_id, token_slot
                ),
                current_tokens AS (
                    SELECT l.instance_id, s.step_name
                    FROM latest_per_token l
                    JOIN finished_steps f ON f.id = l.max_id
                    JOIN steps s ON s.id = f.step_id
                )
                SELECT p.name, p.version, i.id
                FROM instances i
                JOIN processes p ON p.id = i.process_id
                LEFT JOIN current_tokens c ON c.instance_id = i.id
                GROUP BY p.name, p.version, i.id
                HAVING COALESCE(SUM(CASE WHEN c.step_name = $1 THEN 0 ELSE 1 END), 1) > 0
                ",
                &[&Step::END],
            )?;

            let mut unfinished = Vec::new();
            for row in rows {
                let name: String = row.get(0);
                let version: i32 = row.get(1);
                let instance_id_raw: String = row.get(2);

                let Ok(version_u32) = u32::try_from(version) else {
                    continue;
                };

                let Ok(process_name) = format!("{name}-{version_u32}").parse() else {
                    continue;
                };
                let Ok(instance_id) = instance_id_raw.parse() else {
                    continue;
                };

                unfinished.push((process_name, instance_id));
            }

            Ok(unfinished)
        })
    }
}

/// Storage for one process instance backed by PostgreSQL tables.
#[derive(Clone)]
pub struct PostgresStorage {
    pool: Arc<Pool<PostgresConnectionManager<NoTls>>>,
    steps: Steps,
    instance_id: InstanceId,
    instance_id_raw: Arc<str>,
    token_slots: Arc<DashMap<TokenId, i64>>,
    step_ids: Arc<DashMap<String, i64>>,
    next_token_slot: Arc<Mutex<i64>>,
    value_schema_cache: Arc<DashMap<TypeId, JsonValue>>,
    initialized_step_metadata: Arc<DashMap<i64, ()>>,
}

impl std::fmt::Debug for PostgresStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresStorage")
            .field("instance_id", &self.instance_id)
            .finish_non_exhaustive()
    }
}

impl PartialEq for PostgresStorage {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.pool, &other.pool)
            && Arc::ptr_eq(&self.token_slots, &other.token_slots)
            && self.instance_id == other.instance_id
    }
}

impl Eq for PostgresStorage {}

impl PostgresStorage {
    fn instance_id_str(&self) -> &str {
        &self.instance_id_raw
    }

    fn value_schema_json_for<V: Value>(&self) -> JsonValue {
        let type_id = TypeId::of::<V>();
        if let Some(schema) = self.value_schema_cache.get(&type_id) {
            return schema.clone();
        }

        // Cache generated schemas per Rust type to avoid rebuilding and
        // serializing the same schema for every single write.
        let schema_json =
            serde_json::to_value(schemars::schema_for!(V)).expect("schema must serialize");

        self.value_schema_cache.insert(type_id, schema_json.clone());

        schema_json
    }

    fn value_type_key<V: Value>() -> String {
        <V as schemars::JsonSchema>::schema_name().into_owned()
    }

    fn with_client<T>(
        &self,
        op: impl FnOnce(&mut postgres::Client) -> Result<T, postgres::Error>,
    ) -> T {
        let mut client = self
            .pool
            .get()
            .expect("failed to acquire PostgreSQL pooled connection");
        op(&mut client).expect("PostgreSQL operation failed")
    }

    fn token_slot_for(&self, token_id: TokenId) -> i64 {
        *self.token_slots.entry(token_id).or_insert_with(|| {
            let mut next = self.next_token_slot.lock();
            let slot = *next;
            *next += 1;
            slot
        })
    }

    fn token_slots_for(&self, token_ids: &[TokenId]) -> Vec<i64> {
        token_ids
            .iter()
            .filter_map(|token_id| self.token_slots.get(token_id).map(|entry| *entry))
            .collect()
    }

    fn assign_resumed_slot(&self, token_id: TokenId, slot: i64) {
        self.token_slots.insert(token_id, slot);
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

impl Storage for PostgresStorage {
    fn add<V: Value>(&self, responsible: &Entity, token_id: TokenId, place: Step, value: V) {
        let token_slot = self.token_slot_for(token_id);
        let step_id = self.step_db_id(&place);
        let value_json = serde_json::to_value(&value).expect("value must serialize");
        let schema_type = Self::value_type_key::<V>();
        let entity_name = (responsible != &Entity::SYSTEM).then(|| responsible.to_string());
        let needs_step_metadata_update =
            self.initialized_step_metadata.insert(step_id, ()).is_none();
        let schema_json = needs_step_metadata_update.then(|| self.value_schema_json_for::<V>());

        self.with_client(|client| {
            let mut tx = client.transaction()?;

            let entity_id: Option<i64> = if let Some(entity_name) = entity_name.as_deref() {
                Some(
                    tx.query_one(
                        "
                        INSERT INTO entities (name)
                        VALUES ($1)
                        ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
                        RETURNING id
                        ",
                        &[&entity_name],
                    )?
                    .get(0),
                )
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
                VALUES ($1, $2, $3, $4, $5)
                ",
                &[
                    &self.instance_id_str(),
                    &step_id,
                    &token_slot,
                    &value_json,
                    &entity_id,
                ],
            )?;

            if let Some(schema_json) = schema_json {
                tx.execute(
                    "
                    UPDATE steps
                    SET value_schema_json = COALESCE(value_schema_json, $1),
                        rust_type = COALESCE(rust_type, $2)
                    WHERE id = $3
                    ",
                    &[&schema_json, &schema_type, &step_id],
                )?;
            }

            tx.commit()?;

            Ok(())
        });
    }

    fn current_places(&self) -> Vec<Step> {
        self.with_client(|client| {
            let rows = client.query(
                "
                WITH latest_per_token AS (
                    SELECT token_slot, MAX(id) AS max_id
                    FROM finished_steps
                    WHERE instance_id = $1
                    GROUP BY token_slot
                )
                SELECT s.step_name
                FROM latest_per_token l
                JOIN finished_steps f ON f.id = l.max_id
                JOIN steps s ON s.id = f.step_id
                ",
                &[&self.instance_id_str()],
            )?;

            let mut places = Vec::new();
            for row in rows {
                let step_name: String = row.get(0);
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

        let raw_json: Option<JsonValue> = self.with_client(|client| {
            client
                .query_opt(
                    "
                    SELECT f.value_json
                    FROM finished_steps f
                    JOIN steps s ON s.id = f.step_id
                    WHERE f.instance_id = $1
                      AND s.rust_type = $2
                      AND f.token_slot = ANY($3)
                    ORDER BY f.id DESC
                    LIMIT 1
                    ",
                    &[&self.instance_id_str(), &schema_type, &token_slots],
                )
                .map(|opt| opt.map(|row| row.get(0)))
        });

        raw_json.and_then(|json| serde_json::from_value::<T>(json).ok())
    }

    fn last_step(&self, token_ids: &[TokenId]) -> Option<Step> {
        let token_slots = self.token_slots_for(token_ids);
        if token_slots.is_empty() {
            return None;
        }

        let raw_step: Option<String> = self.with_client(|client| {
            client
                .query_opt(
                    "
                    SELECT s.step_name
                    FROM finished_steps f
                    JOIN steps s ON s.id = f.step_id
                    WHERE f.instance_id = $1
                      AND f.token_slot = ANY($2)
                    ORDER BY f.id DESC
                    LIMIT 1
                    ",
                    &[&self.instance_id_str(), &token_slots],
                )
                .map(|opt| opt.map(|row| row.get(0)))
        });

        raw_step.and_then(|name| self.steps.get(&name))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bpmn::{MetaData, Process, ProcessBuilder, Runtime, Token};
    use crate::executor::TokioExecutor;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct PostgresQueryProcess;

    impl Process for PostgresQueryProcess {
        type Input = i32;
        type Output = i32;

        fn metadata(&self) -> &MetaData {
            static META: MetaData =
                MetaData::new("postgres-query", "PostgreSQL query backend tests");
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
    struct PostgresOtherProcess;

    impl Process for PostgresOtherProcess {
        type Input = i32;
        type Output = i32;

        fn metadata(&self) -> &MetaData {
            static META: MetaData =
                MetaData::new("postgres-query-other", "PostgreSQL query mismatch tests");
            &META
        }

        fn define<S: Storage>(
            &self,
            builder: ProcessBuilder<Self, Self::Input, S>,
        ) -> ProcessBuilder<Self, Self::Output, S> {
            builder.then("identity", |_token, value| value)
        }
    }

    fn test_backend() -> Option<Postgres> {
        let database_url = std::env::var("TOWER_BPMN_TEST_POSTGRES_URL").ok()?;
        Postgres::connect(&database_url).ok()
    }

    #[test]
    fn postgres_query_returns_values_for_step_in_insert_order() {
        let Some(backend) = test_backend() else {
            return;
        };

        let mut runtime = Runtime::new(TokioExecutor, backend.clone());
        runtime
            .register_process(PostgresQueryProcess)
            .expect("process registration must succeed");

        let process_name = ProcessName::from(PostgresQueryProcess.metadata());
        let registered = runtime
            .get_registered_process(&process_name)
            .expect("registered process must exist");

        let instance_id: InstanceId = uuid::Uuid::new_v4()
            .to_string()
            .parse()
            .expect("uuid must parse to instance id");

        let storage = backend.new_instance(registered, instance_id);
        let step = registered
            .steps
            .get("identity")
            .expect("identity step must exist");

        let token = Token::new(Entity::SYSTEM, storage.clone());
        storage.add(&token.responsible, token.id(), step.clone(), 11_i32);
        storage.add(&token.responsible, token.id(), step.clone(), 22_i32);

        let finished_steps = backend
            .query(registered, step, instance_id)
            .expect("query must succeed");

        assert_eq!(finished_steps.len(), 2);
        assert_eq!(finished_steps[0].output, serde_json::json!(11));
        assert_eq!(finished_steps[1].output, serde_json::json!(22));
        assert_eq!(finished_steps[0].responsible, Entity::SYSTEM);
        assert_eq!(finished_steps[1].responsible, Entity::SYSTEM);
        assert!(finished_steps[0].timestamp <= finished_steps[1].timestamp);
    }

    #[test]
    fn postgres_query_returns_process_mismatch_for_other_process() {
        let Some(backend) = test_backend() else {
            return;
        };

        let mut runtime = Runtime::new(TokioExecutor, backend.clone());
        runtime
            .register_process(PostgresQueryProcess)
            .expect("first process registration must succeed");
        runtime
            .register_process(PostgresOtherProcess)
            .expect("second process registration must succeed");

        let process_name = ProcessName::from(PostgresQueryProcess.metadata());
        let other_process_name = ProcessName::from(PostgresOtherProcess.metadata());
        let registered = runtime
            .get_registered_process(&process_name)
            .expect("registered process must exist");
        let other_registered = runtime
            .get_registered_process(&other_process_name)
            .expect("other registered process must exist");

        let instance_id: InstanceId = uuid::Uuid::new_v4()
            .to_string()
            .parse()
            .expect("uuid must parse to instance id");

        let storage = backend.new_instance(registered, instance_id);
        let step = registered
            .steps
            .get("identity")
            .expect("identity step must exist");

        storage.add(
            &Entity::SYSTEM,
            Token::new(Entity::SYSTEM, storage.clone()).id(),
            step,
            7_i32,
        );

        let other_step = other_registered
            .steps
            .get("identity")
            .expect("other identity step must exist");

        let error = backend
            .query(other_registered, other_step, instance_id)
            .expect_err("query must fail due to process mismatch");

        assert_eq!(error, StorageError::ProcessMismatch);
    }

    #[test]
    fn postgres_query_all_returns_latest_state_per_instance() {
        let Some(backend) = test_backend() else {
            return;
        };

        let mut runtime = Runtime::new(TokioExecutor, backend.clone());
        runtime
            .register_process(PostgresQueryProcess)
            .expect("first process registration must succeed");
        runtime
            .register_process(PostgresOtherProcess)
            .expect("second process registration must succeed");

        let process_name = ProcessName::from(PostgresQueryProcess.metadata());
        let other_process_name = ProcessName::from(PostgresOtherProcess.metadata());
        let registered = runtime
            .get_registered_process(&process_name)
            .expect("registered process must exist");
        let other_registered = runtime
            .get_registered_process(&other_process_name)
            .expect("other registered process must exist");

        let step = registered
            .steps
            .get("identity")
            .expect("identity step must exist");
        let other_step = other_registered
            .steps
            .get("identity")
            .expect("other identity step must exist");

        let instance_a: InstanceId = uuid::Uuid::new_v4()
            .to_string()
            .parse()
            .expect("uuid must parse to instance id");
        let instance_b: InstanceId = uuid::Uuid::new_v4()
            .to_string()
            .parse()
            .expect("uuid must parse to instance id");
        let other_instance: InstanceId = uuid::Uuid::new_v4()
            .to_string()
            .parse()
            .expect("uuid must parse to instance id");

        let storage_a = backend.new_instance(registered, instance_a);
        let storage_b = backend.new_instance(registered, instance_b);
        let other_storage = backend.new_instance(other_registered, other_instance);

        let token_a = Token::new(Entity::SYSTEM, storage_a.clone());
        let token_b = Token::new(Entity::SYSTEM, storage_b.clone());
        let token_other = Token::new(Entity::SYSTEM, other_storage.clone());

        storage_a.add(&Entity::SYSTEM, token_a.id(), step.clone(), 10_i32);
        storage_a.add(&Entity::SYSTEM, token_a.id(), step.clone(), 20_i32);
        storage_b.add(&Entity::SYSTEM, token_b.id(), step.clone(), 30_i32);
        other_storage.add(&Entity::SYSTEM, token_other.id(), other_step, 99_i32);

        let all = backend
            .query_all(registered)
            .expect("query_all must succeed");

        assert_eq!(all.len(), 2);

        let row_a = all
            .iter()
            .find(|row| row.instance_id == instance_a)
            .expect("instance a must be present");
        let row_b = all
            .iter()
            .find(|row| row.instance_id == instance_b)
            .expect("instance b must be present");

        assert_eq!(row_a.step.as_str(), step.as_str());
        assert_eq!(row_a.data.output, serde_json::json!(20));
        assert_eq!(row_b.step.as_str(), step.as_str());
        assert_eq!(row_b.data.output, serde_json::json!(30));
        assert!(all.iter().all(|row| row.instance_id != other_instance));
    }
}
