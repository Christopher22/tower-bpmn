//! Fast route tree, callback registration and optional route-owned OpenAPI metadata.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use http::{Method as HttpMethod, StatusCode};
use http_body_util::BodyExt;
use parking_lot::RwLock;
use schemars::JsonSchema;
use serde::Serialize;

use super::{
    error::Error,
    json_response,
    openapi::{OpenApiOperationData, OpenApiPathData, OpenApiResponseData, OpenApiRouteData},
};
use crate::bpmn::{
    ExtendedExecutor, Instance, InstanceId, Instances, MetaData, ProcessName, RegisteredProcess,
    Runtime, Step, StorageBackend,
    messages::{Context, Participant},
};

type GetCallback<E, B> =
    dyn Fn(&Runtime<E, B>, &Context) -> Result<serde_json::Value, Error> + Send + Sync;
type PostCallback<E, B> = dyn Fn(&Runtime<E, B>, serde_json::Value, &Context) -> Result<serde_json::Value, Error>
    + Send
    + Sync;

/// Route-local schema descriptor used while collecting OpenAPI data.
#[derive(Clone)]
enum SchemaDoc {
    Inline(serde_json::Value),
    Component {
        name: String,
        schema: serde_json::Value,
    },
    Ref(String),
}

impl SchemaDoc {
    fn inline(schema: serde_json::Value) -> Self {
        Self::Inline(schema)
    }

    fn component<T: JsonSchema>(name: &str) -> Self {
        Self::Component {
            name: name.to_string(),
            schema: schema_for::<T>(),
        }
    }

    fn from_component(name: &str) -> Self {
        Self::Ref(name.to_string())
    }
}

/// Route-local response documentation.
#[derive(Clone)]
struct ResponseDoc {
    description: &'static str,
    schema: SchemaDoc,
}

impl ResponseDoc {
    fn new(description: &'static str, schema: SchemaDoc) -> Self {
        Self {
            description,
            schema,
        }
    }
}

/// Route-local operation documentation.
#[derive(Clone)]
pub(super) struct OperationDoc {
    summary: &'static str,
    request_body: Option<SchemaDoc>,
    responses: BTreeMap<u16, ResponseDoc>,
}

impl OperationDoc {
    fn new(
        summary: &'static str,
        request_body: Option<SchemaDoc>,
        responses: impl IntoIterator<Item = (StatusCode, ResponseDoc)>,
    ) -> Self {
        Self {
            summary,
            request_body,
            // We normalize status keys once so rendered OpenAPI output stays stable.
            responses: responses
                .into_iter()
                .map(|(status, response)| (status.as_u16(), response))
                .collect(),
        }
    }
}

/// Per-method documentation, optional by design so route usage stays OpenAPI-agnostic.
#[derive(Clone, Default)]
struct MethodDoc {
    get: Option<OperationDoc>,
    post: Option<OperationDoc>,
}

#[derive(Clone)]
struct GetHandler<E: ExtendedExecutor<B::Storage>, B: StorageBackend> {
    callback: Arc<GetCallback<E, B>>,
    status: StatusCode,
    participant: Participant,
}

impl<E: ExtendedExecutor<B::Storage>, B: StorageBackend> GetHandler<E, B> {
    fn typed<O, F>(callback: F, participant: Participant) -> Self
    where
        O: Serialize,
        F: Fn(&Runtime<E, B>, &Context) -> Result<O, Error> + Send + Sync + 'static,
    {
        Self {
            callback: Arc::new(move |runtime, context| {
                callback(runtime, context).and_then(|value| serialize_json(&value))
            }),
            status: StatusCode::OK,
            participant,
        }
    }

    fn json<F>(status: StatusCode, callback: F, participant: Participant) -> Self
    where
        F: Fn(&Runtime<E, B>, &Context) -> Result<serde_json::Value, Error> + Send + Sync + 'static,
    {
        Self {
            callback: Arc::new(callback),
            status,
            participant,
        }
    }
}

#[derive(Clone)]
struct PostHandler<E: ExtendedExecutor<B::Storage>, B: StorageBackend> {
    callback: Arc<PostCallback<E, B>>,
    status: StatusCode,
    participant: Participant,
}

impl<E: ExtendedExecutor<B::Storage>, B: StorageBackend> PostHandler<E, B> {
    fn json<F>(status: StatusCode, callback: F, participant: Participant) -> Self
    where
        F: Fn(&Runtime<E, B>, serde_json::Value, &Context) -> Result<serde_json::Value, Error>
            + Send
            + Sync
            + 'static,
    {
        Self {
            callback: Arc::new(callback),
            status,
            participant,
        }
    }
}

#[derive(Clone)]
struct Method<E: ExtendedExecutor<B::Storage>, B: StorageBackend> {
    get: Option<GetHandler<E, B>>,
    post: Option<PostHandler<E, B>>,
    doc: MethodDoc,
}

impl<E: ExtendedExecutor<B::Storage>, B: StorageBackend> Default for Method<E, B> {
    fn default() -> Self {
        Self {
            get: None,
            post: None,
            doc: MethodDoc::default(),
        }
    }
}

impl<E: ExtendedExecutor<B::Storage>, B: StorageBackend> Method<E, B> {
    fn set_get_typed<O, F>(
        &mut self,
        callback: F,
        participant: Participant,
        doc: Option<OperationDoc>,
    ) where
        O: Serialize,
        F: Fn(&Runtime<E, B>, &Context) -> Result<O, Error> + Send + Sync + 'static,
    {
        self.get = Some(GetHandler::typed(callback, participant));
        self.doc.get = doc;
    }

    fn set_get_json<F>(
        &mut self,
        status: StatusCode,
        callback: F,
        participant: Participant,
        doc: Option<OperationDoc>,
    ) where
        F: Fn(&Runtime<E, B>, &Context) -> Result<serde_json::Value, Error> + Send + Sync + 'static,
    {
        self.get = Some(GetHandler::json(status, callback, participant));
        self.doc.get = doc;
    }

    fn set_post_json<F>(
        &mut self,
        status: StatusCode,
        callback: F,
        participant: Participant,
        doc: Option<OperationDoc>,
    ) where
        F: Fn(&Runtime<E, B>, serde_json::Value, &Context) -> Result<serde_json::Value, Error>
            + Send
            + Sync
            + 'static,
    {
        self.post = Some(PostHandler::json(status, callback, participant));
        self.doc.post = doc;
    }

    fn to_openapi_path_data(
        &self,
        components: &mut BTreeMap<String, serde_json::Value>,
    ) -> Option<OpenApiPathData> {
        let get = self
            .get
            .as_ref()
            .zip(self.doc.get.as_ref())
            .map(|(handler, operation)| {
                operation_to_openapi(operation, &handler.participant, components)
            });
        let post = self
            .post
            .as_ref()
            .zip(self.doc.post.as_ref())
            .map(|(handler, operation)| {
                operation_to_openapi(operation, &handler.participant, components)
            });

        if get.is_none() && post.is_none() {
            None
        } else {
            Some(OpenApiPathData { get, post })
        }
    }

    async fn execute<Bo: http_body::Body + Send + 'static>(
        &self,
        method: HttpMethod,
        body: Bo,
        runtime: &RwLock<Runtime<E, B>>,
        context: &Context,
    ) -> Result<http::Response<String>, Error>
    where
        Bo::Data: bytes::Buf + Send,
        Bo::Error: std::fmt::Display,
    {
        match method {
            HttpMethod::GET => {
                let handler = self
                    .get
                    .as_ref()
                    .ok_or_else(|| Error::method_not_allowed("method not allowed"))?;
                if !context.is_suitable_for(&handler.participant) {
                    return Err(Error::forbidden("not allowed for this participant"));
                }
                let value = {
                    let runtime = runtime.read();
                    (handler.callback)(&runtime, context)?
                };
                Ok(json_response(handler.status, &value))
            }
            HttpMethod::POST => {
                let handler = self
                    .post
                    .as_ref()
                    .ok_or_else(|| Error::method_not_allowed("method not allowed"))?;
                if !context.is_suitable_for(&handler.participant) {
                    return Err(Error::forbidden("not allowed for this participant"));
                }
                let payload = parse_json_body(body).await?;
                let value = {
                    let runtime = runtime.read();
                    (handler.callback)(&runtime, payload, context)?
                };
                Ok(json_response(handler.status, &value))
            }
            _ => Err(Error::method_not_allowed("method not allowed")),
        }
    }
}

/// Immutable route tree used by the HTTP API service.
#[derive(Clone)]
pub struct Route<E: ExtendedExecutor<B::Storage>, B: StorageBackend> {
    methods: Method<E, B>,
    // HashMap gives faster average lookup than tree maps in hot request paths.
    subpages: HashMap<String, Route<E, B>>,
    fallback: Option<Method<E, B>>,
}

impl<E: ExtendedExecutor<B::Storage>, B: StorageBackend> Route<E, B> {
    fn empty() -> Self {
        Self {
            methods: Method::default(),
            subpages: HashMap::new(),
            fallback: None,
        }
    }

    /// Walks the route tree and executes the matched method handler.
    pub async fn serve<Bo: http_body::Body + Send + 'static>(
        &self,
        path: &[&str],
        method: http::Method,
        body: Bo,
        runtime: &RwLock<Runtime<E, B>>,
        context: &Context,
    ) -> Result<http::Response<String>, Error>
    where
        Bo::Data: bytes::Buf + Send,
        Bo::Error: std::fmt::Display,
    {
        let mut current = self;
        for segment in path {
            if let Some(next) = current.subpages.get(*segment) {
                current = next;
                continue;
            }

            if let Some(fallback) = &current.fallback {
                return fallback.execute(method, body, runtime, context).await;
            }

            return Err(Error::not_found("route not found"));
        }

        current
            .methods
            .execute(method, body, runtime, context)
            .await
    }

    /// Derives a generic OpenAPI model from optional route docs.
    pub(super) fn openapi_data(&self) -> OpenApiRouteData {
        let mut data = OpenApiRouteData::default();
        register_shared_components::<E, B>(&mut data.components);
        self.collect_openapi_data("", &mut data);
        data
    }

    fn collect_openapi_data(&self, current_path: &str, data: &mut OpenApiRouteData) {
        if let Some(path_data) = self.methods.to_openapi_path_data(&mut data.components) {
            let normalized = if current_path.is_empty() {
                "/"
            } else {
                current_path
            };
            data.paths.insert(normalized.to_string(), path_data);
        }

        // For stable OpenAPI output we render children in lexical order, even with HashMap storage.
        let mut child_keys: Vec<_> = self.subpages.keys().collect();
        child_keys.sort_unstable();

        for segment in child_keys {
            let child = self
                .subpages
                .get(segment)
                .expect("key set and map must stay consistent while iterating");
            let child_path = if current_path.is_empty() {
                format!("/{segment}")
            } else {
                format!("{current_path}/{segment}")
            };
            child.collect_openapi_data(&child_path, data);
        }
    }
}

/// Mutable route registry for fast route insertion with optional documentation.
#[derive(Clone)]
pub(super) struct RouteBuilder<E: ExtendedExecutor<B::Storage>, B: StorageBackend> {
    root: Route<E, B>,
}

impl<E: ExtendedExecutor<B::Storage>, B: StorageBackend> RouteBuilder<E, B> {
    /// Creates an empty route builder.
    pub(super) fn new() -> Self {
        Self {
            root: Route::empty(),
        }
    }

    /// Registers a typed GET endpoint without requiring OpenAPI metadata.
    pub(super) fn add_get_typed<O, F>(&mut self, path: &str, callback: F)
    where
        O: Serialize,
        F: Fn(&Runtime<E, B>, &Context) -> Result<O, Error> + Send + Sync + 'static,
    {
        let node = self.route_mut(path);
        node.methods
            .set_get_typed(callback, Participant::Everyone, None);
    }

    /// Registers a JSON GET endpoint without requiring OpenAPI metadata.
    pub(super) fn add_get_json<F>(&mut self, path: &str, status: StatusCode, callback: F)
    where
        F: Fn(&Runtime<E, B>, &Context) -> Result<serde_json::Value, Error> + Send + Sync + 'static,
    {
        let node = self.route_mut(path);
        node.methods
            .set_get_json(status, callback, Participant::Everyone, None);
    }

    /// Registers a JSON POST endpoint without requiring OpenAPI metadata.
    pub(super) fn add_post_json<F>(&mut self, path: &str, status: StatusCode, callback: F)
    where
        F: Fn(&Runtime<E, B>, serde_json::Value, &Context) -> Result<serde_json::Value, Error>
            + Send
            + Sync
            + 'static,
    {
        let node = self.route_mut(path);
        node.methods
            .set_post_json(status, callback, Participant::Everyone, None);
    }

    /// Registers a typed GET endpoint and required participant.
    pub(super) fn add_get_typed_for<O, F>(
        &mut self,
        path: &str,
        participant: Participant,
        callback: F,
    ) where
        O: Serialize,
        F: Fn(&Runtime<E, B>, &Context) -> Result<O, Error> + Send + Sync + 'static,
    {
        let node = self.route_mut(path);
        node.methods.set_get_typed(callback, participant, None);
    }

    /// Registers a JSON GET endpoint and required participant.
    pub(super) fn add_get_json_for<F>(
        &mut self,
        path: &str,
        participant: Participant,
        status: StatusCode,
        callback: F,
    ) where
        F: Fn(&Runtime<E, B>, &Context) -> Result<serde_json::Value, Error> + Send + Sync + 'static,
    {
        let node = self.route_mut(path);
        node.methods
            .set_get_json(status, callback, participant, None);
    }

    /// Registers a JSON POST endpoint and required participant.
    pub(super) fn add_post_json_for<F>(
        &mut self,
        path: &str,
        participant: Participant,
        status: StatusCode,
        callback: F,
    ) where
        F: Fn(&Runtime<E, B>, serde_json::Value, &Context) -> Result<serde_json::Value, Error>
            + Send
            + Sync
            + 'static,
    {
        let node = self.route_mut(path);
        node.methods
            .set_post_json(status, callback, participant, None);
    }

    /// Registers a documented typed GET endpoint used for automatic OpenAPI generation.
    pub(super) fn add_get_typed_doc<O, F>(
        &mut self,
        path: &str,
        participant: Participant,
        callback: F,
        doc: OperationDoc,
    ) where
        O: Serialize,
        F: Fn(&Runtime<E, B>, &Context) -> Result<O, Error> + Send + Sync + 'static,
    {
        let node = self.route_mut(path);
        node.methods.set_get_typed(callback, participant, Some(doc));
    }

    /// Registers a documented JSON GET endpoint used for automatic OpenAPI generation.
    pub(super) fn add_get_json_doc<F>(
        &mut self,
        path: &str,
        participant: Participant,
        status: StatusCode,
        callback: F,
        doc: OperationDoc,
    ) where
        F: Fn(&Runtime<E, B>, &Context) -> Result<serde_json::Value, Error> + Send + Sync + 'static,
    {
        let node = self.route_mut(path);
        node.methods
            .set_get_json(status, callback, participant, Some(doc));
    }

    /// Registers a documented JSON POST endpoint used for automatic OpenAPI generation.
    pub(super) fn add_post_json_doc<F>(
        &mut self,
        path: &str,
        participant: Participant,
        status: StatusCode,
        callback: F,
        doc: OperationDoc,
    ) where
        F: Fn(&Runtime<E, B>, serde_json::Value, &Context) -> Result<serde_json::Value, Error>
            + Send
            + Sync
            + 'static,
    {
        let node = self.route_mut(path);
        node.methods
            .set_post_json(status, callback, participant, Some(doc));
    }

    /// Finalizes the route tree.
    pub(super) fn build(self) -> Route<E, B> {
        self.root
    }

    fn route_mut(&mut self, path: &str) -> &mut Route<E, B> {
        let segments = split_path(path);
        let mut current = &mut self.root;

        for segment in segments {
            current = current
                .subpages
                .entry(segment.to_string())
                .or_insert_with(Route::empty);
        }

        current
    }
}

/// Registers all built-in runtime endpoints in one place.
pub(super) fn register_runtime_routes<E: ExtendedExecutor<B::Storage>, B: StorageBackend>(
    routes: &mut RouteBuilder<E, B>,
    runtime: &Runtime<E, B>,
) {
    routes.add_get_typed_doc(
        "/processes",
        Participant::Everyone,
        |engine, _| {
            Ok(ProcessesResponse {
                processes: collect_process_summaries(engine),
            })
        },
        OperationDoc::new(
            "List registered processes",
            None,
            [(
                StatusCode::OK,
                ResponseDoc::new(
                    "Registered processes",
                    SchemaDoc::component::<ProcessesResponse>("ProcessesResponse"),
                ),
            )],
        ),
    );

    for process in runtime.registered_processes() {
        let process_name = ProcessName::from(&process.meta_data);
        let start_type = process.steps.start();
        register_process_instance_routes(
            routes,
            process_name,
            start_type.as_ref().expected_participant.clone(),
            start_type.as_ref().schema.as_value(),
        );
    }
}

/// Registers the generated OpenAPI endpoint at root.
pub(super) fn register_openapi_root<E: ExtendedExecutor<B::Storage>, B: StorageBackend>(
    routes: &mut RouteBuilder<E, B>,
    openapi_document: serde_json::Value,
) {
    routes.add_get_json_doc(
        "/",
        Participant::Everyone,
        StatusCode::OK,
        move |_, _| Ok(openapi_document.clone()),
        OperationDoc::new(
            "Get OpenAPI document",
            None,
            [(
                StatusCode::OK,
                ResponseDoc::new(
                    "OpenAPI document",
                    SchemaDoc::inline(schema_for::<serde_json::Value>()),
                ),
            )],
        ),
    );
}

fn register_process_instance_routes<E: ExtendedExecutor<B::Storage>, B: StorageBackend>(
    routes: &mut RouteBuilder<E, B>,
    process_name: ProcessName,
    participant: Participant,
    input_schema: &serde_json::Value,
) {
    let direct_path = format!("/processes/{process_name}");
    let instances_path = format!("/processes/{process_name}/instances");

    for path in [&direct_path, &instances_path] {
        let process_name_get = process_name.clone();
        routes.add_get_json_doc(
            path,
            participant.clone(),
            StatusCode::OK,
            move |runtime, _| {
                let instances = runtime
                    .get_instances(&process_name_get)
                    .ok_or_else(|| Error::not_found("unknown process"))?;
                serialize_json(instances)
            },
            OperationDoc::new(
                "List process instances",
                None,
                [
                    (
                        StatusCode::OK,
                        ResponseDoc::new(
                            "Process instances",
                            SchemaDoc::component::<Instances<E, B>>("Instances"),
                        ),
                    ),
                    (
                        StatusCode::NOT_FOUND,
                        ResponseDoc::new("Unknown process", SchemaDoc::component::<Error>("Error")),
                    ),
                ],
            ),
        );

        let process_name_post = process_name.clone();
        let schema = input_schema.clone();
        routes.add_post_json_doc(
            path,
            participant.clone(),
            StatusCode::ACCEPTED,
            move |runtime, input, context| {
                let instance_id = runtime.run_dynamic_with_context(
                    process_name_post.clone(),
                    input,
                    context.clone(),
                )?;
                serialize_json(&StartedInstanceResponse::new(instance_id))
            },
            OperationDoc::new(
                "Start process instance",
                // The process input schema is attached at route registration time.
                Some(SchemaDoc::inline(schema)),
                [
                    (
                        StatusCode::ACCEPTED,
                        ResponseDoc::new(
                            "Started instance",
                            SchemaDoc::component::<StartedInstanceResponse>(
                                "StartedInstanceResponse",
                            ),
                        ),
                    ),
                    (
                        StatusCode::BAD_REQUEST,
                        ResponseDoc::new("Invalid request", SchemaDoc::from_component("Error")),
                    ),
                    (
                        StatusCode::NOT_FOUND,
                        ResponseDoc::new("Unknown process", SchemaDoc::from_component("Error")),
                    ),
                ],
            ),
        );
    }
}

fn operation_to_openapi(
    operation: &OperationDoc,
    participant: &Participant,
    components: &mut BTreeMap<String, serde_json::Value>,
) -> OpenApiOperationData {
    OpenApiOperationData {
        summary: operation.summary,
        request_body: operation
            .request_body
            .as_ref()
            .map(|schema| materialize_schema(schema, components)),
        responses: operation
            .responses
            .iter()
            .map(|(status, response)| {
                let content = Some(materialize_schema(&response.schema, components));
                (
                    status.to_string(),
                    OpenApiResponseData {
                        description: response.description,
                        content,
                    },
                )
            })
            .collect(),
        is_public: participant == &Participant::Everyone,
    }
}

fn materialize_schema(
    schema: &SchemaDoc,
    components: &mut BTreeMap<String, serde_json::Value>,
) -> serde_json::Value {
    match schema {
        SchemaDoc::Inline(value) => value.clone(),
        SchemaDoc::Component { name, schema } => {
            // Repeated inserts are harmless and keep registration local to route declarations.
            components
                .entry(name.clone())
                .or_insert_with(|| schema.clone());
            schema_ref(name)
        }
        SchemaDoc::Ref(name) => schema_ref(name),
    }
}

fn schema_ref(schema_name: &str) -> serde_json::Value {
    serde_json::json!({ "$ref": format!("#/components/schemas/{schema_name}") })
}

fn collect_process_summaries<E: ExtendedExecutor<B::Storage>, B: StorageBackend>(
    runtime: &Runtime<E, B>,
) -> Vec<ProcessSummary> {
    let mut processes: Vec<_> = runtime
        .registered_processes()
        .map(ProcessSummary::from_registered)
        .collect();
    processes.sort_unstable_by(|left, right| left.name.to_string().cmp(&right.name.to_string()));
    processes
}

fn split_path(path: &str) -> Vec<&str> {
    path.trim_matches('/')
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect()
}

fn serialize_json<T: ?Sized + Serialize>(value: &T) -> Result<serde_json::Value, Error> {
    serde_json::to_value(value)
        .map_err(|err| Error::bad_request(format!("failed to serialize response: {err}")))
}

async fn parse_json_body<Bo: http_body::Body + Send + 'static>(
    body: Bo,
) -> Result<serde_json::Value, Error>
where
    Bo::Data: bytes::Buf + Send,
    Bo::Error: std::fmt::Display,
{
    let bytes = body
        .collect()
        .await
        .map_err(|err| Error::bad_request(format!("invalid request body: {err}")))?
        .to_bytes();

    serde_json::from_slice::<serde_json::Value>(&bytes)
        .map_err(|err| Error::bad_request(format!("invalid JSON body: {err}")))
}

fn schema_for<T: JsonSchema>() -> serde_json::Value {
    schemars::schema_for!(T).into()
}

/// Pre-registers all well-known API response schemas so they are always present in the
/// OpenAPI `components/schemas` section, including types only referenced via `$ref`.
fn register_shared_components<E: ExtendedExecutor<B::Storage>, B: StorageBackend>(
    components: &mut BTreeMap<String, serde_json::Value>,
) {
    components.insert("Error".to_string(), schema_for::<Error>());
    components.insert(
        "ProcessesResponse".to_string(),
        schema_for::<ProcessesResponse>(),
    );
    components.insert("ProcessSummary".to_string(), schema_for::<ProcessSummary>());
    components.insert(
        "StartedInstanceResponse".to_string(),
        schema_for::<StartedInstanceResponse>(),
    );
    components.insert("Instances".to_string(), schema_for::<Instances<E, B>>());
    components.insert("Instance".to_string(), schema_for::<Instance<E, B>>());
}

/// Response body for the process collection endpoint.
#[derive(Debug, Clone, Serialize, JsonSchema)]
pub(super) struct ProcessesResponse {
    pub(super) processes: Vec<ProcessSummary>,
}

/// Summary representation for one registered process in API responses.
#[derive(Debug, Clone, Serialize, JsonSchema)]
pub(super) struct ProcessSummary {
    pub(super) name: ProcessName,
    pub(super) metadata: MetaData,
    pub(super) steps: Vec<Step>,
    pub(super) input_schema: serde_json::Value,
}

impl ProcessSummary {
    fn from_registered<B: StorageBackend>(process: &RegisteredProcess<B>) -> Self {
        Self {
            name: ProcessName::from(&process.meta_data),
            metadata: process.meta_data.clone(),
            // Keeping values from canonical storage preserves stable step order.
            steps: process
                .steps
                .steps()
                .filter_map(|step| process.steps.get(step))
                .collect(),
            input_schema: process.steps.start().as_ref().schema.as_value().clone(),
        }
    }
}

/// Accepted response body for a newly started process instance.
#[derive(Debug, Clone, Serialize, JsonSchema)]
pub(super) struct StartedInstanceResponse {
    pub(super) id: InstanceId,
    pub(super) status: String,
}

impl StartedInstanceResponse {
    fn new(id: InstanceId) -> Self {
        Self {
            id,
            status: "running".to_string(),
        }
    }
}
