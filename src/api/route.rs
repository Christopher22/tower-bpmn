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
    openapi::{
        OpenApiOperationData, OpenApiParameterData, OpenApiPathData, OpenApiResponseData,
        OpenApiRouteData,
    },
    xml_response,
};
use crate::bpmn::{
    ExtendedExecutor, Instance, InstanceId, Instances, MetaData, ProcessName, RegisteredProcess,
    Runtime, Step,
    messages::{Context, CorrelationKey, Message, MessageError, Participant},
    storage::StorageBackend,
};

type GetCallback<E, B> =
    dyn Fn(&Runtime<E, B>, &Context) -> Result<serde_json::Value, Error> + Send + Sync;
type GetPathCallback<E, B> =
    dyn Fn(&Runtime<E, B>, &str, &Context) -> Result<serde_json::Value, Error> + Send + Sync;
type GetXmlCallback<E, B> = dyn Fn(&Runtime<E, B>, &Context) -> Result<String, Error> + Send + Sync;
type PostCallback<E, B> = dyn Fn(&Runtime<E, B>, serde_json::Value, &Context) -> Result<serde_json::Value, Error>
    + Send
    + Sync;
type PostPathCallback<E, B> = dyn Fn(&Runtime<E, B>, &str, serde_json::Value, &Context) -> Result<serde_json::Value, Error>
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

    fn component_with_schema(name: &str, schema: serde_json::Value) -> Self {
        Self::Component {
            name: name.to_string(),
            schema,
        }
    }
}

/// Route-local response documentation.
#[derive(Clone)]
struct ResponseDoc {
    description: &'static str,
    schema: SchemaDoc,
    content_type: &'static str,
}

impl ResponseDoc {
    fn new(description: &'static str, schema: SchemaDoc) -> Self {
        Self {
            description,
            schema,
            content_type: "application/json",
        }
    }

    fn xml(description: &'static str, schema: SchemaDoc) -> Self {
        Self {
            description,
            schema,
            content_type: "text/xml",
        }
    }
}

/// Route-local operation parameter documentation.
#[derive(Clone)]
struct ParameterDoc {
    name: &'static str,
    location: &'static str,
    description: &'static str,
    required: bool,
    schema: SchemaDoc,
}

impl ParameterDoc {
    fn path(name: &'static str, description: &'static str, schema: SchemaDoc) -> Self {
        Self {
            name,
            location: "path",
            description,
            required: true,
            schema,
        }
    }
}

/// Route-local operation documentation.
#[derive(Clone)]
pub(super) struct OperationDoc {
    summary: &'static str,
    parameters: Vec<ParameterDoc>,
    request_body: Option<SchemaDoc>,
    responses: BTreeMap<u16, ResponseDoc>,
}

impl OperationDoc {
    fn new(
        summary: &'static str,
        parameters: impl IntoIterator<Item = ParameterDoc>,
        request_body: Option<SchemaDoc>,
        responses: impl IntoIterator<Item = (StatusCode, ResponseDoc)>,
    ) -> Self {
        Self {
            summary,
            parameters: parameters.into_iter().collect(),
            request_body,
            // We normalize status keys once so rendered OpenAPI output stays stable.
            responses: responses
                .into_iter()
                .map(|(status, response)| (status.as_u16(), response))
                .collect(),
        }
    }

    fn path_parameter_name(&self) -> Option<&'static str> {
        self.parameters
            .iter()
            .find(|parameter| parameter.location == "path")
            .map(|parameter| parameter.name)
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
    callback: GetHandlerCallback<E, B>,
    status: StatusCode,
    participant: Participant,
}

#[derive(Clone)]
enum GetHandlerCallback<E: ExtendedExecutor<B::Storage>, B: StorageBackend> {
    Static(Arc<GetCallback<E, B>>),
    PathSegment(Arc<GetPathCallback<E, B>>),
    Xml(Arc<GetXmlCallback<E, B>>),
}

impl<E: ExtendedExecutor<B::Storage>, B: StorageBackend> GetHandler<E, B> {
    fn typed<O, F>(callback: F, participant: Participant) -> Self
    where
        O: Serialize,
        F: Fn(&Runtime<E, B>, &Context) -> Result<O, Error> + Send + Sync + 'static,
    {
        Self {
            callback: GetHandlerCallback::Static(Arc::new(move |runtime, context| {
                callback(runtime, context).and_then(|value| serialize_json(&value))
            })),
            status: StatusCode::OK,
            participant,
        }
    }

    fn json<F>(status: StatusCode, callback: F, participant: Participant) -> Self
    where
        F: Fn(&Runtime<E, B>, &Context) -> Result<serde_json::Value, Error> + Send + Sync + 'static,
    {
        Self {
            callback: GetHandlerCallback::Static(Arc::new(callback)),
            status,
            participant,
        }
    }

    fn json_with_path_segment<F>(status: StatusCode, callback: F, participant: Participant) -> Self
    where
        F: Fn(&Runtime<E, B>, &str, &Context) -> Result<serde_json::Value, Error>
            + Send
            + Sync
            + 'static,
    {
        Self {
            callback: GetHandlerCallback::PathSegment(Arc::new(callback)),
            status,
            participant,
        }
    }

    fn xml<F>(callback: F, participant: Participant) -> Self
    where
        F: Fn(&Runtime<E, B>, &Context) -> Result<String, Error> + Send + Sync + 'static,
    {
        Self {
            callback: GetHandlerCallback::Xml(Arc::new(callback)),
            status: StatusCode::OK,
            participant,
        }
    }
}

#[derive(Clone)]
struct PostHandler<E: ExtendedExecutor<B::Storage>, B: StorageBackend> {
    callback: PostHandlerCallback<E, B>,
    status: StatusCode,
    participant: Participant,
}

#[derive(Clone)]
enum PostHandlerCallback<E: ExtendedExecutor<B::Storage>, B: StorageBackend> {
    Static(Arc<PostCallback<E, B>>),
    PathSegment(Arc<PostPathCallback<E, B>>),
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
            callback: PostHandlerCallback::Static(Arc::new(callback)),
            status,
            participant,
        }
    }

    fn json_with_path_segment<F>(status: StatusCode, callback: F, participant: Participant) -> Self
    where
        F: Fn(
                &Runtime<E, B>,
                &str,
                serde_json::Value,
                &Context,
            ) -> Result<serde_json::Value, Error>
            + Send
            + Sync
            + 'static,
    {
        Self {
            callback: PostHandlerCallback::PathSegment(Arc::new(callback)),
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

    fn set_get_json_with_path_segment<F>(
        &mut self,
        status: StatusCode,
        callback: F,
        participant: Participant,
        doc: Option<OperationDoc>,
    ) where
        F: Fn(&Runtime<E, B>, &str, &Context) -> Result<serde_json::Value, Error>
            + Send
            + Sync
            + 'static,
    {
        self.get = Some(GetHandler::json_with_path_segment(
            status,
            callback,
            participant,
        ));
        self.doc.get = doc;
    }

    fn set_get_xml<F>(&mut self, callback: F, participant: Participant, doc: Option<OperationDoc>)
    where
        F: Fn(&Runtime<E, B>, &Context) -> Result<String, Error> + Send + Sync + 'static,
    {
        self.get = Some(GetHandler::xml(callback, participant));
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

    fn set_post_json_with_path_segment<F>(
        &mut self,
        status: StatusCode,
        callback: F,
        participant: Participant,
        doc: Option<OperationDoc>,
    ) where
        F: Fn(
                &Runtime<E, B>,
                &str,
                serde_json::Value,
                &Context,
            ) -> Result<serde_json::Value, Error>
            + Send
            + Sync
            + 'static,
    {
        self.post = Some(PostHandler::json_with_path_segment(
            status,
            callback,
            participant,
        ));
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

    fn openapi_path_parameter_name(&self) -> Option<&'static str> {
        let get_parameter = self
            .doc
            .get
            .as_ref()
            .and_then(OperationDoc::path_parameter_name);
        let post_parameter = self
            .doc
            .post
            .as_ref()
            .and_then(OperationDoc::path_parameter_name);

        match (get_parameter, post_parameter) {
            (Some(left), Some(right)) if left != right => {
                panic!("fallback route methods must agree on path parameter name")
            }
            (Some(name), _) | (_, Some(name)) => Some(name),
            (None, None) => None,
        }
    }

    async fn execute<Bo: http_body::Body + Send + 'static>(
        &self,
        method: HttpMethod,
        body: Bo,
        runtime: &RwLock<Runtime<E, B>>,
        context: &Context,
        path_segment: Option<&str>,
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
                    match &handler.callback {
                        GetHandlerCallback::Static(callback) => callback(&runtime, context)?,
                        GetHandlerCallback::PathSegment(callback) => {
                            let segment =
                                path_segment.ok_or_else(|| Error::not_found("route not found"))?;
                            callback(&runtime, segment, context)?
                        }
                        GetHandlerCallback::Xml(callback) => {
                            let xml = callback(&runtime, context)?;
                            return Ok(xml_response(handler.status, xml));
                        }
                    }
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
                    match &handler.callback {
                        PostHandlerCallback::Static(callback) => {
                            callback(&runtime, payload, context)?
                        }
                        PostHandlerCallback::PathSegment(callback) => {
                            let segment =
                                path_segment.ok_or_else(|| Error::not_found("route not found"))?;
                            callback(&runtime, segment, payload, context)?
                        }
                    }
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
        for (index, segment) in path.iter().enumerate() {
            if let Some(next) = current.subpages.get(*segment) {
                current = next;
                continue;
            }

            if index + 1 == path.len() {
                if let Some(fallback) = &current.fallback {
                    return fallback
                        .execute(method, body, runtime, context, Some(segment))
                        .await;
                }
            }

            return Err(Error::not_found("route not found"));
        }

        current
            .methods
            .execute(method, body, runtime, context, None)
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

        if let Some(fallback) = self.fallback.as_ref() {
            if let Some(path_data) = fallback.to_openapi_path_data(&mut data.components) {
                let path_parameter_name = fallback
                    .openapi_path_parameter_name()
                    .expect("documented fallback routes must define a path parameter");
                let normalized = if current_path.is_empty() {
                    format!("/{{{path_parameter_name}}}")
                } else {
                    format!("{current_path}/{{{path_parameter_name}}}")
                };
                data.paths.insert(normalized, path_data);
            }
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

    /// Registers a documented JSON GET endpoint using one dynamic final path segment.
    pub(super) fn add_get_json_doc_with_path_segment<F>(
        &mut self,
        path: &str,
        participant: Participant,
        status: StatusCode,
        callback: F,
        doc: OperationDoc,
    ) where
        F: Fn(&Runtime<E, B>, &str, &Context) -> Result<serde_json::Value, Error>
            + Send
            + Sync
            + 'static,
    {
        let node = self.route_mut(path);
        let fallback = node.fallback.get_or_insert_with(Method::default);
        fallback.set_get_json_with_path_segment(status, callback, participant, Some(doc));
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

    /// Registers a documented JSON POST endpoint using one dynamic final path segment.
    pub(super) fn add_post_json_doc_with_path_segment<F>(
        &mut self,
        path: &str,
        participant: Participant,
        status: StatusCode,
        callback: F,
        doc: OperationDoc,
    ) where
        F: Fn(
                &Runtime<E, B>,
                &str,
                serde_json::Value,
                &Context,
            ) -> Result<serde_json::Value, Error>
            + Send
            + Sync
            + 'static,
    {
        let node = self.route_mut(path);
        let fallback = node.fallback.get_or_insert_with(Method::default);
        fallback.set_post_json_with_path_segment(status, callback, participant, Some(doc));
    }

    /// Registers a documented XML GET endpoint used for automatic OpenAPI generation.
    pub(super) fn add_get_xml_doc<F>(
        &mut self,
        path: &str,
        participant: Participant,
        callback: F,
        doc: OperationDoc,
    ) where
        F: Fn(&Runtime<E, B>, &Context) -> Result<String, Error> + Send + Sync + 'static,
    {
        let node = self.route_mut(path);
        node.methods.set_get_xml(callback, participant, Some(doc));
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
    for process in runtime.registered_processes() {
        let process_name = ProcessName::from(&process.meta_data);
        let start_type = process.steps.start();
        let input_schema = start_type.as_ref().input.schema.as_value();
        let participant = start_type.as_ref().expected_participant.clone();

        // Register process metadata endpoint: GET /{process}/
        let process_name_meta = process_name.clone();
        routes.add_get_json_doc(
            &format!("/{process_name}"),
            participant.clone(),
            StatusCode::OK,
            move |runtime, _| {
                let registered = runtime
                    .get_registered_process(&process_name_meta)
                    .ok_or_else(|| Error::not_found("unknown process"))?;
                serialize_json(&ProcessSummary::from_registered(registered))
            },
            OperationDoc::new(
                "Get process metadata",
                [],
                None,
                [
                    (
                        StatusCode::OK,
                        ResponseDoc::new(
                            "Process metadata",
                            SchemaDoc::component::<ProcessSummary>("ProcessSummary"),
                        ),
                    ),
                    (
                        StatusCode::NOT_FOUND,
                        ResponseDoc::new("Unknown process", SchemaDoc::component::<Error>("Error")),
                    ),
                ],
            ),
        );

        // Register BPMN endpoint: GET /{process}/bpmn
        let process_name_bpmn = process_name.clone();
        routes.add_get_xml_doc(
            &format!("/{process_name}/bpmn"),
            Participant::Everyone,
            move |runtime, _| {
                let registered = runtime
                    .get_registered_process(&process_name_bpmn)
                    .ok_or_else(|| Error::not_found("unknown process"))?;
                Ok(registered.bpmn())
            },
            OperationDoc::new(
                "Get BPMN 2.0 process definition as XML",
                [],
                None,
                [(
                    StatusCode::OK,
                    ResponseDoc::xml(
                        "BPMN 2.0 XML",
                        SchemaDoc::inline(serde_json::json!({"type": "string"})),
                    ),
                )],
            ),
        );

        // Register instances endpoints: GET/POST /{process}/instances/
        register_process_instance_routes(
            routes,
            process_name.clone(),
            participant.clone(),
            input_schema,
        );

        // Register external step message routes
        for external_step in process.steps.external_steps() {
            let Some(step) = process.steps.get(external_step.name.as_str()) else {
                continue;
            };
            register_external_step_message_route(
                routes,
                process_name.clone(),
                step,
                external_step.expected_participant.clone(),
                external_step.input.schema.as_value(),
            );
        }
    }
}

/// Registers read-only step history endpoints for explicitly exposed processes.
pub(super) fn register_exposed_step_query_routes<
    E: ExtendedExecutor<B::Storage>,
    B: StorageBackend,
>(
    routes: &mut RouteBuilder<E, B>,
    runtime: &Runtime<E, B>,
    exposed_processes: &HashMap<MetaData, Participant>,
) {
    for process in runtime.registered_processes() {
        let Some(participant) = exposed_processes.get(&process.meta_data) else {
            continue;
        };

        let process_name = ProcessName::from(&process.meta_data);

        for step_name in process.steps.steps() {
            let Some(step) = process.steps.get(step_name) else {
                continue;
            };

            let response_schema_name = format!(
                "FinishedSteps_{}_{}",
                process_name,
                step.as_str().replace('-', "_")
            );
            let output_schema = step.output().schema.as_value().clone();
            let response_schema = finished_steps_schema_for_output(&output_schema);

            let history_path = format!("/{process_name}/step/{step_name}");
            let step_for_query = step.clone();
            let process_name_for_query = process_name.clone();
            routes.add_get_json_doc_with_path_segment(
                &history_path,
                participant.clone(),
                StatusCode::OK,
                move |runtime, instance_id, _| {
                    let instance_id: InstanceId = instance_id.parse()?;
                    let registered = runtime
                        .get_registered_process(&process_name_for_query)
                        .ok_or_else(|| Error::not_found("unknown process"))?;
                    let rows = runtime.storage_backend.query(
                        registered,
                        step_for_query.clone(),
                        instance_id,
                    )?;
                    let response_rows = rows
                        .into_iter()
                        .map(FinishedStepResponse::from_storage)
                        .collect::<Vec<_>>();
                    serialize_json(&response_rows)
                },
                OperationDoc::new(
                    "List finished step entries for one process instance",
                    [ParameterDoc::path(
                        "id",
                        "Process instance id",
                        SchemaDoc::component::<InstanceId>("InstanceId"),
                    )],
                    None,
                    [
                        (
                            StatusCode::OK,
                            ResponseDoc::new(
                                "Step execution history",
                                SchemaDoc::component_with_schema(
                                    &response_schema_name,
                                    response_schema,
                                ),
                            ),
                        ),
                        (
                            StatusCode::BAD_REQUEST,
                            ResponseDoc::new("Invalid request", SchemaDoc::from_component("Error")),
                        ),
                        (
                            StatusCode::NOT_FOUND,
                            ResponseDoc::new(
                                "Unknown instance",
                                SchemaDoc::from_component("Error"),
                            ),
                        ),
                    ],
                ),
            );
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct FinishedStepResponse {
    timestamp: chrono::DateTime<chrono::Utc>,
    responsible: crate::bpmn::messages::Entity,
    output_value: serde_json::Value,
}

impl FinishedStepResponse {
    fn from_storage(step: crate::bpmn::storage::FinishedStep) -> Self {
        Self {
            timestamp: step.timestamp,
            responsible: step.responsible,
            output_value: step.output,
        }
    }
}

fn finished_steps_schema_for_output(output_schema: &serde_json::Value) -> serde_json::Value {
    schemars::json_schema!({
        "type": "array",
        "items": {
            "type": "object",
            "properties": {
                "timestamp": {
                    "type": "string",
                    "format": "date-time",
                    "description": "The datetime when the step was finished."
                },
                "responsible": schema_for::<crate::bpmn::messages::Entity>(),
                "output_value": output_schema.clone()
            },
            "required": ["timestamp", "responsible", "output_value"],
            "additionalProperties": false
        }
    })
    .into()
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
            [],
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
    let instances_path = format!("/{process_name}/instances");

    // GET /{process}/instances/ - List instances
    let process_name_get = process_name.clone();
    routes.add_get_json_doc(
        &instances_path,
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
            [],
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

    // POST /{process}/instances/ - Create instance
    let process_name_post = process_name.clone();
    let schema = input_schema.clone();
    routes.add_post_json_doc(
        &instances_path,
        participant,
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
            [],
            Some(SchemaDoc::inline(schema)),
            [
                (
                    StatusCode::ACCEPTED,
                    ResponseDoc::new(
                        "Started instance",
                        SchemaDoc::component::<StartedInstanceResponse>("StartedInstanceResponse"),
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

fn register_external_step_message_route<E: ExtendedExecutor<B::Storage>, B: StorageBackend>(
    routes: &mut RouteBuilder<E, B>,
    process_name: ProcessName,
    step: Step,
    participant: Participant,
    input_schema: &serde_json::Value,
) {
    let step_name = step.as_str().to_string();
    let base_path = format!("/{process_name}/step/{step_name}");
    let schema = input_schema.clone();

    routes.add_post_json_doc_with_path_segment(
        &base_path,
        participant,
        StatusCode::ACCEPTED,
        move |runtime, id, payload, context| {
            let correlation_key: CorrelationKey = id.parse()?;
            let mut delivered = false;

            for target_process in std::iter::once(process_name.clone()).chain(
                runtime
                    .registered_processes()
                    .map(|process| ProcessName::from(&process.meta_data)),
            ) {
                let mut message = Message::for_waiting_step(
                    target_process,
                    step.clone(),
                    payload.clone(),
                    correlation_key,
                );
                message.context = context.clone();

                match runtime.messages.send(message) {
                    Ok(Ok(())) => {
                        delivered = true;
                        break;
                    }
                    Ok(Err(MessageError::NoTarget)) | Err(MessageError::NoTarget) => {}
                    Ok(Err(error)) => return Err(error.into()),
                    Err(error) => return Err(error.into()),
                }
            }

            if !delivered {
                return Err(Error::conflict("no target for this message"));
            }

            Ok(serde_json::json!({ "status": "accepted" }))
        },
        OperationDoc::new(
            "Send message to waiting step",
            [ParameterDoc::path(
                "id",
                "Correlation key used to match a waiting process instance",
                SchemaDoc::component::<CorrelationKey>("CorrelationKey"),
            )],
            Some(SchemaDoc::inline(schema)),
            [
                (
                    StatusCode::ACCEPTED,
                    ResponseDoc::new(
                        "Message accepted",
                        SchemaDoc::inline(
                            schemars::json_schema!({
                                "type": "object",
                                "properties": {
                                    "status": { "type": "string" }
                                },
                                "required": ["status"]
                            })
                            .into(),
                        ),
                    ),
                ),
                (
                    StatusCode::BAD_REQUEST,
                    ResponseDoc::new("Invalid request", SchemaDoc::from_component("Error")),
                ),
                (
                    StatusCode::FORBIDDEN,
                    ResponseDoc::new("Forbidden", SchemaDoc::from_component("Error")),
                ),
                (
                    StatusCode::CONFLICT,
                    ResponseDoc::new("No waiting target", SchemaDoc::from_component("Error")),
                ),
            ],
        ),
    );
}

fn operation_to_openapi(
    operation: &OperationDoc,
    participant: &Participant,
    components: &mut BTreeMap<String, serde_json::Value>,
) -> OpenApiOperationData {
    OpenApiOperationData {
        summary: operation.summary,
        parameters: operation
            .parameters
            .iter()
            .map(|parameter| OpenApiParameterData {
                name: parameter.name,
                location: parameter.location,
                description: parameter.description,
                required: parameter.required,
                schema: materialize_schema(&parameter.schema, components),
            })
            .collect(),
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
                        content_type: response.content_type,
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
    components.insert("ProcessSummary".to_string(), schema_for::<ProcessSummary>());
    components.insert(
        "StartedInstanceResponse".to_string(),
        schema_for::<StartedInstanceResponse>(),
    );
    components.insert("Instances".to_string(), schema_for::<Instances<E, B>>());
    components.insert("Instance".to_string(), schema_for::<Instance<E, B>>());
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
            input_schema: process
                .steps
                .start()
                .as_ref()
                .input
                .schema
                .as_value()
                .clone(),
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
