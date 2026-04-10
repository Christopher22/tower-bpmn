use std::{collections::BTreeMap, sync::Arc};

use http::{Method as HttpMethod, StatusCode};
use http_body_util::BodyExt;
use parking_lot::RwLock;
use schemars::JsonSchema;
use serde::Serialize;

use super::{error::Error, json_response, openapi};
use crate::{
    ExtendedExecutor, InstanceId, MetaData, ProcessName, Step, StorageBackend, bpmn::Runtime,
};

type GetCallback<E, B, C> = fn(&Runtime<E, B>, &C) -> Result<serde_json::Value, Error>;
type PostCallback<E, B, C> =
    fn(&Runtime<E, B>, &C, serde_json::Value) -> Result<serde_json::Value, Error>;
type GetCallbackOwned<E, B> =
    dyn Fn(&Runtime<E, B>) -> Result<serde_json::Value, Error> + Send + Sync;

/// Immutable route tree used by the HTTP API service.
pub struct Route<E: ExtendedExecutor<B::Storage>, B: StorageBackend> {
    methods: Method<E, B>,
    subpages: BTreeMap<String, Route<E, B>>,
    fallback: Option<Method<E, B>>,
}

impl<E: ExtendedExecutor<B::Storage>, B: StorageBackend> Route<E, B> {
    /// Builds the static route tree for the currently registered processes.
    pub fn from_engine(engine: &Runtime<E, B>) -> Self {
        let process_routes: BTreeMap<_, _> = engine
            .registered_processes()
            .map(|process| {
                let process_name = ProcessName::from(&process.meta_data);
                let path_segment = process_name.to_string();
                let instances_method = process_instances_method(process_name);

                (
                    path_segment,
                    Self::branch(
                        instances_method.clone(),
                        [("instances".to_string(), Self::leaf(instances_method))],
                    ),
                )
            })
            .collect();

        let processes = Self::branch(
            Method::get(
                |runtime, _| {
                    Ok(openapi::ProcessesResponse {
                        processes: collect_process_summaries(runtime),
                    })
                },
                (),
            ),
            process_routes,
        );

        let openapi = openapi::build(engine);

        Self::branch(
            Method::get_json(StatusCode::OK, |_, document| Ok(document.clone()), openapi),
            [("processes".to_string(), processes)],
        )
    }

    fn leaf(methods: Method<E, B>) -> Self {
        Self {
            methods,
            subpages: BTreeMap::new(),
            fallback: None,
        }
    }

    fn branch(
        methods: Method<E, B>,
        subpages: impl IntoIterator<Item = (String, Route<E, B>)>,
    ) -> Self {
        Self {
            methods,
            subpages: subpages.into_iter().collect(),
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
                return fallback.execute(method, body, runtime).await;
            }

            return Err(Error::not_found("route not found"));
        }

        current.methods.execute(method, body, runtime).await
    }
}

#[derive(Clone)]
struct GetHandler<E: ExtendedExecutor<B::Storage>, B: StorageBackend> {
    callback: Arc<GetCallbackOwned<E, B>>,
    status: StatusCode,
}

impl<E: ExtendedExecutor<B::Storage>, B: StorageBackend> GetHandler<E, B> {
    pub fn new<O: 'static + Serialize + JsonSchema, C: 'static + Send + Sync>(
        callback: fn(&Runtime<E, B>, &C) -> Result<O, Error>,
        context: C,
    ) -> Self {
        Self {
            callback: Arc::new(move |engine| {
                callback(engine, &context).and_then(|value| serialize_json(&value))
            }),
            status: StatusCode::OK,
        }
    }

    pub fn new_json<C: 'static + Send + Sync>(
        status: StatusCode,
        callback: fn(&Runtime<E, B>, &C) -> Result<serde_json::Value, Error>,
        context: C,
    ) -> Self {
        Self {
            callback: Arc::new(move |engine| callback(engine, &context)),
            status,
        }
    }
}

type PostCallbackOwned<E, B> =
    dyn Fn(&Runtime<E, B>, serde_json::Value) -> Result<serde_json::Value, Error> + Send + Sync;

#[derive(Clone)]
struct PostHandler<E: ExtendedExecutor<B::Storage>, B: StorageBackend> {
    callback: Arc<PostCallbackOwned<E, B>>,
    status: StatusCode,
}

impl<E: ExtendedExecutor<B::Storage>, B: StorageBackend> PostHandler<E, B> {
    pub fn new_json<C: 'static + Send + Sync>(
        status: StatusCode,
        callback: PostCallback<E, B, C>,
        context: C,
    ) -> Self {
        Self {
            callback: Arc::new(move |engine, input| callback(engine, &context, input)),
            status,
        }
    }
}

#[derive(Clone)]
pub struct Method<E: ExtendedExecutor<B::Storage>, B: StorageBackend> {
    get: Option<GetHandler<E, B>>,
    post: Option<PostHandler<E, B>>,
}

impl<E: ExtendedExecutor<B::Storage>, B: StorageBackend> Method<E, B> {
    /// Creates a GET-only method whose response is serialized from a typed value.
    pub fn get<C: 'static + Send + Sync, O: 'static + Serialize + JsonSchema>(
        callback: fn(&Runtime<E, B>, &C) -> Result<O, Error>,
        context: C,
    ) -> Self {
        Self {
            get: Some(GetHandler::new(callback, context)),
            post: None,
        }
    }

    /// Creates a GET-only method whose callback already returns raw JSON.
    pub fn get_json<C: 'static + Send + Sync>(
        status: StatusCode,
        callback: GetCallback<E, B, C>,
        context: C,
    ) -> Self {
        Self {
            get: Some(GetHandler::new_json(status, callback, context)),
            post: None,
        }
    }

    /// Adds a POST variant whose callback already returns raw JSON.
    pub fn with_post_json<C: 'static + Send + Sync>(
        self,
        status: StatusCode,
        callback: PostCallback<E, B, C>,
        context: C,
    ) -> Self {
        Self {
            post: Some(PostHandler::new_json(status, callback, context)),
            ..self
        }
    }

    async fn execute<Bo: http_body::Body + Send + 'static>(
        &self,
        method: HttpMethod,
        body: Bo,
        runtime: &RwLock<Runtime<E, B>>,
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
                let value = {
                    let runtime = runtime.read();
                    (handler.callback)(&runtime)?
                };
                Ok(json_response(handler.status, &value))
            }
            HttpMethod::POST => {
                let handler = self
                    .post
                    .as_ref()
                    .ok_or_else(|| Error::method_not_allowed("method not allowed"))?;
                let payload = parse_json_body(body).await?;
                let value = {
                    let runtime = runtime.read();
                    (handler.callback)(&runtime, payload)?
                };
                Ok(json_response(handler.status, &value))
            }
            _ => Err(Error::method_not_allowed("method not allowed")),
        }
    }
}

fn collect_process_summaries<E: ExtendedExecutor<B::Storage>, B: StorageBackend>(
    runtime: &Runtime<E, B>,
) -> Vec<ProcessSummary> {
    let mut processes: Vec<_> = runtime
        .registered_processes()
        .map(ProcessSummary::from_registered)
        .collect();
    processes.sort_by(|left, right| left.name.to_string().cmp(&right.name.to_string()));
    processes
}

fn process_instances_method<E: ExtendedExecutor<B::Storage>, B: StorageBackend>(
    process_name: ProcessName,
) -> Method<E, B> {
    Method::get_json(
        StatusCode::OK,
        |runtime, process_name| {
            let instances = runtime
                .get_instances(process_name)
                .ok_or_else(|| Error::not_found("unknown process"))?;
            serialize_json(instances)
        },
        process_name.clone(),
    )
    .with_post_json(
        StatusCode::ACCEPTED,
        |runtime, process_name, input| {
            let instance_id = runtime.run_dynamic(process_name.clone(), input)?;
            serialize_json(&StartedInstanceResponse::new(instance_id))
        },
        process_name,
    )
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

#[derive(Debug, Clone, Serialize, JsonSchema)]
pub(super) struct ProcessSummary {
    pub(super) name: ProcessName,
    pub(super) metadata: MetaData,
    pub(super) steps: Vec<Step>,
    pub(super) input_schema: serde_json::Value,
}

impl ProcessSummary {
    /// Builds the API summary representation for one registered process.
    fn from_registered<B: StorageBackend>(process: &crate::RegisteredProcess<B>) -> Self {
        Self {
            name: ProcessName::from(&process.meta_data),
            metadata: process.meta_data.clone(),
            steps: process
                .steps
                .steps()
                .filter_map(|step| process.steps.get(step))
                .collect(),
            input_schema: process.input.json_schema.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
pub(super) struct StartedInstanceResponse {
    pub(super) id: InstanceId,
    pub(super) status: String,
}

impl StartedInstanceResponse {
    /// Creates the accepted-response body for a newly started instance.
    fn new(id: InstanceId) -> Self {
        Self {
            id,
            status: "running".to_string(),
        }
    }
}
