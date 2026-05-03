# tower-bpmn

A BPMN runtime for custom backends built on Tower, enabling definition and execution of process workflows in Rust with full async/await support.

## Overview

`tower-bpmn` provides a high-performance, type-safe BPMN 2.0 runtime implementation for Rust. Define complex business processes as strongly-typed code, execute them with pluggable executors and storage backends, and expose them via a REST API with automatic OpenAPI documentation.

## Usage

Add `tower-bpmn` to your `Cargo.toml` and combine it with a `Tower`-compatible HTTP(S) server of your choice:

```toml
[dependencies]
tower-bpmn = { git = "https://github.com/Christopher22/tower-bpmn.git" }
axum = "0.8"
tokio = { version = "1.49", features = ["full"] }
```

## Quick Start

### 1. Define a Simple Process

```rust
use tower_bpmn::bpmn::{
    Process, ProcessBuilder, Step, MetaData, Storage, InMemory, Runtime,
};

#[derive(Clone, Copy)]
struct OrderProcess;

impl Process for OrderProcess {
    type Input = i32;
    type Output = String;

    fn metadata(&self) -> &MetaData {
        &MetaData::new("order-process", "Process order payments")
    }

    fn define<S: Storage>(
        &self,
        process: ProcessBuilder<Self, Self::Input, S>,
    ) -> ProcessBuilder<Self, Self::Output, S> {
        process
            .then("validate", |_token, amount| amount > 0)
            .then("charge", |_token, amount| {
                format!("Charged ${}", amount)
            })
    }
}
```

### 2. Execute the Process ...

```rust
#[tokio::main]
async fn main() {
    let mut runtime: Runtime<tower_bpmn::executor::TokioExecutor, InMemory> =
        Runtime::default();

    runtime.register_process(OrderProcess).expect("registration failed");

    let instance = runtime.run(OrderProcess, 100).expect("execution failed");
    let result = instance.wait_complete().await;

    println!("Order result: {:?}", result);
}
```

### 3. ... or expose via REST API.

```rust
use axum::Router;
use tower_bpmn::Api;

let runtime: Runtime<_, InMemory> = Runtime::default();
runtime.register_process(OrderProcess).unwrap();

let api = Api::new("api", runtime);
let app = Router::new()
    .nest_service("/api", api);

// Start server and visit http://localhost:3000/api/
```

## Contributing

Contributions are welcome! Please:
1. Fork the repository
2. Create a feature branch (`git checkout -b feature/my-feature`)
3. Write tests for new functionality
4. Ensure `cargo fmt && cargo clippy -- -D warnings` pass
5. Open a pull request

## License

This project is licensed under the [MIT License](LICENSE).
