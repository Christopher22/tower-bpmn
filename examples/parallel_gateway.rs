use axum_bpmn::{Process, ProcessBuilder, Runtime};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ParallelExample;

impl Process for ParallelExample {
    type Input = i32;
    type Output = [i32; 2];

    fn name(&self) -> &str {
        "parallel-gateway-example"
    }

    fn define(
        &self,
        process: ProcessBuilder<Self, Self::Input>,
    ) -> ProcessBuilder<Self, Self::Output> {
        let [left, right] = process.split(axum_bpmn::gateways::And);
        ProcessBuilder::join(
            axum_bpmn::gateways::And,
            [
                left.then("left-task", |_token, value| value + 1),
                right.then("right-task", |_token, value| value + 2),
            ],
        )
    }
}

#[tokio::main]
async fn main() {
    let mut runtime = Runtime::new(axum_bpmn::executor::TokioExecutor);
    runtime
        .register_process(ParallelExample)
        .expect("register process");

    let token = runtime
        .run(ParallelExample, 10)
        .expect("run process")
        .wait_for_completion()
        .await;

    println!(
        "Parallel gateway output: {:?}",
        token.get_last::<[i32; 2]>()
    );
    println!("Current task: {}", token.current_task());
}
