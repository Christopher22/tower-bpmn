use axum_bpmn::{Process, ProcessBuilder, Runtime, Token};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct LoanApplication {
    applicant_id: u64,
    amount: u32,
    risk_score: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LoanApproval;

impl Process for LoanApproval {
    type Input = LoanApplication;
    type Output = String;

    fn name(&self) -> &str {
        "loan-approval"
    }

    fn define(
        &self,
        process: ProcessBuilder<Self, Self::Input>,
    ) -> ProcessBuilder<Self, Self::Output> {
        let [auto_approve, manual_review] = process
            .then("collect-application", |_token: &Token, app| app)
            .split(axum_bpmn::gateways::Xor::for_splitting(
                |_token: &Token, app: LoanApplication| {
                    if app.risk_score < 450 && app.amount <= 50_000 {
                        0
                    } else {
                        1
                    }
                },
            ));

        let auto_done = auto_approve.then("auto-approve", |_token, app| {
            format!("approved automatically for applicant {}", app.applicant_id)
        });

        let [fraud_check, compliance_check] = manual_review.split(axum_bpmn::gateways::And);
        let manual_done = ProcessBuilder::join(
            axum_bpmn::gateways::And,
            [
                fraud_check.then("fraud-check", |_token, app| {
                    format!("fraud-check-ok: {}", app.applicant_id)
                }),
                compliance_check.then("compliance-check", |_token, app| {
                    format!("compliance-check-ok: {}", app.applicant_id)
                }),
            ],
        )
        .then("manual-decision", |_token, [fraud, compliance]| {
            format!("manual approval after {fraud} and {compliance}")
        });

        ProcessBuilder::join(
            axum_bpmn::gateways::Xor::for_joining(),
            [auto_done, manual_done],
        )
    }
}

#[tokio::main]
async fn main() {
    let mut runtime = Runtime::new(axum_bpmn::executor::TokioExecutor);
    runtime
        .register_process(LoanApproval)
        .expect("register loan approval process");

    let token = runtime
        .run(
            LoanApproval,
            LoanApplication {
                applicant_id: 9001,
                amount: 120_000,
                risk_score: 620,
            },
        )
        .expect("start process")
        .wait_for_completion()
        .await;

    println!("Loan decision: {:?}", token.get_last::<String>());
    println!("Current task: {}", token.current_task());
}
