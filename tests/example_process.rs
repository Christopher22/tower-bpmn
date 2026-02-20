use std::borrow::Cow;

use axum_bpmn::CorrelationKey;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct Proposal(String);

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct CheckedProposal {
    proposal: Proposal,
    formally_valid: bool,
    correlation_key: CorrelationKey,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct Rating(u32);

#[derive(Debug, Clone, PartialEq, Eq)]
struct Judgment;

impl axum_bpmn::Process for Judgment {
    type Input = CheckedProposal;
    type Output = (Rating, CorrelationKey);

    fn name(&self) -> &str {
        "Judge proposal"
    }

    fn define(
        &self,
        process: axum_bpmn::ProcessBuilder<Self, Self::Input>,
    ) -> axum_bpmn::ProcessBuilder<Self, Self::Output> {
        process
            .then(
                "Rate",
                |_token: &axum_bpmn::Token, checked_proposal: CheckedProposal| {
                    (Rating(42), checked_proposal.correlation_key.clone())
                },
            )
            .throw_message(
                "Inform manager",
                |_token: &axum_bpmn::Token, (rating, correlation_key): (Rating, CorrelationKey)| {
                    axum_bpmn::Message {
                        process: ProposalReview,
                        correlation_key,
                        payload: rating,
                    }
                },
            )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ProposalReview;

impl axum_bpmn::Process for ProposalReview {
    type Input = Proposal;
    type Output = Option<u32>;

    fn name(&self) -> &str {
        "Recieve and check proposal"
    }

    fn define(
        &self,
        process: axum_bpmn::ProcessBuilder<Self, Self::Input>,
    ) -> axum_bpmn::ProcessBuilder<Self, Self::Output> {
        let [process_valid_proposal, process_invalid_proposal] = process
            .then(
                "Check proposal",
                |_token: &axum_bpmn::Token, proposal: Proposal| CheckedProposal {
                    proposal: proposal.clone(),
                    formally_valid: match proposal.0.as_str() {
                        "Proposal A" => true,
                        "Proposal B" => false,
                        _ => false,
                    },
                    correlation_key: CorrelationKey::new(),
                },
            )
            .split(axum_bpmn::gateways::Xor::for_splitting(
                |_token: &axum_bpmn::Token, checked_proposal: CheckedProposal| {
                    if checked_proposal.formally_valid {
                        0
                    } else {
                        1
                    }
                },
            ));

        let parallel_rating_process = {
            let [process_judge1, process_judge2] =
                process_valid_proposal.split(axum_bpmn::gateways::And);

            axum_bpmn::ProcessBuilder::join(
                axum_bpmn::gateways::And,
                [
                    process_judge1
                        .then(
                            "Set judge 1",
                            |_: &axum_bpmn::Token, mut checked_proposal: CheckedProposal| {
                                checked_proposal.rater_id = 1;
                                (checked_proposal, CorrelationKey::new())
                            },
                        )
                        .throw_message(
                            "Inform judge 1",
                            |_token: &axum_bpmn::Token, proposal: CheckedProposal| {
                                axum_bpmn::Message::new(Judgment, proposal)
                            },
                        )
                        .wait_for(axum_bpmn::IncomingMessage::<Judgment, Rating>::new(
                            Judgment,
                            "Receive rating 1",
                        )),
                    process_judge2
                        .then(
                            "Set judge 2",
                            |_: &axum_bpmn::Token, mut checked_proposal: CheckedProposal| {
                                checked_proposal.rater_id = 2;
                                (checked_proposal, CorrelationKey::new())
                            },
                        )
                        .throw_message(
                            "Inform judge 2",
                            |_token: &axum_bpmn::Token, proposal: CheckedProposal| {
                                axum_bpmn::Message::new(Judgment, proposal)
                            },
                        )
                        .wait_for(axum_bpmn::IncomingMessage::<Judgment, Rating>::new(
                            Judgment,
                            "Receive rating 2",
                        )),
                ],
            )
            .then(
                "Aggregate",
                |_token: &axum_bpmn::Token, ratings: &[(CheckedProposal, Rating); 2]| {
                    Some(ratings[0].1.0 + ratings[1].1.0)
                },
            )
        };

        axum_bpmn::ProcessBuilder::join(
            axum_bpmn::gateways::Xor::for_joining(),
            [
                parallel_rating_process,
                process_invalid_proposal.then("Invalid proposal", |_token, _| None),
            ],
        )
    }
}

#[tokio::test]
async fn rating_process() {
    let mut runtime = axum_bpmn::Runtime::new(axum_bpmn::executor::TokioExecutor);
    runtime
        .register_process(ProposalReview)
        .expect("valid process");
    runtime.register_process(Judgment).expect("valid process");

    let instance = runtime
        .run(ProposalReview, Proposal("Proposal A".to_string()))
        .expect("valid instance");

    let context = instance.wait_for_completion().await;
    assert_eq!(
        context
            .get_last::<Option<u32>>()
            .expect("Aggregate should be run once"),
        Some(42 + 42)
    );
}
