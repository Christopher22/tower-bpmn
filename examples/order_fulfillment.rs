use axum_bpmn::{Process, ProcessBuilder, Runtime, Token};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct OrderRequest {
    order_id: u64,
    express_shipping: bool,
    inventory_available: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct OrderFulfillment;

impl Process for OrderFulfillment {
    type Input = OrderRequest;
    type Output = String;

    fn name(&self) -> &str {
        "order-fulfillment"
    }

    fn define(
        &self,
        process: ProcessBuilder<Self, Self::Input>,
    ) -> ProcessBuilder<Self, Self::Output> {
        let [in_stock, out_of_stock] = process
            .then("validate-order", |_token: &Token, order| order)
            .split(axum_bpmn::gateways::Xor::for_splitting(
                |_token: &Token, order: OrderRequest| {
                    if order.inventory_available { 0 } else { 1 }
                },
            ));

        let [payment, packaging] = in_stock.split(axum_bpmn::gateways::And);
        let in_stock_done = ProcessBuilder::join(
            axum_bpmn::gateways::And,
            [
                payment.then("capture-payment", |_token, order| {
                    format!("payment-captured: {}", order.order_id)
                }),
                packaging.then("prepare-shipment", |_token, order| {
                    if order.express_shipping {
                        format!("express-label-created: {}", order.order_id)
                    } else {
                        format!("standard-label-created: {}", order.order_id)
                    }
                }),
            ],
        )
        .then("dispatch", |_token, [payment_state, shipment_state]| {
            format!("{payment_state} | {shipment_state} | dispatched")
        });

        ProcessBuilder::join(
            axum_bpmn::gateways::Xor::for_joining(),
            [
                in_stock_done,
                out_of_stock.then("cancel-order", |_token, order| {
                    format!(
                        "order-cancelled: {} (inventory unavailable)",
                        order.order_id
                    )
                }),
            ],
        )
    }
}

#[tokio::main]
async fn main() {
    let mut runtime = Runtime::new(axum_bpmn::executor::TokioExecutor);
    runtime
        .register_process(OrderFulfillment)
        .expect("register order fulfillment process");

    let token = runtime
        .run(
            OrderFulfillment,
            OrderRequest {
                order_id: 2026001,
                express_shipping: true,
                inventory_available: true,
            },
        )
        .expect("start process")
        .wait_for_completion()
        .await;

    println!("Order result: {:?}", token.get_last::<String>());
    println!("Current task: {}", token.current_task());
}
