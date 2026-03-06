mod error;
mod response;
mod service;

pub use response::openapi;
pub use service::Api;
#[cfg(test)]
mod tests;
