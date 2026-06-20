use thiserror::Error;

#[derive(Debug, Error)]
pub enum ReversionError {
    #[error("configuration invalid: {0}")]
    Config(String),

    #[error("numerical instability in {component}: {detail}")]
    NumericalInstability { component: &'static str, detail: String },
}
