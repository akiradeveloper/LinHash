#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Local level mismatch")]
    LocalLevelMismatch,
    #[error(transparent)]
    Rkyv(#[from] rkyv::rancor::Error),
    #[error(transparent)]
    IO(#[from] std::io::Error),
    #[error(transparent)]
    RustixIO(#[from] rustix::io::Errno),
}

pub type Result<T> = std::result::Result<T, Error>;
