use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Value(&'static str),
    Index(&'static str),
    Overflow(&'static str),
    Runtime(&'static str),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Value(s) | Self::Index(s) | Self::Overflow(s) | Self::Runtime(s) => {
                f.write_str(s)
            }
        }
    }
}

impl std::error::Error for Error {}
pub type Result<T> = std::result::Result<T, Error>;

pub fn require(ok: bool, message: &'static str) -> Result<()> {
    if ok {
        Ok(())
    } else {
        Err(Error::Value(message))
    }
}

pub fn add(a: i64, b: i64) -> Result<i64> {
    a.checked_add(b)
        .ok_or(Error::Overflow("time addition overflow"))
}

pub fn index(value: i32, len: usize, message: &'static str) -> Result<usize> {
    let i = usize::try_from(value).map_err(|_| Error::Index(message))?;
    if i < len {
        Ok(i)
    } else {
        Err(Error::Index(message))
    }
}
