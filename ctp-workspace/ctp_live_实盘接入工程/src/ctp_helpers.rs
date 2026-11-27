//! CTP 定长字符串与 u8 枚举的转换辅助。
//! 原则：禁止静默截断、禁止 unwrap、禁止 panic 跨 FFI 边界。

use std::fmt;

#[derive(Debug)]
pub enum CstrError {
    TooLong { field_len: usize, max: usize },
}

impl fmt::Display for CstrError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CstrError::TooLong { field_len, max } => {
                write!(f, "field too long: {} bytes, max {}", field_len, max)
            }
        }
    }
}

impl std::error::Error for CstrError {}

/// 把 `&str` 写入 CTP 定长 `[u8; N]`，结尾补 0。
/// 超长直接报错，不静默截断。
pub fn cstr<const N: usize>(s: &str) -> Result<[u8; N], CstrError> {
    let bytes = s.as_bytes();
    if bytes.len() >= N {
        return Err(CstrError::TooLong { field_len: bytes.len(), max: N - 1 });
    }
    let mut buf = [0u8; N];
    buf[..bytes.len()].copy_from_slice(bytes);
    Ok(buf)
}

/// 从 CTP 定长 `[u8; N]` 读出字符串，遇 0 截断。
/// 回调线程不能 panic，非法 UTF-8 用 lossy 替换并标注。
#[must_use]
pub fn from_cstr(buf: &[u8]) -> String {
    let end = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
    String::from_utf8_lossy(&buf[..end]).trim().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cstr_ok() {
        let b: [u8; 8] = cstr("abc").expect("abc fits");
        assert_eq!(&b[..3], b"abc");
        assert_eq!(b[3], 0);
    }

    #[test]
    fn cstr_too_long_errors() {
        // N=4，只允许 3 字节，4 字节必须报错
        let r: Result<[u8; 4], _> = cstr("abcd");
        assert!(matches!(r, Err(CstrError::TooLong { .. })));
    }

    #[test]
    fn from_cstr_trims_at_nul() {
        let b = [b'a', b'b', 0, b'c'];
        assert_eq!(from_cstr(&b), "ab");
    }

    #[test]
    fn from_cstr_lossy_invalid_utf8() {
        let b = [0xff, 0xfe, 0];
        // 不 panic 即可，内容不保证
        let _ = from_cstr(&b);
    }
}
