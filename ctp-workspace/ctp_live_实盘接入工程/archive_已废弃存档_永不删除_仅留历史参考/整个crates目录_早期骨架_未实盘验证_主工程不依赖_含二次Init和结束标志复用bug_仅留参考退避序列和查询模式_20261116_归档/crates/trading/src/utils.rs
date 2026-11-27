//! CTP（上期技术综合交易平台）回调工具函数。从旧项目原样复用。
//! `copy_str` / `read_str`（GB18030 解码）/ `rsp_err`。

/// 从 CTP 定长字节数组拷贝出字符串，遇 0 截断。
pub fn copy_str(src: &[u8]) -> String {
    let end = src.iter().position(|&b| b == 0).unwrap_or(src.len());
    String::from_utf8_lossy(&src[..end]).into_owned()
}

/// GB18030 解码，用于中文错误信息。
pub fn read_str(src: &[u8]) -> String {
    let end = src.iter().position(|&b| b == 0).unwrap_or(src.len());
    let (cow, _, _) = encoding_rs::GB18030.decode(&src[..end]);
    cow.into_owned()
}

/// 统一错误响应描述。
pub fn rsp_err(error_id: i32, error_msg: &[u8]) -> String {
    format!("ErrorID={} ErrorMsg={}", error_id, read_str(error_msg))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn copy_str_stops_at_zero() {
        let mut buf = [0u8; 8];
        buf[..3].copy_from_slice(b"abc");
        assert_eq!(copy_str(&buf), "abc");
    }

    #[test]
    fn read_str_gb18030() {
        // “错误” 的 GB18030 编码
        let bytes = [0xB4, 0xED, 0xCE, 0xF3];
        assert_eq!(read_str(&bytes), "错误");
    }
}
