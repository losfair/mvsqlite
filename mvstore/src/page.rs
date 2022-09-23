use bytes::Bytes;

use crate::fixed::FixedString;

pub struct Page {
    pub version: FixedString,
    pub data: Bytes,
}

#[derive(Default)]
pub struct DecodedPage {
    pub data: Vec<u8>,
}

pub const PAGE_ENCODING_NONE: u8 = 0;
pub const PAGE_ENCODING_ZSTD: u8 = 1;
pub const PAGE_ENCODING_DELTA: u8 = 2;

pub const MAX_PAGE_SIZE: usize = 32768;

impl Page {
    pub fn compress_zstd(data: &[u8]) -> Vec<u8> {
        let max_compressed_size = zstd::zstd_safe::compress_bound(data.len());
        let mut buf = vec![0u8; max_compressed_size + 1];
        buf[0] = PAGE_ENCODING_ZSTD;
        let compressed_size = zstd::bulk::compress_to_buffer(data, &mut buf[1..], 0)
            .expect("compress_to_buffer failed");
        buf.truncate(compressed_size + 1);
        buf
    }
}
