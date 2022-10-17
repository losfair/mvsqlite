use foundationdb::tuple::pack;

use crate::fixed::FixedKeyVec;

pub struct KeyCodec {
    pub metadata_prefix: String,
    pub raw_data_prefix: Vec<u8>,
}

impl KeyCodec {
    pub fn construct_nsmd_key(&self, ns_id: [u8; 10]) -> Vec<u8> {
        let mut key = pack(&(self.metadata_prefix.as_str(), "nsmd"));
        key.push(0x32);
        key.extend_from_slice(&ns_id);
        key
    }

    pub fn construct_nsrollbackcursor_key(&self, ns_id: [u8; 10]) -> Vec<u8> {
        let mut key = pack(&(self.metadata_prefix.as_str(), "nsrollbackcursor"));
        key.push(0x32);
        key.extend_from_slice(&ns_id);
        key
    }

    pub fn construct_nstask_key(&self, ns_id: [u8; 10], task: &str) -> Vec<u8> {
        let mut key = pack(&(self.metadata_prefix.as_str(), "nstask"));
        key.push(0x32);
        key.extend_from_slice(&ns_id);
        key.extend_from_slice(&pack(&(task,)));
        key
    }

    pub fn construct_globaltask_key(&self, task: &str) -> Vec<u8> {
        let key = pack(&(self.metadata_prefix.as_str(), "globaltask", task));
        key
    }

    pub fn construct_time2version_prefix(&self) -> Vec<u8> {
        let key = pack(&(self.metadata_prefix.as_str(), "time2version"));
        key
    }

    pub fn construct_time2version_key(&self, time_secs: u64) -> Vec<u8> {
        let key = pack(&(self.metadata_prefix.as_str(), "time2version", time_secs));
        key
    }

    pub fn construct_nskey_prefix(&self) -> Vec<u8> {
        pack(&(self.metadata_prefix.as_str(), "nskey"))
    }

    pub fn construct_nskey_key(&self, ns_key: &str) -> Vec<u8> {
        pack(&(self.metadata_prefix.as_str(), "nskey", ns_key))
    }

    pub fn construct_ns_commit_token_key(&self, ns_id: [u8; 10]) -> Vec<u8> {
        let mut key = pack(&(self.metadata_prefix.as_str(), "ns_commit_token"));
        key.push(0x32);
        key.extend_from_slice(&ns_id);
        key
    }

    pub fn construct_last_write_version_key(&self, ns_id: [u8; 10]) -> FixedKeyVec {
        let mut buf = FixedKeyVec::new();
        buf.extend_from_slice(&self.raw_data_prefix).unwrap();
        buf.extend_from_slice(&ns_id).unwrap();
        buf.push(b'v').unwrap();
        buf
    }

    pub fn construct_ns_data_prefix(&self, ns_id: [u8; 10]) -> FixedKeyVec {
        let mut buf = FixedKeyVec::new();
        buf.extend_from_slice(&self.raw_data_prefix).unwrap();
        buf.extend_from_slice(&ns_id).unwrap();
        buf
    }

    pub fn construct_page_key(
        &self,
        ns_id: [u8; 10],
        page_index: u32,
        page_version: [u8; 10],
    ) -> FixedKeyVec {
        let mut buf = FixedKeyVec::new();
        buf.extend_from_slice(&self.raw_data_prefix).unwrap();
        buf.extend_from_slice(&ns_id).unwrap();
        buf.push(b'p').unwrap();
        buf.extend_from_slice(&page_index.to_be_bytes()).unwrap();
        buf.extend_from_slice(&page_version).unwrap();
        buf
    }

    pub fn construct_content_key(&self, ns_id: [u8; 10], hash: [u8; 32]) -> FixedKeyVec {
        let mut buf = FixedKeyVec::new();
        buf.extend_from_slice(&self.raw_data_prefix).unwrap();
        buf.extend_from_slice(&ns_id).unwrap();
        buf.push(b'c').unwrap();
        buf.extend_from_slice(&hash).unwrap();
        buf
    }

    pub fn construct_contentindex_key(&self, ns_id: [u8; 10], hash: [u8; 32]) -> FixedKeyVec {
        let mut buf = FixedKeyVec::new();
        buf.extend_from_slice(&self.raw_data_prefix).unwrap();
        buf.extend_from_slice(&ns_id).unwrap();
        buf.push(b'd').unwrap();
        buf.extend_from_slice(&hash).unwrap();
        buf
    }

    pub fn construct_delta_referrer_key(
        &self,
        ns_id: [u8; 10],
        from_hash: [u8; 32],
    ) -> FixedKeyVec {
        let mut buf = FixedKeyVec::new();
        buf.extend_from_slice(&self.raw_data_prefix).unwrap();
        buf.extend_from_slice(&ns_id).unwrap();
        buf.push(b'r').unwrap();
        buf.extend_from_slice(&from_hash).unwrap();
        buf
    }

    pub fn construct_changelog_key(&self, ns_id: [u8; 10], version: [u8; 10]) -> FixedKeyVec {
        let mut buf = FixedKeyVec::new();
        buf.extend_from_slice(&self.raw_data_prefix).unwrap();
        buf.extend_from_slice(&ns_id).unwrap();
        buf.push(b'l').unwrap();
        buf.extend_from_slice(&version).unwrap();
        buf
    }
}
