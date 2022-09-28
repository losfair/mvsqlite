use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::RwLock;

use anyhow::Result;
use mvclient::{CommitError, CommitOutput, MultiVersionClient, Transaction};
use rand::{thread_rng, Rng, RngCore};

use crate::inmem::Inmem;

pub struct Tester {
    mem: RwLock<Inmem>,
    client: Arc<MultiVersionClient>,
    busy_versions: Mutex<BTreeMap<String, u64>>,
    config: TesterConfig,
}

pub struct TesterConfig {
    pub disable_ryw: bool,
    pub admin_api: String,
    pub num_pages: u32,
    pub permit_410: bool,
    pub disable_read_set: bool,
}

impl Tester {
    pub fn new(client: Arc<MultiVersionClient>, config: TesterConfig) -> Arc<Self> {
        Arc::new(Self {
            mem: RwLock::new(Inmem::new()),
            client,
            busy_versions: Mutex::new(BTreeMap::new()),
            config,
        })
    }

    pub async fn run(self: &Arc<Self>, concurrency: usize, iterations: usize) {
        let truncate_worker = tokio::spawn(self.clone().truncate_worker());
        let delete_unreferenced_content_worker =
            tokio::spawn(self.clone().delete_unreferenced_content_worker());
        let handles = (0..concurrency)
            .map(|i| {
                let me = self.clone();
                tokio::spawn(me.task(i, iterations))
            })
            .collect::<Vec<_>>();
        for handle in handles {
            handle.await.unwrap().unwrap();
        }
        truncate_worker.abort();
        delete_unreferenced_content_worker.abort();
    }

    async fn truncate_worker(self: Arc<Self>) {
        let rc = reqwest::Client::new();
        loop {
            let sleep_dur_ms = rand::thread_rng().gen_range(1..1000);
            let sleep_dur = Duration::from_millis(sleep_dur_ms);
            tokio::time::sleep(sleep_dur).await;

            let mut remove_point: String;
            {
                let mut mem = self.mem.write().await;
                let mut versions = mem.versions.keys().cloned().collect::<Vec<_>>();
                versions.pop(); // never remove the latest version, if any

                if versions.len() == 0 {
                    continue;
                }
                let split_point = rand::thread_rng().gen_range(0..versions.len());
                remove_point = versions[split_point].clone();

                if let Some((k, _)) = self.busy_versions.lock().unwrap().iter().next() {
                    if *k < remove_point {
                        remove_point = k.clone();
                    }
                }
                let mut removals: HashSet<String> = HashSet::new();
                for x in &versions {
                    if *x < remove_point {
                        mem.versions.remove(x);
                        removals.insert(x.clone());
                    }
                }
                mem.version_list = mem
                    .version_list
                    .iter()
                    .filter(|x| !removals.contains(*x))
                    .cloned()
                    .collect::<Vec<_>>();
            }
            let payload = serde_json::json!({
                "key": &self.client.config().ns_key,
                "before_version": &remove_point,
                "apply": true,
            });
            tracing::info!(remove_point = remove_point, "triggering truncation");
            match rc
                .post(format!("{}/api/truncate_namespace", self.config.admin_api,))
                .json(&payload)
                .send()
                .await
            {
                Ok(res) => match res.bytes().await {
                    Ok(_) => {
                        tracing::info!("truncated namespace");
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "failed to read response for truncate namespace");
                    }
                },
                Err(e) => {
                    tracing::error!(error = %e, "failed to truncate namespace");
                }
            }
        }
    }

    async fn delete_unreferenced_content_worker(self: Arc<Self>) {
        let rc = reqwest::Client::new();
        loop {
            let sleep_dur_ms = rand::thread_rng().gen_range(1..5000);
            let sleep_dur = Duration::from_millis(sleep_dur_ms);
            tokio::time::sleep(sleep_dur).await;
            let payload = serde_json::json!({
                "key": &self.client.config().ns_key,
                "apply": true,
            });
            tracing::info!("triggering duc");
            match rc
                .post(format!(
                    "{}/api/delete_unreferenced_content",
                    self.config.admin_api,
                ))
                .json(&payload)
                .send()
                .await
            {
                Ok(res) => match res.bytes().await {
                    Ok(_) => {
                        tracing::info!("deleted unreferenced content");
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "failed to read response for duc");
                    }
                },
                Err(e) => {
                    tracing::error!(error = %e, "failed to delete unreferenced content");
                }
            }
        }
    }

    fn acquire_version(&self, version: &str) {
        *self
            .busy_versions
            .lock()
            .unwrap()
            .entry(version.to_string())
            .or_default() += 1;
    }

    fn release_version(&self, version: &str) {
        let mut versions = self.busy_versions.lock().unwrap();
        let entry = versions.get_mut(version).unwrap();
        assert!(*entry > 0, "version {} is not acquired", version);
        *entry -= 1;
        if *entry == 0 {
            versions.remove(version);
        }
    }

    async fn task(self: Arc<Self>, task_id: usize, iterations: usize) -> Result<()> {
        let mut mem = self.mem.write().await;
        let mut txn = self.client.create_transaction(None).await?;
        let mut txn_id = mem.start_transaction(txn.version());
        self.acquire_version(txn.version());
        drop(mem);

        let mut last_writes: Vec<Option<Vec<u8>>> = vec![None; self.config.num_pages as usize];
        for it in 0..iterations {
            let mode = rand::thread_rng().gen_range(0..11);
            tracing::debug!(task = task_id, iteration = it, mode = mode, "iteration");
            match mode {
                0..=5 => {
                    let num_reads_requested = rand::thread_rng().gen_range(1..=10);
                    let reads = (0..num_reads_requested)
                        .map(|_| rand::thread_rng().gen_range::<u32, _>(0..self.config.num_pages))
                        .filter(|x| !self.config.disable_ryw || !txn.page_is_written(*x))
                        .collect::<Vec<_>>();
                    if reads.len() == 0 {
                        continue;
                    }
                    for &id in &reads {
                        txn.mark_read(id);
                    }
                    let pages = txn.read_many_nomark(&reads).await?;
                    let mut mem = self.mem.write().await;
                    for (&index, page) in reads.iter().zip(pages.iter()) {
                        tracing::debug!(
                            task = task_id,
                            txn_id,
                            iteration = it,
                            index = index,
                            version = txn.version(),
                            "read"
                        );
                        mem.verify_page(txn_id, index, page, txn.version());
                    }
                }
                6..=7 => {
                    let num_writes = rand::thread_rng().gen_range(1..=10);
                    let writes = (0..num_writes)
                        .map(|_| {
                            let mut rng = rand::thread_rng();
                            let index = rng.gen_range::<u32, _>(0..self.config.num_pages);

                            // Test delta encoding
                            let data = {
                                let last_write = &last_writes[index as usize];
                                if last_write.is_some() && rng.gen_bool(0.2) {
                                    let mut data = last_write.as_ref().unwrap().clone();
                                    let start = rng.gen_range(0..data.len());
                                    let end = rng.gen_range(start..data.len());
                                    rng.fill_bytes(&mut data[start..end]);
                                    data
                                } else {
                                    let mut data = vec![0u8; 2048];
                                    rng.fill_bytes(&mut data);
                                    data
                                }
                            };
                            if rng.gen_bool(0.5) {
                                last_writes[index as usize] = Some(data.clone());
                            }

                            tracing::debug!(
                                task = task_id,
                                txn_id,
                                iteration = it,
                                index = index,
                                version = txn.version(),
                                "write"
                            );

                            (index, data)
                        })
                        .collect::<Vec<_>>();
                    let writes = writes
                        .iter()
                        .map(|(index, data)| (*index, data.as_slice()))
                        .collect::<Vec<_>>();
                    txn.write_many(&writes).await?;
                    let mut mem = self.mem.write().await;
                    for &(index, data) in &writes {
                        mem.write_page(txn_id, index, data);
                    }
                }
                8 => {
                    let mut mem = self.mem.write().await;
                    let version = txn.version().to_string();
                    let txn_version = txn.version().to_string();
                    match txn.commit(None, &HashMap::new()).await {
                        Ok(CommitOutput::Committed(info)) => {
                            mem.commit_transaction(txn_id, &info.version, txn_version.as_str());
                        }
                        Ok(CommitOutput::Conflict) => mem.drop_transaction(txn_id),
                        Ok(CommitOutput::Empty) => mem.drop_transaction(txn_id),
                        Err(e) => {
                            let mut ignore = false;
                            if let Some(x) = e.downcast_ref::<CommitError>() {
                                match x {
                                    CommitError::Status(code)
                                        if code.as_u16() == 410 && self.config.permit_410 =>
                                    {
                                        tracing::warn!("ignored http 410 as requested");
                                        ignore = true;
                                    }
                                    _ => {}
                                }
                            }
                            if ignore {
                                mem.drop_transaction(txn_id);
                            } else {
                                return Err(e);
                            }
                        }
                    }
                    self.release_version(&version);
                    drop(mem);
                    tokio::task::yield_now().await;
                    (txn, txn_id) = self.create_transaction_random_base().await?;
                    tracing::debug!(version = txn.version(), "created txn");
                }
                9 => {
                    let mut mem = self.mem.write().await;
                    mem.drop_transaction(txn_id);
                    self.release_version(txn.version());
                    drop(mem);
                    tokio::task::yield_now().await;
                    (txn, txn_id) = self.create_transaction_random_base().await?;
                    tracing::debug!(version = txn.version(), "created txn");
                }
                10 => {
                    let dur_millis = rand::thread_rng().gen_range(1..100);
                    tokio::time::sleep(Duration::from_millis(dur_millis)).await;
                }
                _ => unreachable!(),
            }
        }

        Ok(())
    }

    async fn create_transaction_random_base(&self) -> Result<(Transaction, u64)> {
        let mut mem = self.mem.write().await;

        if thread_rng().gen_bool(0.5) {
            if let Some(version) = mem.pick_random_version() {
                let mut txn = self
                    .client
                    .create_transaction_at_version(None, version, false);
                let txn_id = mem.start_transaction(txn.version());
                self.acquire_version(txn.version());
                if !self.config.disable_read_set && thread_rng().gen_bool(0.5) {
                    txn.enable_read_set();
                }
                return Ok((txn, txn_id));
            }
        }

        let mut txn = self.client.create_transaction(None).await?;
        let txn_id = mem.start_transaction(txn.version());
        self.acquire_version(txn.version());
        if !self.config.disable_read_set && thread_rng().gen_bool(0.5) {
            txn.enable_read_set();
        }
        Ok((txn, txn_id))
    }
}
