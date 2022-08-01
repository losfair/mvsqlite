use std::sync::Arc;
use tokio::sync::RwLock;

use anyhow::Result;
use mvclient::{MultiVersionClient, Transaction};
use rand::{thread_rng, Rng, RngCore};

use crate::inmem::Inmem;

pub struct Tester {
    mem: RwLock<Inmem>,
    client: Arc<MultiVersionClient>,
    num_pages: u32,
}

impl Tester {
    pub fn new(client: Arc<MultiVersionClient>, num_pages: u32) -> Arc<Self> {
        Arc::new(Self {
            mem: RwLock::new(Inmem::new()),
            client,
            num_pages,
        })
    }

    pub async fn run(self: &Arc<Self>, concurrency: usize, iterations: usize) {
        let handles = (0..concurrency)
            .map(|i| {
                let me = self.clone();
                tokio::spawn(me.task(i, iterations))
            })
            .collect::<Vec<_>>();
        for handle in handles {
            handle.await.unwrap().unwrap();
        }
    }

    async fn task(self: Arc<Self>, task_id: usize, iterations: usize) -> Result<()> {
        let mut mem = self.mem.write().await;
        let mut txn = self.client.create_transaction().await?;
        let mut txn_id = mem.start_transaction(txn.version());
        drop(mem);

        let mut last_writes: Vec<Option<Vec<u8>>> = vec![None; self.num_pages as usize];
        for it in 0..iterations {
            let mode = rand::thread_rng().gen_range(0..10);
            tracing::info!(task = task_id, iteration = it, mode = mode, "iteration");
            match mode {
                0..=5 => {
                    let num_reads = rand::thread_rng().gen_range(1..=10);
                    let reads = (0..num_reads)
                        .map(|_| rand::thread_rng().gen_range::<u32, _>(0..self.num_pages))
                        .collect::<Vec<_>>();
                    let pages = txn.read_many(&reads).await?;
                    let mem = self.mem.read().await;
                    for (&index, page) in reads.iter().zip(pages.iter()) {
                        tracing::info!(task = task_id, iteration = it, index = index, "read");
                        mem.verify_page(txn_id, index, page);
                    }
                }
                6..=7 => {
                    let num_writes = rand::thread_rng().gen_range(1..=10);
                    let writes = (0..num_writes)
                        .map(|_| {
                            let mut rng = rand::thread_rng();
                            let index = rng.gen_range::<u32, _>(0..self.num_pages);

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

                            tracing::info!(task = task_id, iteration = it, index = index, "write");

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
                    match txn.commit(None).await? {
                        Some(info) => {
                            mem.commit_transaction(txn_id, &info.version);
                        }
                        None => mem.drop_transaction(txn_id),
                    }
                    drop(mem);
                    tokio::task::yield_now().await;
                    (txn, txn_id) = self.create_transaction_random_base().await?;
                }
                9 => {
                    self.mem.write().await.drop_transaction(txn_id);
                    tokio::task::yield_now().await;
                    (txn, txn_id) = self.create_transaction_random_base().await?;
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
                let txn = self.client.create_transaction_at_version(version);
                let txn_id = mem.start_transaction(txn.version());
                return Ok((txn, txn_id));
            }
        }

        let txn = self.client.create_transaction().await?;
        let txn_id = mem.start_transaction(txn.version());
        Ok((txn, txn_id))
    }
}
