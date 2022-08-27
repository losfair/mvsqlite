use std::{
    collections::{HashMap, HashSet},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
};

use bytes::Bytes;
use rand::{Rng, RngCore};

use crate::{moka_backend::MokaBackend, LoadOutput, VersionedPageCache};

struct MockstoreConfig {
    target_page_count: u32,
    page_bound: u32,
    max_reads_per_round: u32,
    max_invalidations_per_round: u32,
    stale_all_probability: f64,
}

struct Entry {
    data: Option<Bytes>,
    version: u64,
}

async fn run_test(
    mut cache: Box<dyn VersionedPageCache>,
    mockstore_config: MockstoreConfig,
    test_size: usize,
) {
    let mockstore: Arc<Mutex<HashMap<u32, Entry>>> = Arc::new(Mutex::new(Default::default()));
    let mockstore_revalidate_count: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
    let mockstore_load_count: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
    let mut next_version: u64 = 1;
    let mut full_stale_count: u64 = 0;
    let mut invalidate_count: u64 = 0;
    let mut nonempty_count: u64 = 0;
    let mut empty_count: u64 = 0;

    for _ in 0..mockstore_config.target_page_count {
        let mut rng = rand::thread_rng();
        let page_id = rng.gen_range(0..mockstore_config.page_bound);
        let mut data = vec![0u8; 128];
        rng.fill_bytes(&mut data);
        let data = Bytes::from(data);
        mockstore.lock().unwrap().insert(
            page_id,
            Entry {
                data: Some(data),
                version: 0,
            },
        );
    }

    let nonempty_page_probability =
        mockstore_config.target_page_count as f64 / mockstore_config.page_bound as f64;

    for _ in 0..test_size {
        let num_reads = rand::thread_rng().gen_range(0..mockstore_config.max_reads_per_round);

        for _ in 0..num_reads {
            let page_id = rand::thread_rng().gen_range(0..mockstore_config.page_bound);
            let page = {
                let mockstore = mockstore.clone();
                let mockstore_revalidate_count = mockstore_revalidate_count.clone();
                let mockstore_load_count = mockstore_load_count.clone();
                cache
                    .get(
                        page_id,
                        Box::new(move |stale_version| {
                            Box::pin(async move {
                                let mockstore = mockstore.lock().unwrap();
                                let entry = mockstore.get(&page_id);
                                if entry.is_none() {
                                    Ok(LoadOutput::Replace {
                                        version: [0u8; 10],
                                        data: None,
                                    })
                                } else {
                                    let entry = entry.unwrap();
                                    let mut entry_version = [0u8; 10];
                                    entry_version[2..10]
                                        .copy_from_slice(&entry.version.to_be_bytes());
                                    if let Some(stale_version) = stale_version {
                                        if stale_version == entry_version {
                                            mockstore_revalidate_count
                                                .fetch_add(1, Ordering::Relaxed);
                                            Ok(LoadOutput::Fresh)
                                        } else {
                                            mockstore_load_count.fetch_add(1, Ordering::Relaxed);
                                            Ok(LoadOutput::Replace {
                                                version: entry_version,
                                                data: entry.data.clone(),
                                            })
                                        }
                                    } else {
                                        mockstore_load_count.fetch_add(1, Ordering::Relaxed);
                                        Ok(LoadOutput::Replace {
                                            version: entry_version,
                                            data: entry.data.clone(),
                                        })
                                    }
                                }
                            })
                        }),
                    )
                    .await
                    .unwrap()
            };
            let actual_page = mockstore
                .lock()
                .unwrap()
                .get(&page_id)
                .and_then(|x| x.data.clone());
            assert_eq!(page, actual_page);

            if page.is_none() {
                empty_count += 1;
            } else {
                nonempty_count += 1;
            }
        }

        let num_invalidations =
            rand::thread_rng().gen_range(0..=mockstore_config.max_invalidations_per_round);
        let mut invalidated_pages: HashSet<u32> = Default::default();
        for _ in 0..num_invalidations {
            let page_id = rand::thread_rng().gen_range(0..mockstore_config.page_bound);
            invalidated_pages.insert(page_id);

            let new_version_is_some = rand::thread_rng().gen_bool(nonempty_page_probability);
            if new_version_is_some {
                let mut data = vec![0u8; 128];
                rand::thread_rng().fill_bytes(&mut data);
                let data = Bytes::from(data);
                mockstore.lock().unwrap().insert(
                    page_id,
                    Entry {
                        version: next_version,
                        data: Some(data),
                    },
                );
            } else {
                mockstore.lock().unwrap().insert(
                    page_id,
                    Entry {
                        version: next_version,
                        data: None,
                    },
                );
            }
            next_version += 1;
        }

        let do_full_stale = rand::thread_rng().gen_bool(mockstore_config.stale_all_probability);
        if do_full_stale {
            cache.mark_all_as_stale();
            full_stale_count += 1;
        } else {
            cache
                .invalidate(&invalidated_pages.iter().copied().collect::<Vec<u32>>())
                .await;
            invalidate_count += 1;
        }
    }

    println!("test size: {}", test_size);
    println!(
        "revalidate: {}",
        mockstore_revalidate_count.load(Ordering::Relaxed)
    );
    println!("load: {}", mockstore_load_count.load(Ordering::Relaxed));
    println!("full stale: {}", full_stale_count);
    println!("invalidate: {}", invalidate_count);
    println!("nonempty: {}", nonempty_count);
    println!("empty: {}", empty_count);
}

#[tokio::test]
async fn test_moka_backend() {
    let backend = MokaBackend::new(2000);
    run_test(
        Box::new(backend),
        MockstoreConfig {
            target_page_count: 900,
            page_bound: 2500,
            max_reads_per_round: 10,
            max_invalidations_per_round: 0,
            stale_all_probability: 0.05,
        },
        10000,
    )
    .await;
}
