use std::future::Future;

pub struct IoEngine {
    rt: Option<tokio::runtime::Runtime>,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum IoEngineKind {
    Coroutine,
    MultiThread,
    CurrentThread,
}

impl IoEngine {
    pub fn new(kind: IoEngineKind) -> IoEngine {
        IoEngine {
            rt: match kind {
                IoEngineKind::Coroutine => None,
                IoEngineKind::MultiThread => Some(
                    tokio::runtime::Builder::new_multi_thread()
                        .worker_threads(2)
                        .enable_all()
                        .build()
                        .unwrap(),
                ),
                IoEngineKind::CurrentThread => Some(
                    tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .unwrap(),
                ),
            },
        }
    }

    pub fn run<T>(&self, fut: impl Future<Output = T>) -> T {
        if let Some(rt) = &self.rt {
            rt.block_on(fut)
        } else {
            stackful::wait(fut)
        }
    }
}
