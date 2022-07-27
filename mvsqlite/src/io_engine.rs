use std::future::Future;

pub struct IoEngine {
    rt: Option<tokio::runtime::Runtime>,
}

impl IoEngine {
    pub fn new(coroutine: bool) -> IoEngine {
        IoEngine {
            rt: if coroutine {
                None
            } else {
                Some(
                    tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .unwrap(),
                )
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
