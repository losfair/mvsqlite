use std::time::SystemTime;

use crate::{keys::KeyCodec, replica::ReplicaManager};
use anyhow::Result;
use foundationdb::{
    future::{FdbValue},
    options::StreamingMode,
    tuple::unpack,
    RangeOption, Transaction,
};
use futures::TryStreamExt;
use serde::Serialize;

#[derive(Serialize)]
pub struct TimeToVersionResponse {
    pub after: Option<TimeToVersionPoint>,
    pub not_after: Option<TimeToVersionPoint>,
}

#[derive(Serialize)]
pub struct TimeToVersionPoint {
    pub version: String,
    pub time: u64,
}

pub async fn time2version(
    txn: &Transaction,
    key_codec: &KeyCodec,
    time_in_seconds: u64,
    rm: Option<&ReplicaManager>,
) -> Result<TimeToVersionResponse> {
    // Special case: t=0 gives the current version
    if time_in_seconds == 0 {
        let version = if let Some(rm) = rm {
            rm.replica_version(&txn).await?
        } else {
            txn.get_read_version().await?
        };
        let next_version = version + 1;
        let time = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let after = Some(TimeToVersionPoint {
            version: format!("{}ffff", hex::encode(&version.to_be_bytes())),
            time,
        });
        let not_after = Some(TimeToVersionPoint {
            version: format!("{}0000", hex::encode(&next_version.to_be_bytes())),
            time: time + 1,
        });

        return Ok(TimeToVersionResponse { after, not_after });
    }

    let key = key_codec.construct_time2version_key(time_in_seconds);
    let lower_bound = key_codec.construct_time2version_key(std::u64::MIN);
    let upper_bound = key_codec.construct_time2version_key(std::u64::MAX);
    let prefix = key_codec.construct_time2version_prefix();

    let after: Vec<_> = txn
        .get_ranges_keyvalues(
            RangeOption {
                limit: Some(1),
                reverse: true,
                mode: StreamingMode::Small,
                ..RangeOption::from(lower_bound.clone()..key.clone())
            },
            true,
        )
        .try_collect()
        .await?;

    let not_after: Vec<_> = txn
        .get_ranges_keyvalues(
            RangeOption {
                limit: Some(1),
                reverse: false,
                mode: StreamingMode::Small,
                ..RangeOption::from(key.clone()..upper_bound.clone())
            },
            true,
        )
        .try_collect()
        .await?;
    let map_it = |x: Vec<FdbValue>| -> Option<TimeToVersionPoint> {
        if x.len() == 0 || x[0].key().len() < prefix.len() {
            None
        } else {
            let item = &x[0];
            let time_suffix = &item.key()[prefix.len()..];
            match unpack::<u64>(time_suffix) {
                Ok(time_secs) => match <[u8; 10]>::try_from(item.value()) {
                    Ok(version) => Some(TimeToVersionPoint {
                        version: hex::encode(&version),
                        time: time_secs,
                    }),
                    Err(_) => None,
                },
                Err(_) => None,
            }
        }
    };
    let after = map_it(after);
    let not_after = map_it(not_after);

    Ok(TimeToVersionResponse { after, not_after })
}
