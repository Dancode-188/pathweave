use std::collections::{BTreeMap, HashMap};
use std::convert::TryFrom;
use std::time::Duration;

use async_trait::async_trait;
use bp7::{
    bundle::Bundle,
    canonical::new_payload_block,
    crc::CRC_NO,
    dtntime::{CreationTimestamp, DTN_TIME_EPOCH},
    eid::EndpointID,
    flags::{BlockControlFlags, BundleControlFlags},
    primary::PrimaryBlockBuilder,
};
use bytes::Bytes;

use crate::{Connection, PathweaveError, Result};

const PW_NODE: &str = "pathweave/local";
const BUNDLE_LIFETIME: Duration = Duration::from_secs(3600);

struct PendingBundle {
    total_data_length: u64,
    fragments: BTreeMap<u64, Vec<u8>>,
}

impl PendingBundle {
    fn received_len(&self) -> u64 {
        self.fragments.values().map(|v| v.len() as u64).sum()
    }

    fn is_complete(&self) -> bool {
        self.received_len() >= self.total_data_length
    }

    fn reassemble(self) -> Vec<u8> {
        let mut data = Vec::with_capacity(self.total_data_length as usize);
        for (_, chunk) in self.fragments {
            data.extend_from_slice(&chunk);
        }
        data
    }
}

/// Wraps a transport Connection with BPv7 framing, fragmentation, and reassembly.
///
/// Session holds a BundleLayer as its inner Box<dyn Connection>. The session
/// layer passes ciphertext here without any knowledge of fragmentation; this
/// layer handles all BPv7 encoding and splits large messages to fit the MTU.
pub struct BundleLayer {
    inner: Box<dyn Connection>,
    seq: u64,
    pending: HashMap<u64, PendingBundle>,
}

impl BundleLayer {
    pub fn new(inner: Box<dyn Connection>) -> Self {
        Self {
            inner,
            seq: 0,
            pending: HashMap::new(),
        }
    }
}

fn pw_eid() -> Result<EndpointID> {
    EndpointID::with_dtn(PW_NODE).map_err(|e| PathweaveError::Bundle(e.to_string()))
}

/// Serializes a zero-payload fragment bundle and returns its CBOR byte length.
///
/// This gives the fixed overhead that every fragment carries. We use `total_len`
/// as the fragmentation_offset in the dummy bundle so that the CBOR varint for
/// the offset field is at least as large as any actual fragment offset will be,
/// giving a conservative (never under) estimate.
fn fragment_header_overhead(
    eid: &EndpointID,
    ts: &CreationTimestamp,
    total_len: u64,
) -> Result<usize> {
    let pblock = PrimaryBlockBuilder::default()
        .bundle_control_flags(BundleControlFlags::BUNDLE_IS_FRAGMENT.bits())
        .destination(eid.clone())
        .source(eid.clone())
        .report_to(EndpointID::none())
        .creation_timestamp(ts.clone())
        .lifetime(BUNDLE_LIFETIME)
        .fragmentation_offset(total_len)
        .total_data_length(total_len)
        .build()
        .map_err(|e| PathweaveError::Bundle(e.to_string()))?;
    let mut dummy = Bundle::new(
        pblock,
        vec![new_payload_block(BlockControlFlags::empty(), vec![])],
    );
    dummy.set_crc(CRC_NO);
    dummy.sort_canonicals();
    Ok(dummy.to_cbor().len())
}

#[async_trait]
impl Connection for BundleLayer {
    async fn send_bytes(&mut self, bytes: &[u8]) -> Result<()> {
        let seq = self.seq;
        self.seq = self.seq.wrapping_add(1);

        let eid = pw_eid()?;
        let ts = CreationTimestamp::with_time_and_seq(DTN_TIME_EPOCH, seq);
        let mtu = self.inner.mtu();

        // Try to encode the whole payload as a single unfragmented bundle.
        let pblock = PrimaryBlockBuilder::default()
            .destination(eid.clone())
            .source(eid.clone())
            .report_to(EndpointID::none())
            .creation_timestamp(ts.clone())
            .lifetime(BUNDLE_LIFETIME)
            .build()
            .map_err(|e| PathweaveError::Bundle(e.to_string()))?;
        let mut bundle = Bundle::new(
            pblock,
            vec![new_payload_block(BlockControlFlags::empty(), bytes.to_vec())],
        );
        bundle.set_crc(CRC_NO);
        bundle.sort_canonicals();
        let cbor = bundle.to_cbor();

        // mtu == 0 means "no limit" (used in tests with unlimited connections).
        if mtu == 0 || cbor.len() <= mtu {
            return self.inner.send_bytes(&cbor).await;
        }

        // Fragmentation required.
        let overhead = fragment_header_overhead(&eid, &ts, bytes.len() as u64)?;
        let max_payload = mtu.saturating_sub(overhead);
        if max_payload == 0 {
            return Err(PathweaveError::Bundle(
                "MTU too small for BPv7 bundle framing overhead".into(),
            ));
        }

        let total_len = bytes.len() as u64;
        let mut offset = 0usize;
        while offset < bytes.len() {
            let end = (offset + max_payload).min(bytes.len());
            let chunk = bytes[offset..end].to_vec();

            let pblock = PrimaryBlockBuilder::default()
                .bundle_control_flags(BundleControlFlags::BUNDLE_IS_FRAGMENT.bits())
                .destination(eid.clone())
                .source(eid.clone())
                .report_to(EndpointID::none())
                .creation_timestamp(ts.clone())
                .lifetime(BUNDLE_LIFETIME)
                .fragmentation_offset(offset as u64)
                .total_data_length(total_len)
                .build()
                .map_err(|e| PathweaveError::Bundle(e.to_string()))?;

            let mut fragment = Bundle::new(
                pblock,
                vec![new_payload_block(BlockControlFlags::empty(), chunk)],
            );
            fragment.set_crc(CRC_NO);
            fragment.sort_canonicals();
            self.inner.send_bytes(&fragment.to_cbor()).await?;

            offset = end;
        }
        Ok(())
    }

    async fn recv_bytes(&mut self) -> Result<Bytes> {
        loop {
            let raw = self.inner.recv_bytes().await?;
            let bundle = Bundle::try_from(raw.as_ref())
                .map_err(|e| PathweaveError::Bundle(e.to_string()))?;

            let flags =
                BundleControlFlags::from_bits_truncate(bundle.primary.bundle_control_flags);

            if !flags.contains(BundleControlFlags::BUNDLE_IS_FRAGMENT) {
                let payload = bundle
                    .payload()
                    .ok_or_else(|| PathweaveError::Bundle("bundle missing payload".into()))?
                    .clone();
                return Ok(Bytes::from(payload));
            }

            // Fragment: accumulate until the bundle is complete.
            let seq = bundle.primary.creation_timestamp.seqno();
            let offset = bundle.primary.fragmentation_offset;
            let total_len = bundle.primary.total_data_length;
            let chunk = bundle
                .payload()
                .ok_or_else(|| PathweaveError::Bundle("fragment missing payload".into()))?
                .clone();

            let pending = self.pending.entry(seq).or_insert_with(|| PendingBundle {
                total_data_length: total_len,
                fragments: BTreeMap::new(),
            });
            pending.fragments.insert(offset, chunk);

            if pending.is_complete() {
                let complete = self.pending.remove(&seq).unwrap();
                return Ok(Bytes::from(complete.reassemble()));
            }
        }
    }

    async fn close(&mut self) -> Result<()> {
        self.inner.close().await
    }

    fn mtu(&self) -> usize {
        self.inner.mtu()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

    struct TestConn {
        tx: UnboundedSender<Bytes>,
        rx: UnboundedReceiver<Bytes>,
        mtu: usize,
    }

    fn conn_pair(mtu: usize) -> (TestConn, TestConn) {
        let (tx1, rx1) = unbounded_channel();
        let (tx2, rx2) = unbounded_channel();
        (
            TestConn { tx: tx1, rx: rx2, mtu },
            TestConn { tx: tx2, rx: rx1, mtu },
        )
    }

    #[async_trait]
    impl Connection for TestConn {
        async fn send_bytes(&mut self, bytes: &[u8]) -> crate::Result<()> {
            self.tx
                .send(Bytes::copy_from_slice(bytes))
                .map_err(|_| PathweaveError::Transport("channel closed".into()))
        }

        async fn recv_bytes(&mut self) -> crate::Result<Bytes> {
            self.rx
                .recv()
                .await
                .ok_or_else(|| PathweaveError::Transport("channel closed".into()))
        }

        async fn close(&mut self) -> crate::Result<()> {
            Ok(())
        }

        fn mtu(&self) -> usize {
            self.mtu
        }
    }

    #[tokio::test]
    async fn roundtrip_no_fragmentation() {
        let (conn_a, conn_b) = conn_pair(65535);
        let mut sender = BundleLayer::new(Box::new(conn_a));
        let mut receiver = BundleLayer::new(Box::new(conn_b));

        sender.send_bytes(b"hello bundle layer").await.unwrap();
        let received = receiver.recv_bytes().await.unwrap();
        assert_eq!(&received[..], b"hello bundle layer");
    }

    #[tokio::test]
    async fn fragmentation_and_reassembly() {
        // MTU of 256 forces fragmentation for a 1000-byte payload.
        let (conn_a, conn_b) = conn_pair(256);
        let mut sender = BundleLayer::new(Box::new(conn_a));
        let mut receiver = BundleLayer::new(Box::new(conn_b));

        let payload: Vec<u8> = (0u8..=255).cycle().take(1000).collect();
        sender.send_bytes(&payload).await.unwrap();
        let received = receiver.recv_bytes().await.unwrap();
        assert_eq!(&received[..], &payload[..]);
    }

    #[tokio::test]
    async fn multiple_messages_in_sequence() {
        let (conn_a, conn_b) = conn_pair(65535);
        let mut sender = BundleLayer::new(Box::new(conn_a));
        let mut receiver = BundleLayer::new(Box::new(conn_b));

        for i in 0u8..8 {
            let msg = vec![i; 64];
            sender.send_bytes(&msg).await.unwrap();
            let received = receiver.recv_bytes().await.unwrap();
            assert_eq!(&received[..], &msg[..]);
        }
    }
}
