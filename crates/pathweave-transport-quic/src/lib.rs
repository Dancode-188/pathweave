use async_trait::async_trait;
use futures::stream::BoxStream;
use pathweave_core::{
    Connection, PeerAnnouncement, Result, Transport, TransportCost, TransportKind,
};

pub struct QuicTransport;

#[async_trait]
impl Transport for QuicTransport {
    async fn start(&self) -> Result<()> {
        todo!()
    }

    async fn stop(&self) -> Result<()> {
        todo!()
    }

    fn discover(&self) -> BoxStream<'_, PeerAnnouncement> {
        todo!()
    }

    async fn connect(&self, _peer: &PeerAnnouncement) -> Result<Box<dyn Connection>> {
        todo!()
    }

    async fn accept(&self) -> Result<Box<dyn Connection>> {
        todo!()
    }

    fn mtu_hint(&self) -> usize {
        1200
    }

    fn cost(&self) -> TransportCost {
        TransportCost::Metered
    }

    fn kind(&self) -> TransportKind {
        TransportKind::Quic
    }

    fn name(&self) -> &'static str {
        "quic"
    }
}
