use bytes::Bytes;

use crate::{
    peer_id_from_public_key, Connection, NodeIdentity, PathweaveError, PeerId, Result,
    NOISE_PARAMS, NOISE_XK_PARAMS,
};

// Maximum Noise protocol message size (spec limit).
const NOISE_MSG_MAX: usize = 65535;

// Protocol version prefix sent before the first Noise handshake message.
const PROTO_XX: u8 = 0x00;
const PROTO_XK: u8 = 0x01;

/// An authenticated, encrypted channel established via Noise_XX or Noise_XK.
///
/// Callers obtain a Session by completing a handshake through `initiate` or `respond`.
/// After construction the remote peer's identity is known and all traffic is encrypted.
pub struct Session {
    peer_id: PeerId,
    remote_static_key: [u8; 32],
    state: snow::TransportState,
    conn: Box<dyn Connection>,
}

impl Session {
    /// Performs the Noise handshake as the initiator over `conn`.
    ///
    /// Pass `remote_key = Some(key)` to use Noise_XK when the peer's static public key
    /// is already known (from the key registry). Pass `None` to use Noise_XX.
    /// A 1-byte protocol version prefix is sent before the first handshake message so
    /// the responder builds the matching HandshakeState. See ADR 020.
    pub async fn initiate(
        identity: &NodeIdentity,
        mut conn: Box<dyn Connection>,
        remote_key: Option<&[u8; 32]>,
    ) -> Result<Self> {
        let (prefix, mut state) = match remote_key {
            Some(key) => {
                let state = snow::Builder::new(
                    NOISE_XK_PARAMS
                        .parse()
                        .expect("hardcoded noise params are valid"),
                )
                .local_private_key(identity.private_key_bytes())
                .map_err(|e: snow::Error| PathweaveError::Session(e.to_string()))?
                .remote_public_key(key)
                .map_err(|e: snow::Error| PathweaveError::Session(e.to_string()))?
                .build_initiator()
                .map_err(|e: snow::Error| PathweaveError::Session(e.to_string()))?;
                (PROTO_XK, state)
            }
            None => {
                let state = snow::Builder::new(
                    NOISE_PARAMS
                        .parse()
                        .expect("hardcoded noise params are valid"),
                )
                .local_private_key(identity.private_key_bytes())
                .map_err(|e: snow::Error| PathweaveError::Session(e.to_string()))?
                .build_initiator()
                .map_err(|e: snow::Error| PathweaveError::Session(e.to_string()))?;
                (PROTO_XX, state)
            }
        };

        conn.send_bytes(&[prefix]).await?;

        let mut buf = vec![0u8; NOISE_MSG_MAX];

        // Message 1: -> e (XX) or -> e, es (XK)
        let n = state
            .write_message(&[], &mut buf)
            .map_err(|e: snow::Error| PathweaveError::Session(e.to_string()))?;
        conn.send_bytes(&buf[..n]).await?;

        // Message 2: <- e, ee, s, es (XX) or <- e, ee, se (XK)
        let msg = conn.recv_bytes().await?;
        state
            .read_message(&msg, &mut buf)
            .map_err(|e: snow::Error| PathweaveError::Session(e.to_string()))?;

        // Message 3: -> s, se (both XX and XK)
        let n = state
            .write_message(&[], &mut buf)
            .map_err(|e: snow::Error| PathweaveError::Session(e.to_string()))?;
        conn.send_bytes(&buf[..n]).await?;

        Self::finish(state, conn)
    }

    /// Performs the Noise handshake as the responder over `conn`.
    ///
    /// Reads the 1-byte protocol version prefix sent by the initiator and builds the
    /// matching HandshakeState (XX or XK). The three-message exchange is identical
    /// in both patterns. See ADR 020.
    pub async fn respond(identity: &NodeIdentity, mut conn: Box<dyn Connection>) -> Result<Self> {
        let prefix_msg = conn.recv_bytes().await?;
        let prefix = prefix_msg
            .first()
            .copied()
            .ok_or_else(|| PathweaveError::Session("missing protocol prefix byte".into()))?;

        let params_str = match prefix {
            PROTO_XX => NOISE_PARAMS,
            PROTO_XK => NOISE_XK_PARAMS,
            other => {
                return Err(PathweaveError::Session(format!(
                    "unknown protocol prefix: {other:#04x}"
                )));
            }
        };

        let mut state = snow::Builder::new(
            params_str
                .parse()
                .expect("hardcoded noise params are valid"),
        )
        .local_private_key(identity.private_key_bytes())
        .map_err(|e: snow::Error| PathweaveError::Session(e.to_string()))?
        .build_responder()
        .map_err(|e: snow::Error| PathweaveError::Session(e.to_string()))?;

        let mut buf = vec![0u8; NOISE_MSG_MAX];

        // Message 1: -> e (XX) or -> e, es (XK)
        let msg = conn.recv_bytes().await?;
        state
            .read_message(&msg, &mut buf)
            .map_err(|e: snow::Error| PathweaveError::Session(e.to_string()))?;

        // Message 2: <- e, ee, s, es (XX) or <- e, ee, se (XK)
        let n = state
            .write_message(&[], &mut buf)
            .map_err(|e: snow::Error| PathweaveError::Session(e.to_string()))?;
        conn.send_bytes(&buf[..n]).await?;

        // Message 3: -> s, se (both XX and XK)
        let msg = conn.recv_bytes().await?;
        state
            .read_message(&msg, &mut buf)
            .map_err(|e: snow::Error| PathweaveError::Session(e.to_string()))?;

        Self::finish(state, conn)
    }

    fn finish(state: snow::HandshakeState, conn: Box<dyn Connection>) -> Result<Self> {
        let raw = state.get_remote_static().ok_or_else(|| {
            PathweaveError::Session("remote static key unavailable after handshake".into())
        })?;
        let remote_static_key: [u8; 32] =
            raw.try_into().expect("Noise static key is always 32 bytes");
        let peer_id = peer_id_from_public_key(&remote_static_key);
        let transport = state
            .into_transport_mode()
            .map_err(|e| PathweaveError::Session(e.to_string()))?;
        Ok(Self {
            peer_id,
            remote_static_key,
            state: transport,
            conn,
        })
    }

    /// Encrypts `plaintext` and sends it over the session connection.
    pub async fn send(&mut self, plaintext: &[u8]) -> Result<()> {
        // ChaChaPoly AEAD overhead is 16 bytes.
        let mut ciphertext = vec![0u8; plaintext.len() + 16];
        let n = self
            .state
            .write_message(plaintext, &mut ciphertext)
            .map_err(|e: snow::Error| PathweaveError::Session(e.to_string()))?;
        self.conn.send_bytes(&ciphertext[..n]).await
    }

    /// Receives and decrypts one message from the session connection.
    pub async fn recv(&mut self) -> Result<Bytes> {
        let ciphertext = self.conn.recv_bytes().await?;
        let mut plaintext = vec![0u8; ciphertext.len()];
        let n = self
            .state
            .read_message(&ciphertext, &mut plaintext)
            .map_err(|e: snow::Error| PathweaveError::Session(e.to_string()))?;
        Ok(Bytes::copy_from_slice(&plaintext[..n]))
    }

    pub fn peer_id(&self) -> &PeerId {
        &self.peer_id
    }

    pub fn remote_static_key(&self) -> &[u8; 32] {
        &self.remote_static_key
    }

    pub async fn close(&mut self) -> Result<()> {
        self.conn.close().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{NodeIdentity, PathweaveError};
    use async_trait::async_trait;
    use bytes::Bytes;
    use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

    struct TestConn {
        tx: UnboundedSender<Bytes>,
        rx: UnboundedReceiver<Bytes>,
    }

    fn conn_pair() -> (TestConn, TestConn) {
        let (tx1, rx1) = unbounded_channel();
        let (tx2, rx2) = unbounded_channel();
        (TestConn { tx: tx1, rx: rx2 }, TestConn { tx: tx2, rx: rx1 })
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
            65535
        }
    }

    async fn handshaked_pair() -> (Session, Session) {
        let id_a = NodeIdentity::generate();
        let id_b = NodeIdentity::generate();
        let (conn_a, conn_b) = conn_pair();
        let (a, b) = tokio::join!(
            Session::initiate(&id_a, Box::new(conn_a), None),
            Session::respond(&id_b, Box::new(conn_b)),
        );
        (a.unwrap(), b.unwrap())
    }

    #[tokio::test]
    async fn handshake_reveals_correct_static_key() {
        let id_a = NodeIdentity::generate();
        let id_b = NodeIdentity::generate();
        let (conn_a, conn_b) = conn_pair();
        let (session_a, session_b) = tokio::join!(
            Session::initiate(&id_a, Box::new(conn_a), None),
            Session::respond(&id_b, Box::new(conn_b)),
        );
        let session_a = session_a.unwrap();
        let session_b = session_b.unwrap();
        assert_eq!(&session_a.remote_static_key()[..], id_b.public_key());
        assert_eq!(&session_b.remote_static_key()[..], id_a.public_key());
    }

    #[tokio::test]
    async fn handshake_completes_and_peer_ids_cross() {
        let id_a = NodeIdentity::generate();
        let id_b = NodeIdentity::generate();
        let (conn_a, conn_b) = conn_pair();
        let (session_a, session_b) = tokio::join!(
            Session::initiate(&id_a, Box::new(conn_a), None),
            Session::respond(&id_b, Box::new(conn_b)),
        );
        let session_a = session_a.unwrap();
        let session_b = session_b.unwrap();
        assert_eq!(session_a.peer_id(), id_b.peer_id());
        assert_eq!(session_b.peer_id(), id_a.peer_id());
    }

    #[tokio::test]
    async fn xk_handshake_succeeds_with_known_key() {
        let id_a = NodeIdentity::generate();
        let id_b = NodeIdentity::generate();
        let known_key: [u8; 32] = id_b.public_key().try_into().unwrap();
        let (conn_a, conn_b) = conn_pair();
        let (session_a, session_b) = tokio::join!(
            Session::initiate(&id_a, Box::new(conn_a), Some(&known_key)),
            Session::respond(&id_b, Box::new(conn_b)),
        );
        let session_a = session_a.unwrap();
        let session_b = session_b.unwrap();
        assert_eq!(session_a.peer_id(), id_b.peer_id());
        assert_eq!(session_b.peer_id(), id_a.peer_id());
        assert_eq!(&session_a.remote_static_key()[..], id_b.public_key());
        assert_eq!(&session_b.remote_static_key()[..], id_a.public_key());
    }

    #[tokio::test]
    async fn xk_with_wrong_key_returns_session_error() {
        let id_a = NodeIdentity::generate();
        let id_b = NodeIdentity::generate();
        let id_wrong = NodeIdentity::generate();
        let wrong_key: [u8; 32] = id_wrong.public_key().try_into().unwrap();
        let (conn_a, conn_b) = conn_pair();
        let (result_a, result_b) = tokio::join!(
            Session::initiate(&id_a, Box::new(conn_a), Some(&wrong_key)),
            Session::respond(&id_b, Box::new(conn_b)),
        );
        assert!(
            result_a.is_err() || result_b.is_err(),
            "XK handshake with wrong key must fail on at least one side"
        );
    }

    #[tokio::test]
    async fn send_recv_encrypts_and_decrypts() {
        let (mut a, mut b) = handshaked_pair().await;
        a.send(b"hello pathweave").await.unwrap();
        let received = b.recv().await.unwrap();
        assert_eq!(&received[..], b"hello pathweave");
    }

    #[tokio::test]
    async fn bidirectional_exchange_works() {
        let (mut a, mut b) = handshaked_pair().await;
        a.send(b"ping").await.unwrap();
        let msg = b.recv().await.unwrap();
        assert_eq!(&msg[..], b"ping");
        b.send(b"pong").await.unwrap();
        let reply = a.recv().await.unwrap();
        assert_eq!(&reply[..], b"pong");
    }

    #[tokio::test]
    async fn tampered_ciphertext_returns_error() {
        let id_a = NodeIdentity::generate();
        let id_b = NodeIdentity::generate();

        // Wire: a -> intercept_tx | intercept_rx -> b
        let (tx_a_to_b, mut intercept_rx) = unbounded_channel::<Bytes>();
        let (intercept_tx, rx_a_to_b) = unbounded_channel::<Bytes>();
        let (tx_b_to_a, rx_b_to_a) = unbounded_channel::<Bytes>();

        let conn_a = TestConn {
            tx: tx_a_to_b,
            rx: rx_b_to_a,
        };
        let conn_b = TestConn {
            tx: tx_b_to_a,
            rx: rx_a_to_b,
        };

        // Forward all handshake messages unmodified, then tamper the first transport message.
        // The initiator sends 3 messages before entering transport mode: the 1-byte protocol
        // prefix, Noise msg1, and Noise msg3. Count <= 3 passes all three through.
        let relay = tokio::spawn(async move {
            let mut count = 0usize;
            while let Some(msg) = intercept_rx.recv().await {
                count += 1;
                if count <= 3 {
                    // Protocol prefix, msg1, and msg3 from initiator: pass through.
                    intercept_tx.send(msg).ok();
                } else {
                    // First transport message: flip the first byte.
                    let mut tampered = msg.to_vec();
                    if !tampered.is_empty() {
                        tampered[0] ^= 0xff;
                    }
                    intercept_tx.send(Bytes::from(tampered)).ok();
                    break;
                }
            }
        });

        let (session_a, session_b) = tokio::join!(
            Session::initiate(&id_a, Box::new(conn_a), None),
            Session::respond(&id_b, Box::new(conn_b)),
        );
        let mut session_a = session_a.unwrap();
        let mut session_b = session_b.unwrap();

        session_a.send(b"secret").await.unwrap();
        relay.await.unwrap();
        let result = session_b.recv().await;
        assert!(
            matches!(result, Err(PathweaveError::Session(_))),
            "expected session error on tampered ciphertext, got: {result:?}"
        );
    }
}
