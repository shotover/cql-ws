use cassandra_protocol::compression::Compression;
use cassandra_protocol::frame::message_query::BodyReqQuery;
use cassandra_protocol::frame::Envelope;
use cassandra_protocol::frame::Flags;
use cassandra_protocol::frame::Version;
use cassandra_protocol::query::query_params::QueryParams;
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc::unbounded_channel;
use tokio_tungstenite::tungstenite::Message;

pub struct Session {
    in_rx: tokio::sync::mpsc::UnboundedReceiver<Envelope>,
    out_tx: tokio::sync::mpsc::UnboundedSender<Envelope>,
}

impl Session {
    pub async fn new(address: &str) -> Self {
        let (ws_stream, _) = tokio_tungstenite::connect_async(address).await.unwrap();

        let (mut write, mut read) = ws_stream.split();

        let (in_tx, in_rx) = unbounded_channel::<Envelope>();
        let (out_tx, mut out_rx) = unbounded_channel::<Envelope>();

        // read task
        tokio::spawn(async move {
            loop {
                while let Some(message) = read.next().await {
                    let message = message.unwrap();
                    match message {
                        Message::Binary(data) => {
                            let envelope =
                                Envelope::from_buffer(&mut data.as_slice(), Compression::None)
                                    .unwrap()
                                    .envelope;
                            in_tx.send(envelope).unwrap();
                        }
                        _ => panic!("expected to receive a binary message"),
                    }
                }
            }
        });

        // write task
        tokio::spawn(async move {
            loop {
                while let Some(envelope) = out_rx.recv().await {
                    let data = envelope.encode_with(Compression::None).unwrap();
                    write.send(Message::Binary(data)).await.unwrap();
                }
            }
        });

        let mut session = Self { in_rx, out_tx };
        session.startup().await;
        session
    }

    async fn startup(&mut self) {
        let envelope = Envelope::new_req_startup(None, Version::V4);
        self.out_tx.send(envelope).unwrap();

        if let Some(envelope) = self.in_rx.recv().await {
            println!("cql-ws: received: {:?}", envelope)
        }
    }

    pub async fn query(&mut self, query: &str) {
        let envelope = Envelope::new_query(
            BodyReqQuery {
                query: query.into(),
                query_params: QueryParams::default(),
            },
            Flags::empty(),
            Version::V4,
        );

        self.out_tx.send(envelope).unwrap();

        if let Some(envelope) = self.in_rx.recv().await {
            println!("cql-ws: received: {:?}", envelope)
        }
    }
}
