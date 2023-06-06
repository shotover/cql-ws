use cassandra_protocol::compression::Compression;
use cassandra_protocol::frame::message_query::BodyReqQuery;
use cassandra_protocol::frame::message_response::ResponseBody;
use cassandra_protocol::frame::message_result::{BodyResResultRows, ResResultBody};
use cassandra_protocol::frame::Envelope;
use cassandra_protocol::frame::Flags;
use cassandra_protocol::frame::Opcode;
use cassandra_protocol::frame::Version;
use cassandra_protocol::query::query_params::QueryParams;
use cassandra_protocol::types::cassandra_type::{wrapper_fn, CassandraType};
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
            match envelope.opcode {
                Opcode::Ready => println!("cql-ws: received: {:?}", envelope),
                Opcode::Authenticate => {
                    todo!();
                }
                _ => panic!("expected to receive a ready or authenticate message"),
            }
        } else {
            panic!("expected to receive a ready or authenticate message")
        }
    }

    pub async fn query(&mut self, query: &str) -> Vec<Vec<CassandraType>> {
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
            if let ResponseBody::Result(ResResultBody::Rows(BodyResResultRows {
                rows_content,
                metadata,
                ..
            })) = envelope.response_body().unwrap()
            {
                let mut result_values = vec![];
                for row in &rows_content {
                    let mut row_result_values = vec![];
                    for (i, col_spec) in metadata.col_specs.iter().enumerate() {
                        let wrapper = wrapper_fn(&col_spec.col_type.id);
                        let value = wrapper(&row[i], &col_spec.col_type, envelope.version).unwrap();

                        row_result_values.push(value);
                    }
                    result_values.push(row_result_values);
                }

                return result_values;
            }
        }

        panic!("expected to receive a result message")
    }
}
