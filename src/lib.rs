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
use tokio_tungstenite::tungstenite::error::Error;
use tokio_tungstenite::tungstenite::error::ProtocolError;
use tokio_tungstenite::tungstenite::handshake::client::generate_key;
use tokio_tungstenite::tungstenite::handshake::server::Request;
use tokio_tungstenite::tungstenite::Message;

pub struct Session {
    in_rx: tokio::sync::mpsc::UnboundedReceiver<Message>,
    out_tx: tokio::sync::mpsc::UnboundedSender<Message>,
}

impl Session {
    fn construct_request(uri: &str) -> Request {
        let uri = uri.parse::<http::Uri>().unwrap();

        let authority = uri.authority().unwrap().as_str();
        let host = authority
            .find('@')
            .map(|idx| authority.split_at(idx + 1).1)
            .unwrap_or_else(|| authority);

        if host.is_empty() {
            panic!("Empty host name");
        }

        http::Request::builder()
            .method("GET")
            .header("Host", host)
            .header("Connection", "Upgrade")
            .header("Upgrade", "websocket")
            .header("Sec-WebSocket-Version", "13")
            .header("Sec-WebSocket-Key", generate_key())
            .header(
                "Sec-WebSocket-Protocol",
                "cql".parse::<http::HeaderValue>().unwrap(),
            )
            .uri(uri)
            .body(())
            .unwrap()
    }

    pub async fn new(address: &str) -> Self {
        let (ws_stream, _) = tokio_tungstenite::connect_async(Self::construct_request(address))
            .await
            .unwrap();

        let (mut write, mut read) = ws_stream.split();

        let (in_tx, in_rx) = unbounded_channel::<Message>();
        let (out_tx, mut out_rx) = unbounded_channel::<Message>();

        // read task
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    result = read.next() => {
                        if let Some(message) = result {
                            match message {
                                Ok(ws_message @ Message::Binary(_)) => {
                                    in_tx.send(ws_message).unwrap();
                                }
                                Ok(Message::Close(_)) => {
                                    return;
                                }
                                Ok(_) => panic!("expected to recieve a binary message"),
                                Err(err) => panic!("{err}")
                            }
                        }
                    }
                    _ = in_tx.closed() => {
                        return;
                    }
                }
            }
        });

        // write task
        tokio::spawn(async move {
            loop {
                if let Some(ws_message) = out_rx.recv().await {
                    write.send(ws_message).await.unwrap();
                } else {
                    match write.send(Message::Close(None)).await {
                        Ok(_) => {}
                        Err(Error::Protocol(ProtocolError::SendAfterClosing)) => {}
                        Err(err) => panic!("{err}"),
                    }
                    break;
                }
            }
        });

        let mut session = Self { in_rx, out_tx };
        session.startup().await;
        session
    }

    async fn startup(&mut self) {
        let envelope = Envelope::new_req_startup(None, Version::V4);
        self.out_tx.send(Self::encode(envelope)).unwrap();

        let envelope = Self::decode(self.in_rx.recv().await.unwrap());

        match envelope.opcode {
            Opcode::Ready => println!("cql-ws: received: {:?}", envelope),
            Opcode::Authenticate => {
                todo!();
            }
            _ => panic!("expected to receive a ready or authenticate message"),
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

        self.out_tx.send(Self::encode(envelope)).unwrap();

        let envelope = Self::decode(self.in_rx.recv().await.unwrap());

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

            result_values
        } else {
            panic!("unexpected to recieve a result envelope");
        }
    }

    fn encode(envelope: Envelope) -> Message {
        let data = envelope.encode_with(Compression::None).unwrap();
        Message::Binary(data)
    }

    fn decode(ws_message: Message) -> Envelope {
        match ws_message {
            Message::Binary(data) => {
                Envelope::from_buffer(data.as_slice(), Compression::None)
                    .unwrap()
                    .envelope
            }
            _ => panic!("expected to receive a binary message"),
        }
    }

    pub async fn send_raw_ws_message(&mut self, ws_message: Message) {
        self.out_tx.send(ws_message).unwrap();
    }

    pub async fn wait_for_raw_ws_message_resp(&mut self) -> Message {
        self.in_rx.recv().await.unwrap()
    }

    // pub async fn query(&mut self, query: &str) -> Vec<Vec<CassandraType>> {
    //     let envelope = Envelope::new_query(
    //         BodyReqQuery {
    //             query: query.into(),
    //             query_params: QueryParams::default(),
    //         },
    //         Flags::empty(),
    //         Version::V4,
    //     );
    //
    //     self.out_tx.send(envelope).unwrap();
    //
    //     match tokio::time::timeout(tokio::time::Duration::from_millis(5000), self.in_rx.recv()).await
    //     {
    //         Ok(Some(envelope)) => {
    //             if let ResponseBody::Result(ResResultBody::Rows(BodyResResultRows {
    //                 rows_content,
    //                 metadata,
    //                 ..
    //             })) = envelope.response_body().unwrap()
    //             {
    //                 let mut result_values = vec![];
    //                 for row in &rows_content {
    //                     let mut row_result_values = vec![];
    //                     for (i, col_spec) in metadata.col_specs.iter().enumerate() {
    //                         let wrapper = wrapper_fn(&col_spec.col_type.id);
    //                         let value =
    //                             wrapper(&row[i], &col_spec.col_type, envelope.version).unwrap();
    //
    //                         row_result_values.push(value);
    //                     }
    //                     result_values.push(row_result_values);
    //                 }
    //
    //                 return result_values;
    //             }
    //         }
    //         Ok(None) => panic!("no envelope"),
    //         Err(_) => panic!("Timeout!"),
    //     }
    //
    //     panic!("expected to receive a result message")
    // }
}
