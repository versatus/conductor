use tokio::net::TcpStream;
use tokio::io::AsyncWriteExt;

use crate::broker::{HEADER_SIZE, TOPIC_SIZE_OFFSET};

pub struct Publisher {
    stream: TcpStream,
}

impl Publisher {
    pub async fn new(uri: &str) -> std::io::Result<Self> {
        let stream = TcpStream::connect(uri).await?;
        Ok(Self { stream })
    }

    pub async fn publish(&mut self, topic: String, msg: &str) -> std::io::Result<()> {
        let topic_len = topic.len();
        let topic_len_bytes = topic_len.to_be_bytes();
        let message_len = msg.len();
        let message_len_bytes = message_len.to_be_bytes();

        let total_len = HEADER_SIZE + TOPIC_SIZE_OFFSET + topic_len + message_len;
        let mut full_message = Vec::with_capacity(total_len);
        full_message.extend_from_slice(&message_len_bytes);
        full_message.extend_from_slice(&topic_len_bytes);
        full_message.extend_from_slice(topic.as_bytes());
        full_message.extend_from_slice(msg.as_bytes());
        self.stream.write_all(&full_message).await?;

        Ok(())
    }
}
