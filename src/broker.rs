#![allow(unreachable_code)]
use tokio::io::AsyncReadExt;
use tokio::net::{TcpStream, TcpListener};
use tokio::sync::mpsc;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use crate::util::{handle_new_subscription, handle_message_parsing};

pub struct Broker {
    frontend: Arc<TcpListener>,
    backend: Arc<TcpListener>,
    topics: Arc<Mutex<HashMap<String, Vec<Arc<Mutex<TcpStream>>>>>>
}

impl Broker {
    pub async fn new(frontend_uri: &str, backend_uri: &str) -> std::io::Result<Self> {
        let frontend = Arc::new(TcpListener::bind(frontend_uri).await?);
        let backend = Arc::new(TcpListener::bind(backend_uri).await?);
        Ok(Self {
            frontend,
            backend,
            topics: Arc::new(Mutex::new(HashMap::new()))
        })
    }

    pub async fn start(&self) -> std::io::Result<()> {

        let (tx, mut rx) = mpsc::channel(1024);

        let frontend = self.frontend.clone();
        let backend = self.backend.clone();
        let subscriptions = self.topics.clone();
        let frontend_task = tokio::spawn(async move {
            loop {
                let (mut stream, _) = frontend.accept().await?;
                let tx = tx.clone();
                tokio::spawn(async move {
                    loop {
                        let mut buffer = [0; 1024];
                        let n = stream.read(&mut buffer).await?;
                        if n == 0 {
                            break;
                        }
                        if tx.send(buffer[..n].to_vec()).await.is_err() {
                            eprintln!("Channel closed");
                            break;
                        }
                    }

                    Ok::<(), std::io::Error>(())
                });
            }

            Ok::<(), std::io::Error>(())
        });

        let backend_task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    Some(mut msg) = rx.recv() => {
                        handle_message_parsing(&mut msg, subscriptions.clone()).await?;
                    }

                    Ok((stream, _)) = backend.accept() => {
                        handle_new_subscription(subscriptions.clone(), stream).await?;
                    }
                }
            }

            Ok::<(), std::io::Error>(())
        });

        let _ = tokio::join!(frontend_task, backend_task);

        Ok(())
    }
}

