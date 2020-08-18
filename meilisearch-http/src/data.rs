use std::error::Error;
use std::ops::Deref;
use std::sync::Arc;

use meilisearch_core::{Database, DatabaseOptions};
use sha2::Digest;
use raft::Store;
use serde::{Serialize, Deserialize};
use bincode::deserialize;
use async_trait::async_trait;

use crate::index_update_callback;
use crate::option::Opt;
use crate::routes::document::{update_multiple_documents, UpdateDocumentsQuery};
use crate::routes::index::{ IndexCreateRequest, create_index };

#[derive(Clone)]
pub struct Data {
    inner: Arc<DataInner>,
}

#[derive(Serialize, Deserialize)]
pub enum Message {
    DocumentAddition { index: String, addition: String, partial: bool },
    IndexCreation { data: IndexCreateRequest },
}

#[async_trait]
impl Store for Data {
    async fn apply(&mut self, message: &[u8]) -> raft::Result<Vec<u8>> {
        let message: Message = deserialize(message).unwrap();
        println!("here");
        match message {
            Message::DocumentAddition { index, addition, partial } => {
                let update = UpdateDocumentsQuery { primary_key: None };
                let addition: serde_json::Value = serde_json::from_str(&addition).unwrap();
                let addition = addition
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|v| v
                        .as_object()
                        .unwrap()
                        .iter()
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect())
                    .collect();
                let response = update_multiple_documents(self, &index, update, addition, partial).await;
                println!("response: {:?}", response);

                Ok(vec![])
            }
            Message::IndexCreation { data } => {
                let response = create_index(self, data).await;
                println!("response: {:?}", response);
                Ok(vec![])
            }
        }
    }

    async fn snapshot(&self) -> raft::Result<Vec<u8>> {
        Ok(vec![])
    }

    async fn restore(&mut self, _snapshot: &[u8]) -> raft::Result<()> {
        Ok(())
    }
}

impl Deref for Data {
    type Target = DataInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[derive(Clone)]
pub struct DataInner {
    pub db: Arc<Database>,
    pub db_path: String,
    pub api_keys: ApiKeys,
    pub server_pid: u32,
    pub http_payload_size_limit: usize,
}

#[derive(Clone)]
pub struct ApiKeys {
    pub public: Option<String>,
    pub private: Option<String>,
    pub master: Option<String>,
}

impl ApiKeys {
    pub fn generate_missing_api_keys(&mut self) {
        if let Some(master_key) = &self.master {
            if self.private.is_none() {
                let key = format!("{}-private", master_key);
                let sha = sha2::Sha256::digest(key.as_bytes());
                self.private = Some(format!("{:x}", sha));
            }
            if self.public.is_none() {
                let key = format!("{}-public", master_key);
                let sha = sha2::Sha256::digest(key.as_bytes());
                self.public = Some(format!("{:x}", sha));
            }
        }
    }
}

impl Data {
    pub fn new(opt: Opt) -> Result<Data, Box<dyn Error>> {
        let db_path = opt.db_path.clone();
        let server_pid = std::process::id();

        let db_opt = DatabaseOptions {
            main_map_size: opt.main_map_size,
            update_map_size: opt.update_map_size,
        };

        let http_payload_size_limit = opt.http_payload_size_limit;

        let db = Arc::new(Database::open_or_create(opt.db_path, db_opt)?);

        let mut api_keys = ApiKeys {
            master: opt.master_key,
            private: None,
            public: None,
        };

        api_keys.generate_missing_api_keys();

        let inner_data = DataInner {
            db: db.clone(),
            db_path,
            api_keys,
            server_pid,
            http_payload_size_limit,
        };

        let data = Data {
            inner: Arc::new(inner_data),
        };

        let callback_context = data.clone();
        db.set_update_callback(Box::new(move |index_uid, status| {
            index_update_callback(&index_uid, &callback_context, status);
        }));

        Ok(data)
    }
}
