use crate::ring::ConsistentHashRing;
use anyhow::Result;
use futures_util::stream::StreamExt;
use k8s_openapi::api::core::v1::Pod;
use kube::runtime::watcher::{Config, Event, watcher};
use kube::{Api, Client};
use parking_lot::RwLock;
use std::sync::Arc;

pub struct K8sMembership {
    client: Client,
    namespace: String,
    label_selector: String,
    port: u16,
}

impl K8sMembership {
    pub async fn new(label_selector: String, port: u16) -> Result<Self> {
        let client = Client::try_default().await?;
        let namespace = client.default_namespace().to_string();

        tracing::info!("Using namespace: {}", namespace);

        Ok(Self {
            client,
            namespace,
            label_selector,
            port,
        })
    }

    pub async fn watch_pods(self, ring: Arc<RwLock<ConsistentHashRing>>) -> Result<()> {
        let api: Api<Pod> = Api::namespaced(self.client, &self.namespace);
        let config = Config::default().labels(&self.label_selector);
        let mut watcher = std::pin::pin!(watcher(api, config));

        loop {
            let event = match watcher.next().await {
                Some(Ok(event)) => event,
                Some(Err(e)) => {
                    tracing::error!("Watcher error: {}", e);
                    continue;
                }
                None => break,
            };
            match event {
                Event::Apply(pod) => {
                    if let Some(ip) = pod.status.and_then(|s| s.pod_ip) {
                        let addr = format!("{}:{}", ip, self.port);
                        tracing::info!("Adding node: {}", addr);
                        ring.write().add_node(addr);
                    }
                }
                Event::Delete(pod) => {
                    if let Some(ip) = pod.status.and_then(|s| s.pod_ip) {
                        let addr = format!("{}:{}", ip, self.port);
                        tracing::info!("Removing node: {}", addr);
                        ring.write().remove_node(&addr);
                    }
                }
                Event::Init => {
                    tracing::info!("Watcher initialized");
                }
                Event::InitApply(pod) => {
                    if let Some(ip) = pod.status.and_then(|s| s.pod_ip) {
                        let addr = format!("{}:{}", ip, self.port);
                        tracing::info!("Init adding node: {}", addr);
                        ring.write().add_node(addr);
                    }
                }
                Event::InitDone => {
                    tracing::info!("Initial sync complete");
                }
            }
        }

        Ok(())
    }
}
