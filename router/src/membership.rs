use std::net::SocketAddr;
use std::sync::Arc;

use futures_util::stream::StreamExt;
use k8s_openapi::api::core::v1::Pod;
use kube::{
    Api, Client,
    runtime::{
        WatchStreamExt,
        watcher::{Config, Event, watcher},
    },
};
use tokio_util::sync::CancellationToken;

use crate::ring::{Node, Straw2Router};

pub struct K8sMembership {
    client: Client,
    namespace: String,
    label_selector: String,
    port: u16,
}

struct PodInfo {
    name: String,
    ip: Option<String>,
    addr: Option<SocketAddr>,
    ready: bool,
    terminating: bool,
}

fn pod_info(pod: &Pod, port: u16) -> PodInfo {
    let name = pod
        .metadata
        .name
        .clone()
        .unwrap_or_else(|| "<unknown>".into());
    let terminating = pod.metadata.deletion_timestamp.is_some();
    let ip = pod.status.as_ref().and_then(|s| s.pod_ip.clone());
    let ready = pod
        .status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .and_then(|conds| {
            conds
                .iter()
                .find(|c| c.type_ == "Ready")
                .map(|c| c.status == "True")
        })
        .unwrap_or(false);
    let addr = ip
        .as_ref()
        .and_then(|ip| format!("{ip}:{port}").parse().ok());
    PodInfo {
        name,
        ip,
        addr,
        ready,
        terminating,
    }
}

/// A pod is eligible for the ring when it has an IP, is Ready, and not terminating.
fn eligible(info: &PodInfo) -> Option<Node> {
    if info.ready && !info.terminating {
        Some(Node {
            pod_name: info.name.clone(),
            ip_addr: info.addr?,
        })
    } else {
        None
    }
}

impl K8sMembership {
    pub async fn new(label_selector: String, port: u16) -> Result<Self, kube::Error> {
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

    pub async fn watch_pods(self, ring: Arc<Straw2Router>, cancel: CancellationToken) {
        let api: Api<Pod> = Api::namespaced(self.client, &self.namespace);
        let config = Config::default().labels(&self.label_selector);
        let mut watcher = std::pin::pin!(watcher(api, config).default_backoff());
        let mut init_buf: Option<Vec<Node>> = None;

        loop {
            let event = tokio::select! {
                ev = watcher.next() => match ev {
                    Some(Ok(event)) => event,
                    Some(Err(e)) => {
                        tracing::error!("Watcher error: {}", e);
                        continue;
                    }
                    None => return,
                },
                _ = cancel.cancelled() => return,
            };
            match event {
                Event::Apply(pod) | Event::InitApply(pod) => {
                    let info = pod_info(&pod, self.port);
                    let (action, msg) = if let Some(node) = eligible(&info) {
                        if let Some(buf) = init_buf.as_mut() {
                            buf.push(node);
                            ("init_add", "Buffering eligible pod")
                        } else {
                            ring.add_node(node);
                            ("add", "Ring membership change")
                        }
                    } else {
                        if init_buf.is_none() {
                            ring.remove_node(&info.name);
                        }
                        ("remove", "Ring membership change")
                    };
                    tracing::info!(
                        pod = %info.name, ip = ?info.ip,
                        ready = info.ready, terminating = info.terminating,
                        action, msg,
                    );
                }
                Event::Delete(pod) => {
                    let info = pod_info(&pod, self.port);
                    tracing::info!(
                        pod = %info.name, ip = ?info.ip,
                        action = "remove", "Pod deleted"
                    );
                    ring.remove_node(&info.name);
                }
                Event::Init => {
                    tracing::info!("Watcher initialized, buffering initial sync");
                    init_buf = Some(Vec::new());
                }
                Event::InitDone => {
                    if let Some(buf) = init_buf.take() {
                        tracing::info!("Initial sync complete, {} servers", buf.len());
                        ring.set_servers(buf);
                    }
                }
            }
        }
    }
}
