use std::sync::Arc;

use anyhow::Result;
use futures_util::stream::StreamExt;
use k8s_openapi::api::core::v1::Pod;
use kube::{
    Api, Client,
    runtime::{
        WatchStreamExt,
        watcher::{Config, Event, watcher},
    },
};

use crate::ring::Straw2Router;

pub struct K8sMembership {
    client: Client,
    namespace: String,
    label_selector: String,
    port: u16,
}

struct PodInfo {
    name: String,
    ip: Option<String>,
    addr: Option<String>,
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
    let addr = ip.as_ref().map(|ip| format!("{ip}:{port}"));
    PodInfo {
        name,
        ip,
        addr,
        ready,
        terminating,
    }
}

/// A pod is eligible for the ring when it has an IP, is Ready, and not terminating.
fn eligible_addr(info: &PodInfo) -> Option<&str> {
    if info.ready && !info.terminating {
        info.addr.as_deref()
    } else {
        None
    }
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

    pub async fn watch_pods(self, ring: Arc<Straw2Router>) -> Result<()> {
        let api: Api<Pod> = Api::namespaced(self.client, &self.namespace);
        let config = Config::default().labels(&self.label_selector);
        let mut watcher = std::pin::pin!(watcher(api, config).default_backoff());
        let mut init_buf: Option<Vec<String>> = None;

        loop {
            let event = match watcher.next().await {
                Some(Ok(event)) => event,
                Some(Err(e)) => {
                    tracing::error!("Watcher error: {}", e);
                    continue;
                }
                None => return Ok(()),
            };
            match event {
                Event::Apply(pod) => {
                    let info = pod_info(&pod, self.port);
                    if let Some(addr) = eligible_addr(&info) {
                        tracing::info!(
                            pod = %info.name, ip = ?info.ip,
                            ready = info.ready, terminating = info.terminating,
                            action = "add", "Ring membership change"
                        );
                        ring.add_node(addr.to_owned());
                    } else if let Some(addr) = &info.addr {
                        tracing::info!(
                            pod = %info.name, ip = ?info.ip,
                            ready = info.ready, terminating = info.terminating,
                            action = "remove", "Ring membership change"
                        );
                        ring.remove_node(addr);
                    } else {
                        tracing::info!(
                            pod = %info.name, ip = ?info.ip,
                            ready = info.ready, terminating = info.terminating,
                            action = "skip", "No IP, skipping"
                        );
                    }
                }
                Event::Delete(pod) => {
                    let info = pod_info(&pod, self.port);
                    if let Some(addr) = &info.addr {
                        tracing::info!(
                            pod = %info.name, ip = ?info.ip,
                            ready = info.ready, terminating = info.terminating,
                            action = "remove", "Pod deleted"
                        );
                        ring.remove_node(addr);
                    }
                }
                Event::Init => {
                    tracing::info!("Watcher initialized, buffering initial sync");
                    init_buf = Some(Vec::new());
                }
                Event::InitApply(pod) => {
                    let info = pod_info(&pod, self.port);
                    if let Some(addr) = eligible_addr(&info) {
                        tracing::info!(
                            pod = %info.name, ip = ?info.ip,
                            ready = info.ready, terminating = info.terminating,
                            action = "init_add", "Init buffering eligible pod"
                        );
                        if let Some(buf) = &mut init_buf {
                            buf.push(addr.to_owned());
                        }
                    } else {
                        tracing::info!(
                            pod = %info.name, ip = ?info.ip,
                            ready = info.ready, terminating = info.terminating,
                            action = "init_skip", "Init skipping ineligible pod"
                        );
                    }
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
