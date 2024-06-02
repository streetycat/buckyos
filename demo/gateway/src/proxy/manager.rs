use super::forward::{ForwardProxyConfig, ForwardProxyProtocol, TcpForwardProxy};
use super::socks5::{ProxyConfig, Socks5Proxy};
use crate::error::{GatewayError, GatewayResult};
use crate::peer::{NameManagerRef, PeerManagerRef};

use std::sync::{Arc, Mutex};

enum ProxyService {
    Socks5(Socks5Proxy),
    TcpForward(TcpForwardProxy),
}

pub struct ProxyManager {
    name_manager: NameManagerRef,
    peer_manager: PeerManagerRef,
    socks5_proxy: Arc<Mutex<Vec<Socks5Proxy>>>,
    tcp_forward_proxy: Arc<Mutex<Vec<TcpForwardProxy>>>,
}

impl ProxyManager {
    pub fn new(name_manager: NameManagerRef, peer_manager: PeerManagerRef) -> Self {
        Self {
            name_manager,
            peer_manager,
            socks5_proxy: Arc::new(Mutex::new(Vec::new())),
            tcp_forward_proxy: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /*
    load proxy from json config node as follows:
    {
        block: "proxy",
        id: "proxy_id",
        type: "socks5",
        addr: "127.0.0.1",
        port: 8000,
        auth: {
            type: "password",
            username: "user",
            password: "password"
        }
    }

    or

    {
        block: "proxy",
        id: "proxy_id",
        type: "forward",
        protocol: "tcp",
        target_device: "device_id",
        target_port: 8000,
    }
     */
    pub fn load_proxy(&self, json: &serde_json::Value) -> GatewayResult<()> {
        let proxy_type = json["type"].as_str().unwrap();
        match proxy_type {
            "socks5" => {
                let config = ProxyConfig::load(json)?;

                self.add_socks5_proxy(config)?;
            }
            "forward" => {
                let service = ForwardProxyConfig::load(json)?;
                match service.protocol {
                    ForwardProxyProtocol::Tcp => {
                        self.add_tcp_forward_proxy(service);
                    }
                    ForwardProxyProtocol::Udp => {
                        unimplemented!("UDP forward proxy not implemented yet");
                    }
                }
            }
            _ => {
                warn!("Unknown proxy type: {}", proxy_type);
            }
        }

        Ok(())
    }

    fn get_proxy(&self, id: &str) -> Option<ProxyService> {
        {
            let socks_proxys = self.socks5_proxy.lock().unwrap();
            for p in &*socks_proxys {
                if p.id() == id {
                    return Some(ProxyService::Socks5(p.clone()));
                }
            }
        }

        {
            let forward_proxys = self.tcp_forward_proxy.lock().unwrap();
            for p in &*forward_proxys {
                if p.id() == id {
                    return Some(ProxyService::TcpForward(p.clone()));
                }
            }
        }

        None
    }

    fn check_exist(&self, id: &str) -> bool {
        self.get_proxy(id).is_some()
    }

    fn add_socks5_proxy(&self, config: ProxyConfig) -> GatewayResult<()> {
        let mut socks_proxys = self.socks5_proxy.lock().unwrap();

        // Check id duplication
        if self.check_exist(&config.id) {
            let msg = format!("Duplicated socks5 proxy id: {}", config.id);
            warn!("{}", msg);
            return Err(GatewayError::AlreadyExists(msg.to_owned()));
        }

        let proxy = Socks5Proxy::new(config, self.name_manager.clone(), self.peer_manager.clone());
        socks_proxys.push(proxy);

        Ok(())
    }

    fn add_tcp_forward_proxy(&self, config: ForwardProxyConfig) {
        let mut forward_proxys = self.tcp_forward_proxy.lock().unwrap();

        // Check id duplication
        for p in &*forward_proxys {
            if p.id() == config.id {
                let msg = format!("Duplicated forward proxy id: {}", config.id);
                warn!("{}", msg);
                return;
            }
        }

        let proxy =
            TcpForwardProxy::new(config, self.name_manager.clone(), self.peer_manager.clone());
        forward_proxys.push(proxy);
    }

    pub async fn start(&self) -> GatewayResult<()> {
        let proxy_list = self.socks5_proxy.lock().unwrap().clone();
        for proxy in &proxy_list {
            if let Err(e) = proxy.start().await {
                return Err(e);
            }
        }

        let proxy_list = self.tcp_forward_proxy.lock().unwrap().clone();
        for proxy in &proxy_list {
            if let Err(e) = proxy.start().await {
                return Err(e);
            }
        }

        Ok(())
    }

    pub async fn stop(&self) -> GatewayResult<()> {
        let proxy_list = self.socks5_proxy.lock().unwrap().clone();
        for proxy in &proxy_list {
            if let Err(e) = proxy.stop().await {
                return Err(e);
            }
        }

        Ok(())
    }
}

pub type ProxyManagerRef = Arc<ProxyManager>;
