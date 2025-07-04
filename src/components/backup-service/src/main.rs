#![allow(unused, dead_code)]

mod backup_server;

use buckyos_api::*;
use std::fs::File;

use log::*;
use serde_json::*;

use buckyos_kit::*;
use cyfs_gateway_lib::WarpServerConfig;
use cyfs_warp::*;
use name_client::*;

use crate::backup_server::{BackupServer, BackupServerSetting};

#[tokio::main]
async fn main() {
    init_logging("backup_service", true);
    let mut runtime =
        init_buckyos_api_runtime("backup-service", None, BuckyOSRuntimeType::KernelService)
            .await
            .expect("init buckyos api runtime failed!");
    runtime
        .login()
        .await
        .expect("repo service login to system failed! err:{:?}");
    set_buckyos_api_runtime(runtime);
    let runtime = get_buckyos_api_runtime().expect("get buckyos api runtime failed!");

    let service_settings = runtime
        .get_my_settings()
        .await
        .expect("repo service settings not found!");

    let service_settings: BackupServerSetting =
        serde_json::from_value(service_settings).expect("backup service settings parse error!");

    let server_data_folder = runtime.get_data_folder();
    // 确保server_data_folder目录存在
    if !server_data_folder.exists() {
        std::fs::create_dir_all(&server_data_folder).expect(&format!(
            "Failed to create server_data_folder: {:?}",
            server_data_folder
        ));
        info!("Created repo_server_data_folder: {:?}", server_data_folder);
    }

    let server = BackupServer::new(service_settings)
        .await
        .expect("backup service init error!");

    server
        .init_check()
        .await
        .expect("backup service init check failed!");
    info!("backup service init check OK.");

    register_inner_service_builder("backup-server", move || Box::new(server.clone())).await;
    let server_config = json!({
      "http_port":4000,//TODO：服务的端口分配和管理
      "tls_port":0,
      "hosts": {
        "*": {
          "enable_cors":true,
          "routes": {
            "/kapi/backup-service" : {
                "inner_service":"backup-server"
            }
          }
        }
      }
    });

    let server_config: WarpServerConfig = serde_json::from_value(server_config).unwrap();
    //start!
    info!("Start Backup Service...");
    start_cyfs_warp_server(server_config).await;

    let _ = tokio::signal::ctrl_c().await;
}
