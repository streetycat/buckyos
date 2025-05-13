// Prevents additional console window on Windows in release, DO NOT REMOVE!!
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use std::fs::OpenOptions;
use std::process;

use buckyos_kit::{init_logging, BuckyOSMachineConfig};
use fs2::FileExt;
use name_client::init_name_lib;

#[cfg(windows)]
use std::os::windows::fs::OpenOptionsExt;

#[tokio::main]
async fn main() {
    init_logging("tray-controller", true);

    let mut file = OpenOptions::new();
    file.read(true).write(true).create(true);

    #[cfg(windows)]
    file.share_mode(0);

    let file = file
        .open(std::env::temp_dir().join("buckyos-tray-controller.lock"))
        .expect("open lock-file failed.");

    match file.try_lock_exclusive() {
        Ok(_) => {
            log::info!("buckyos tray-controller started.");

            let mut real_machine_config = BuckyOSMachineConfig::default();
            let machine_config = BuckyOSMachineConfig::load_machine_config();
            if machine_config.is_some() {
                real_machine_config = machine_config.unwrap();
            }

            init_name_lib(&real_machine_config.web3_bridge)
                .await
                .map_err(|err| {
                    log::error!("init default name client failed! {}", err);
                    return String::from("init default name client failed!");
                })
                .expect("init name-lib failed");

            app_lib::run();

            #[cfg(not(any(windows, target_os = "macos")))]
            log::error!("only for windows/macos.")
        }
        Err(_) => {
            log::info!("Another tray-controller is already running.");
            process::exit(1);
        }
    }
}
