// native
// #[cfg(any(windows, target_os = "macos"))]
// extern "C" {
//     fn entry();
// }

use std::sync::Arc;
use tokio::sync::Mutex;

use ffi_extern::*;
use tauri::{
    menu::{Menu, MenuBuilder, SubmenuBuilder},
    tray::{MouseButton, MouseButtonState, TrayIconEvent},
    AppHandle, Emitter, EventLoopMessage, Manager, Wry,
};

mod ffi_extern;

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    // entry();

    entry_rust();
}

fn entry_rust() {
    struct TrayMenuState {
        click_seq: u32,
        popup_seq: u32,
        app_list_seq: u32,
        bucky_status: BuckyStatus,
        app_list: Vec<ApplicationInfoRust>,
    }

    #[derive(Clone)]
    struct AppState {
        tray_icon: tauri::tray::TrayIcon,
        bucky_status: Arc<Mutex<BuckyStatus>>,
        menu_state: Arc<Mutex<TrayMenuState>>,
    }

    const ICON_RUNNING: &[u8] = include_bytes!("tray_app.ico");
    const ICON_STOPPED: &[u8] = include_bytes!("pause.ico");
    const ICON_FAILED: &[u8] = include_bytes!("error.ico");

    fn scan_buckyos_status(app_handle: AppHandle) {
        tokio::task::spawn(async move {
            let app_state = app_handle.state::<AppState>();
            let mut status = BuckyStatus::Stopped;
            let mut interval = std::time::Duration::from_millis(1);
            loop {
                let old_status = status;
                status = get_bucky_status().await;

                if status != old_status {
                    let icon = match status {
                        BuckyStatus::Running => ICON_RUNNING,
                        BuckyStatus::Stopped => ICON_STOPPED,
                        BuckyStatus::NotActive => ICON_FAILED,
                        BuckyStatus::NotInstall => ICON_FAILED,
                        BuckyStatus::Failed => ICON_FAILED,
                    };

                    app_state
                        .tray_icon
                        .set_icon(Some(
                            tauri::image::Image::from_bytes(icon)
                                .expect("load icon from file failed."),
                        ))
                        .expect("update icon failed.");

                    match status {
                        BuckyStatus::NotInstall | BuckyStatus::NotActive | BuckyStatus::Stopped => {
                            interval = std::time::Duration::from_millis(5000)
                        }
                        BuckyStatus::Running | BuckyStatus::Failed => {
                            interval = std::time::Duration::from_millis(500)
                        }
                    }

                    *app_state.bucky_status.lock().await = status;
                }

                tokio::time::sleep(interval).await;
            }
        });
    }

    fn scan_app_list_for_menu(app_handle: AppHandle) {
        let app_state = app_handle.state::<AppState>().inner().clone();
        tokio::task::spawn(async move {
            loop {
                let click_seq = {
                    let mut menu_state = app_state.menu_state.lock().await;
                    menu_state.click_seq += 1;
                    menu_state.click_seq
                };

                // query app list
                let is_app_update = match tokio::time::timeout(
                    std::time::Duration::from_millis(500),
                    list_application_rust(),
                )
                .await
                {
                    Ok(result_apps) => match result_apps {
                        Ok(mut result_apps) => {
                            let mut menu_state = app_state.menu_state.lock().await;
                            if menu_state.app_list_seq < click_seq {
                                menu_state.app_list_seq = click_seq;
                                let mut is_app_update =
                                    result_apps.len() != menu_state.app_list.len();
                                if !is_app_update {
                                    result_apps.sort_by_key(|app| app.id.clone());
                                    is_app_update = result_apps
                                        .iter()
                                        .zip(menu_state.app_list.iter())
                                        .position(|(new_app, old_app)| {
                                            log::debug!(
                                                "is_running: {}->{}, id: {}->{}",
                                                new_app.is_running,
                                                old_app.is_running,
                                                new_app.id,
                                                old_app.id
                                            );
                                            new_app.is_running != old_app.is_running
                                                || new_app.id != old_app.id
                                        })
                                        .is_some();
                                }
                                if is_app_update {
                                    std::mem::swap(&mut menu_state.app_list, &mut result_apps);
                                }
                                is_app_update
                            } else {
                                false
                            }
                        }
                        Err(err) => {
                            log::error!("{}", err);
                            false
                        }
                    },
                    Err(err) => {
                        log::error!("{}", err);
                        false
                    }
                };

                {
                    let new_bucky_status = *app_state.bucky_status.lock().await;
                    let old_bucky_status = app_state.menu_state.lock().await.bucky_status;
                    if !is_app_update && new_bucky_status == old_bucky_status {
                        log::debug!(
                            "bucky_status: {:?} -> {:?}",
                            old_bucky_status,
                            new_bucky_status
                        );
                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                        continue;
                    }
                }

                let menu = {
                    let bucky_status = { *app_state.bucky_status.lock().await };
                    let mut menu_state = app_state.menu_state.lock().await;
                    if menu_state.popup_seq < click_seq {
                        menu_state.popup_seq = click_seq;

                        // create menu
                        let mut builder =
                            MenuBuilder::new(&app_handle).text("homepage", "Home page");
                        let app_list = menu_state.app_list.clone();

                        for i in 0..menu_state.app_list.len() {
                            let app = menu_state.app_list.get(i).unwrap();
                            let app_sub_menu = SubmenuBuilder::new(&app_handle, app.name.as_str())
                                .text(format!("app:homepage:{}", i), "Home page")
                                .text(
                                    format!("app:control:{}", i),
                                    if app.is_running { "Stop" } else { "Start" },
                                )
                                .build()
                                .expect("build app submenu failed");
                            builder = builder.item(&app_sub_menu);
                        }
                        let menu = builder
                            .text(
                                "control",
                                match bucky_status {
                                    BuckyStatus::Running => "Stop",
                                    BuckyStatus::Stopped => "Start",
                                    BuckyStatus::NotActive => "Active",
                                    BuckyStatus::NotInstall => "Install",
                                    BuckyStatus::Failed => "Stop",
                                },
                            )
                            .text("about", "About")
                            .text("exit", "Exit")
                            .build()
                            .expect("build tray menu failed");
                        menu_state.bucky_status = bucky_status;
                        Some((menu, app_list))
                    } else {
                        None
                    }
                };

                if let Some((menu, app_list)) = menu {
                    log::info!("will popup menu");
                    let _ = app_state
                        .tray_icon
                        .set_menu(Some(menu))
                        .expect("set menu failed");
                    let app_state = app_state.clone();
                    app_state.tray_icon.on_menu_event(move |app_handle, event| {
                        match event.id().0.as_str() {
                            "homepage" => {
                                tokio::task::spawn(async move {
                                    let mut homepage = ACTIVE_PAGE_URL;
                                    let node_info = get_node_info_rust().await;
                                    if let Some(node_info) = node_info.as_ref() {
                                        homepage = node_info.home_page_url.as_str();
                                    }
                                    webbrowser::open(homepage).map_err(|err| {
                                        log::error!(
                                            "open home page failed: {}, {:?}",
                                            homepage,
                                            err
                                        )
                                    });
                                });
                            }
                            "control" => {
                                let bucky_status = app_state.bucky_status.clone();
                                tokio::task::spawn(async move {
                                    let bucky_status = { *bucky_status.lock().await };
                                    match bucky_status {
                                        BuckyStatus::Running => stop_buckyos(),
                                        BuckyStatus::Stopped => start_buckyos(),
                                        BuckyStatus::NotActive => {
                                            webbrowser::open(ACTIVE_PAGE_URL).map_err(|err| {
                                                log::error!(
                                                    "open home page failed: {}, {:?}",
                                                    ACTIVE_PAGE_URL,
                                                    err
                                                )
                                            });
                                        }
                                        BuckyStatus::NotInstall => unimplemented!("Install"),
                                        BuckyStatus::Failed => stop_buckyos(),
                                    }
                                });
                            }
                            "about" => {
                                /*app_handle.dialog().message()*/
                                unimplemented!("About")
                            }
                            "exit" => {
                                app_handle.exit(0);
                            }
                            _ => {
                                let app_cmd = event.id().0.split(":").collect::<Vec<_>>();
                                let app_idx: usize = app_cmd
                                    .get(2)
                                    .expect("invalid app command")
                                    .parse()
                                    .expect("invalid app index format");
                                let app_info = app_list.get(app_idx).expect("invalid app index");

                                match app_cmd.as_slice() {
                                    ["app", "homepage", _] => {
                                        webbrowser::open(app_info.home_page_url.as_str());
                                    }
                                    ["app", "control", _] => {
                                        let is_running = app_info.is_running;
                                        let app_id = app_info.id.clone();
                                        tokio::task::spawn(async move {
                                            if is_running {
                                                stop_app_rust(app_id.as_str()).await;
                                            } else {
                                                start_app_rust(app_id.as_str()).await;
                                            }
                                        });
                                    }
                                    _ => unreachable!("invalid app command"),
                                }
                            }
                        }
                    });
                }

                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        });
    }

    tauri::Builder::default()
        .setup(|app| {
            let tray = {
                let app_handle = app.app_handle().clone();
                tauri::tray::TrayIconBuilder::new()
                    // .on_tray_icon_event(move |_, event| match event {
                    //     TrayIconEvent::Click {
                    //         button: MouseButton::Right,
                    //         ..
                    //     } => {
                    //         log::info!("tray icon clicked.");
                    //         popup_menu(app_handle.clone());
                    //     }
                    //     _ => println!("Clicked: {:?}", event.id()),
                    // })
                    .build(app)?
            };

            tray.set_show_menu_on_left_click(false);

            let app_state = AppState {
                tray_icon: tray,
                bucky_status: Arc::new(Mutex::new(BuckyStatus::Stopped)),
                menu_state: Arc::new(Mutex::new(TrayMenuState {
                    click_seq: 0,
                    popup_seq: 0,
                    app_list_seq: 0,
                    app_list: vec![],
                    bucky_status: BuckyStatus::Stopped,
                })),
            };
            app.manage(app_state);

            scan_buckyos_status(app.app_handle().clone());
            scan_app_list_for_menu(app.app_handle().clone());
            Ok(())
        })
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
