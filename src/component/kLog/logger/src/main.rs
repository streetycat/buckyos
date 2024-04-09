use async_std::process;
use inner_types::HttpCommand;
use log_http::LogHttp;
use system_log_file::LogFileMgr;

mod inner_types;
mod log_http;
mod system_log_file;
mod types;

async fn main_run() {
    // TODO: read config from etcd
    let log_dir = "/var/log/klog";
    let port = 8080;

    let (sender, receiver) = async_std::channel::bounded(64);

    async_std::task::spawn(async move {
        if let Err(err) = LogHttp::startup(port, sender).await {
            log::error!("LogHttp server error: {}, will exit.", err);
            process::exit(-1);
        }
    });

    let mut log_mgr = match LogFileMgr::init(log_dir).await {
        Some(mgr) => mgr,
        None => {
            log::error!("LogFileMgr init error, will exit.");
            process::exit(-1);
        }
    };

    let mut cmds = vec![];
    let mut notifiers = vec![];
    let mut resps = vec![];

    loop {
        while let Ok(cmd) = receiver.recv().await {
            cmds.push(cmd.0);
            notifiers.push(cmd.1);
        }

        for cmd in cmds.iter() {
            match cmd {
                HttpCommand::WriteLog(req) => {
                    log_mgr.append(req).await;
                    resps.push((tide::StatusCode::Ok, "".to_string()));
                }
            }
        }

        while let Some(resp) = resps.pop() {
            let notifier = notifiers.pop().unwrap();
            if let Err(err) = notifier.send(resp).await {
                log::error!("notify response error: {}", err);
            }
        }

        cmds.clear();

        log_mgr.flush().await;
    }
}

fn main() {
    log::info!("main");

    // cyfs_debug::ProcessDeadHelper::patch_task_min_thread();

    log::info!("will main-run");

    let fut = Box::pin(main_run());
    async_std::task::block_on(async move { fut.await })
}
