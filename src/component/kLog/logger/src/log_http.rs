use crate::inner_types::HttpCommand;

enum HttpRequestType {
    WriteLog,
}

pub struct LogHttp {
    app: tide::Server<()>,
    port: u16,
}

impl LogHttp {
    pub async fn startup(
        port: u16,
        channel: async_std::channel::Sender<(
            HttpCommand,
            async_std::channel::Sender<(tide::StatusCode, String)>,
        )>,
    ) -> tide::Result<Self> {
        let mut app = tide::new();

        app.at("/log").put(Self::new_executor(
            channel.clone(),
            HttpRequestType::WriteLog,
            port,
        ));

        let host = format!("127.0.0.1:{}", port);
        app.clone().listen(&host).await.map_err(|err| {
            log::error!("listen http at {} failed: {:?}", host, err);
            err
        })?;

        Ok(Self { app, port })
    }

    fn new_executor(
        channel: async_std::channel::Sender<(
            HttpCommand,
            async_std::channel::Sender<(tide::StatusCode, String)>,
        )>,
        req_type: HttpRequestType,
        port: u16,
    ) -> LogHttpExecutor {
        LogHttpExecutor {
            channel,
            req_type,
            port,
        }
    }
}

struct LogHttpExecutor {
    channel: async_std::channel::Sender<(
        HttpCommand,
        async_std::channel::Sender<(tide::StatusCode, String)>,
    )>,
    req_type: HttpRequestType,
    port: u16,
}

impl LogHttpExecutor {
    async fn log(req: &mut tide::Request<()>) -> tide::Result<HttpCommand> {
        req.body_json().await.map(|req| HttpCommand::WriteLog(req))
    }
}

#[async_trait::async_trait]
impl tide::Endpoint<()> for LogHttpExecutor {
    async fn call(&self, mut req: tide::Request<()>) -> tide::Result {
        let cmd = match self.req_type {
            HttpRequestType::WriteLog => Self::log(&mut req).await,
        };

        match cmd {
            Ok(cmd) => {
                let (sender, receiver) = async_std::channel::bounded(1);
                while let Err(err) = self.channel.send((cmd.clone(), sender.clone())).await {
                    log::error!(
                        "send log request to executor failed: {:?}, local: {}, remote: {:?}",
                        err,
                        self.port,
                        req.remote()
                    );
                }

                let (state_code, resp) = receiver.recv().await?;

                Ok(tide::Response::builder(state_code)
                    .content_type(tide::http::mime::JSON)
                    .body(resp)
                    .build())
            }
            Err(err) => {
                log::error!(
                    "parse log request failed: {:?}, local: {}, remote: {:?}",
                    err,
                    self.port,
                    req.remote()
                );
                Err(err)
            }
        }
    }
}
