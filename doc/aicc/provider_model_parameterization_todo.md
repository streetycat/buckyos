# AICC Provider 模型特判参数化 TODO

状态：Draft
范围：盘点 `src/frame/aicc` 当前按模型名选择 Provider 行为的实现，并列出迁移任务。本文不要求把协议编码器、认证或异步状态机配置化。

## 1. 问题

当前 Provider Rust 实现中存在大量类似逻辑：

```text
if model starts_with "gpt-5" -> 修改请求参数
if model contains "veo" -> 使用视频协议
if model contains "pro" -> 使用另一档价格
if model starts_with "claude-" -> 接纳到 inventory
```

这种实现隐含了“模型厂商等于调用协议厂商”。聚合 Provider 会打破这个假设，例如：

```text
Provider protocol: OpenAI-compatible
provider_model_id: anthropic/claude-sonnet-4
Model Driver: claude
调用 operation: chat.completions.create
```

该模型需要使用 Claude Model Driver 的模型语义，但必须使用当前 Provider 的 OpenAI-compatible operation。代码不能根据 `claude` 模型名切换到 Anthropic `/messages`，也不能因为模型名不以 `gpt-*` 开头就将其从 OpenAI-compatible inventory 删除。

目标是把三个维度彻底分开：

| 维度 | 真相源 |
| --- | --- |
| 模型固有能力、家族、版本和 variant | Model Driver metadata |
| 当前渠道的模型名、operation、请求限制和价格 | Provider 专用实现或配置型 Provider 参数 |
| 请求/响应编码、认证、流式与异步状态机 | Provider protocol adapter Rust 实现 |

官方 Provider 和 OpenRouter 等主流 Provider 仍采用专用实现。这里的“参数化”是让模型差异进入现有规则和 resolver，再将解析结果交给执行层；不是允许外部配置覆盖主流 Provider 的核心逻辑。小型兼容 Provider 可以复用同一规则结构从配置加载参数。

## 2. 复用现有规则和解析结果

当前 `DriverModelRule`、`DriverModelVariant`、`ModelMetadata` 和 `provider_call_from_metadata()` 已经构成“JSON 规则 → inventory → 调用参数”的基本链路。改造应扩展这条链路，而不是再建立一套独立的 Provider Call Plan 配置。

Provider-specific 字段从 Model Driver metadata 剥离后，两类配置继续复用 exact `models`、ordered `patterns`、`defaults` 和 `variants` 的基础规则结构。路由完成后，现有调用参数解析还需要得到：

```text
provider_instance
provider_model_id        # Provider discovery 原始名称，实际调用使用
origin_model_id          # Model Driver 识别出的原厂模型名
model_driver             # 模型技术语义来源
api_type / method
operation                # 当前 Provider 实际调用接口
request rules            # 默认参数、禁止参数、Provider options
resolved pricing
```

其中 `operation` 由当前 Provider rules 决定，不能只由 `model_driver` 或 `origin_model_id` 推导。必要时可以使用轻量内部类型 `ResolvedProviderCall` 承载这次调用的 operation、options 和价格结果，但它只是现有 resolver 的临时输出，不是新的配置文件、inventory 真相源或第二套规则体系。

## 3. P0：公共基础设施

### P0-1 扩展现有模型规则和调用参数解析

- [ ] 从现有 `DriverModelRule` 提取可复用的基础匹配结构，并增加 Provider-specific rule，避免重复实现第二套 exact/pattern resolver。
- [ ] 扩展现有 `ModelMetadata` 或其关联的 Provider rule reference，保存调用阶段所需的 operation、请求规则、价格规则和规则 revision。
- [ ] 扩展 `provider_call_from_metadata()` 等现有入口，使其根据 request 解析 operation、最终 Provider options 和价格；必要时返回轻量 `ResolvedProviderCall`。
- [ ] `provider_model_id`、`origin_model_id`、`model_driver` 和 `operation` 使用不同字段，禁止复用 `provider_driver` 表达多个概念。
- [ ] 专用 Provider 内置规则与配置型 Provider 外部规则都进入同一个 resolver。
- [ ] 空 Provider 参数配置继续使用 adapter 的默认解析结果。
- [ ] resolved operation 必须校验已被当前 adapter 实现。

涉及入口：

- `src/frame/aicc/src/model_types.rs`：当前 `ModelMetadata` 混有 `provider_actual_model_id` 和 `provider_options`。
- `src/frame/aicc/src/aicc.rs:539`：当前 `ResolvedRequest` 只有 method 和 request。
- `src/frame/aicc/src/aicc.rs:1996`：当前 `RouteAttempt` 只有 provider model 和 options。
- `src/frame/aicc/src/aicc.rs:2470`：当前 `provider_call_from_metadata()` 只返回模型名和 options。
- `src/frame/aicc/src/aicc.rs:1627`：当前 `Provider::start()` 没有接收显式 operation。

### P0-2 将 Provider 参数从 Model Driver metadata 剥离

- [ ] 从 `DriverModelRule` 和 `DriverModelVariant` 中移出协议相关 `provider_options`。
- [ ] `origin_provider_aliases`、`origin_mappings` 和渠道专属 `exclude` 不再由 Model Driver metadata 管理。
- [ ] Model Driver variant 只保留语义身份；Provider 参数负责将 variant 转换为当前协议请求字段。
- [ ] Model Driver pricing 只作为最后一级默认估值；Provider 实时价格和 Provider 渠道价格进入现有 pricing 解析结果。
- [ ] 保证 Provider 参数只能收窄 Model Driver 能力，不能扩张模型固有能力。
- [ ] 删除 Registry 在 `model_driver` 为空时直接赋值为 `inventory.provider_driver` 的隐式耦合；未知模型必须显式 fallback。
- [ ] 删除 AICC 根据模型名包含 `gpt` / `gemini` 自动生成 image logical mounts 的逻辑，挂载只来自 Model Driver。

涉及入口：

- `src/frame/aicc/src/metadata_resolver.rs:49`：`DriverMetadataDocument`。
- `src/frame/aicc/src/metadata_resolver.rs:129`：`DriverModelRule`。
- `src/frame/aicc/src/metadata_resolver.rs:246`：`DriverModelVariant`。
- `src/frame/aicc/src/metadata_resolver.rs:473`：metadata 直接生成 `provider_options`。
- `src/frame/aicc/src/model_registry.rs:36`：用 Provider driver 填充 Model Driver。
- `src/frame/aicc/src/aicc.rs:3171`：根据模型名生成图像挂载。

### P0-3 实现统一的 Provider 模型规则解析器

- [ ] 支持 exact `models` 优先于有序 `patterns`。
- [ ] 规则解析上下文同时包含原始 `provider_model_id`、已识别的 `origin_model_id`、`model_driver`、`api_type` 和 method。
- [ ] 渠道命名规则按 `provider_model_id` 匹配；与模型身份相关但需要协议编码的规则按 `model_driver + origin_model_id` 匹配。
- [ ] `metadata_drivers` 限制 Model Driver 搜索范围；官方 Provider 在专用实现中固定范围。
- [ ] 将 resolver 从“按单个 `provider_driver` 加载一份 metadata”改为“在候选 Model Driver 集合中做唯一匹配”。
- [ ] 移除内置 `openrouter` Model Driver metadata；OpenRouter 专用实现只提供候选范围和 origin mapping，模型语义来自原厂 Model Driver。
- [ ] 无 Model Driver 命中统一进入 conservative fallback；多 Driver 命中统一拒绝。
- [ ] 禁止 Provider 执行阶段再次使用字符串启发式重新分类模型。

当前入口：

- `src/frame/aicc/src/metadata_resolver.rs:311`：`resolve_driver_inventory()` 只接收单个 `provider_driver`。
- `src/frame/aicc/src/metadata_resolver.rs:1122`：内置 metadata 按 Provider driver 选择，包含 `openrouter.json`。

### P0-4 operation 必须在现有规则/调用参数解析阶段确定

- [ ] operation 至少按 `api_type` 解析；检查是否还需要 method 作为二级 selector。
- [ ] 同一模型在不同 Provider 可以得到不同 operation。
- [ ] 同一 Provider 的不同模型可以得到不同 operation。
- [ ] endpoint 由 adapter 根据 operation 构造，不再根据 base URL 后缀或模型名称猜测。
- [ ] caller 不能通过普通 request options 任意指定内部协议；只允许显式、受校验的 Provider Instance override。

### P0-5 请求参数特判参数化

- [ ] 盘点模型特有的默认参数、禁止参数、字段改名和条件规则。
- [ ] 按 `request_rules` 的最小公共表达实现：`when`、`defaults`、`set`、`remove`；`remove` 使用 JSON Pointer。
- [ ] `when` 支持单个 `path/op/value` 谓词或一层 `all` 组合；第一版仅支持 `exists`、`equals`、`not_equals`、`in`、`contains`。
- [ ] 只有出现当前结构不能表达的真实规则时才增加条件能力，避免设计通用脚本语言。
- [ ] 协议 adapter 只负责把已解析的语义转换为 wire payload，不再识别 GPT、Claude、Veo 等模型名。
- [ ] 用户显式参数与 Provider 默认参数的覆盖顺序必须统一并有测试。

### P0-6 价格统一从 pricing resolver 获取

- [ ] 删除 Provider Rust 文件里的模型名价格表和 `contains("pro")` 价格判断。
- [ ] 固定优先级：Provider 实时价格 > Provider Instance override > Provider 参数价格 > Model Driver 默认价格。
- [ ] 价格匹配可使用 Provider 原始模型名或已解析原厂身份，但必须记录匹配来源。
- [ ] `pricing` 保留 token 单价，并补充 `unit`、`amount`、`estimated_cost` 和有序 `rules`；条件复用 `request_rules.when`。
- [ ] 图片尺寸、质量、视频时长等阶梯价格使用结构化 pricing rule，不在执行函数中按模型名分支。
- [ ] estimate 与成功响应写入的实际/估算 cost 使用同一个解析结果。

### P0-7 trace 与诊断

- [ ] route trace 增加 operation、规则来源和规则 revision。
- [ ] 同时输出 `provider_model_id`、`origin_model_id`、`model_driver`，避免排障时混淆。
- [ ] 输出 request defaults/removed options 的规则 id，不记录敏感请求内容。
- [ ] inventory/model list 能解释模型为何被排除、归类或降级 fallback。

## 4. P0：OpenAI 与 OpenRouter

### P0-OAI-1 拆分 OpenRouter 专用实现

- [ ] 从 `openai.rs` 中移除 `provider_driver == "openrouter"` 特判，建立独立的 OpenRouter Provider 实现或专用策略对象。
- [ ] OpenRouter 专用实现固定其模型名映射、模型排除、价格 discovery 和 operation 选择，并进入发布测试矩阵。
- [ ] OpenRouter 继续复用 OpenAI-compatible wire encoder，但不能复用“OpenAI 模型名分类器”。
- [ ] OpenRouter inventory 不能再把所有 discovery 模型无条件放入 LLM bucket。

当前入口：

- `src/frame/aicc/src/openai.rs:4112`：`normalize_remote_model_ids()`。
- `src/frame/aicc/src/openai.rs:4135`：OpenRouter 将所有模型归为 LLM 的分支。
- `src/frame/aicc/src/openai.rs:5665`：OpenRouter inventory 相关测试。

### P0-OAI-2 替换 OpenAI discovery 模型名分类

- [ ] 删除 `is_text2image_model_name()` 和 `is_supported_llm_model_name()` 对模型类型的最终裁决权。
- [ ] discovery 只保留 Provider 返回的原始模型与动态字段；模型能力由 Model Driver 匹配，渠道 operation 由 Provider rules 和现有调用参数 resolver 决定。
- [ ] embedding、ASR、TTS、realtime、image、LLM 的 `contains/starts_with` 分类迁移到 OpenAI 专用参数解析。
- [ ] 未识别模型不得因名称不符合 `gpt-*`、`o*` 等白名单直接丢失，应进入 Model Driver 匹配或 conservative fallback。

当前入口：

- `src/frame/aicc/src/openai.rs:4080`：图像模型名称判断。
- `src/frame/aicc/src/openai.rs:4085`：LLM 名称白名单。
- `src/frame/aicc/src/openai.rs:4112`：remote inventory 分类。
- `src/frame/aicc/src/openai.rs:3924`：remote model resolve request 的 fallback 分类。

### P0-OAI-3 参数化 GPT 请求特判

- [ ] 将 GPT-5 nano/nono 的 reasoning、verbosity 默认值迁移为解析后的 request defaults。
- [ ] 将 GPT-5/Codex sampling 参数删除规则迁移为 resolved request rule。
- [ ] OpenAI-compatible Provider 提供 Claude 模型时，不应用 GPT 规则，也不切换 Anthropic 协议。
- [ ] 聚合 Provider 重命名 GPT 模型时，可以依据 `origin_model_id` 应用相同 OpenAI-wire 请求限制。
- [ ] 合并 OpenAI 与 SN 中重复的规则执行器，具体规则仍由各 Provider rules 提供。

当前入口：

- `src/frame/aicc/src/openai_protocol.rs:485`：`apply_provider_model_defaults()`。
- `src/frame/aicc/src/openai_protocol.rs:527`：`strip_incompatible_sampling_options()`。
- `src/frame/aicc/src/openai.rs:2519`：LLM 执行阶段调用模型名特判。

### P0-OAI-4 显式选择 Responses/Chat/Media operation

- [ ] 删除通过 base URL 是否包含 `/chat/completions` 选择协议的方式。
- [ ] LLM 调用参数解析显式选择 `responses.create` 或 `chat.completions.create`。
- [ ] image、embedding、ASR、TTS、video 和 image edit 同样由 operation 选择 endpoint。
- [ ] OpenAI-compatible Claude 模型的 operation 仍由 Provider 指定为 OpenAI-compatible operation。

当前入口：

- `src/frame/aicc/src/openai.rs:1378`：`use_chat_completions_endpoint()`。
- `src/frame/aicc/src/openai.rs:2493`：`start_llm()`。
- `src/frame/aicc/src/openai.rs:3461`：固定 `/videos` 流程。

### P0-OAI-5 移出硬编码价格

- [ ] 迁移 GPT token 价格表。
- [ ] 迁移 DALL-E/GPT Image 按 quality、size 的价格分支。
- [ ] 迁移 Sora/video 中按 `pro` 判断的价格。
- [ ] OpenRouter 优先使用 discovery 返回的渠道价格，不回退到错误的 OpenAI 官方价格。

当前入口：

- `src/frame/aicc/src/openai.rs:567`：GPT token 价格。
- `src/frame/aicc/src/openai.rs:685`：图片阶梯价格。
- `src/frame/aicc/src/openai.rs:3647`、`3793`：视频 `pro` 价格特判。

## 5. P0：Gemini

### P0-GEM-1 替换 inventory 名称分类器

- [ ] `classify_gemini_model()` 不再通过 `embedding`、`tts`、`lyria`、`veo`、`image`、`gemini` 字符串决定最终类型。
- [ ] 优先使用 discovery 的 `supportedGenerationMethods` 和 Model Driver 匹配结果。
- [ ] discovery 信息不足时，由 Gemini 专用参数规则提供 operation/capability 收窄，不由执行层猜测。
- [ ] `imagen`、`nano-banana` 等图像模型判断迁移出 Rust 名称分支。

当前入口：

- `src/frame/aicc/src/gemini.rs:685`：刷新 inventory 时调用分类器。
- `src/frame/aicc/src/gemini.rs:4334`：图像名称判断。
- `src/frame/aicc/src/gemini.rs:4464`：`classify_gemini_model()`。

### P0-GEM-2 显式解析视频 operation

- [ ] Veo 的 `predictLongRunning` 与 Omni 的 `interactions` 在 Provider rule 解析阶段确定。
- [ ] 删除从普通 request `provider_options.protocol` 选择内部协议的方式。
- [ ] OpenRouter 提供 Veo 时，由 OpenRouter Provider rules 选择其 `/videos` operation，不能命中 Gemini 原厂 Veo 分支。
- [ ] 各 operation 分别校验支持的 AICC method，例如 video2video、extend。

当前入口：

- `src/frame/aicc/src/gemini.rs:2494`：从 request 读取 `provider_options.protocol`。
- `src/frame/aicc/src/gemini.rs:3687`：Interactions video。
- `src/frame/aicc/src/gemini.rs:3796`：video dispatch。
- `src/frame/aicc/src/gemini.rs:3823`：运行时协议分支。

### P0-GEM-3 参数化版本、alias 和弃用策略

- [ ] 将“保留最大 Gemini 主版本”的名称规则迁移为 Gemini 专用 inventory 参数。
- [ ] 将数字版本快照与 alias 去重规则迁移为专用参数。
- [ ] deprecated 文案 signals 作为 Provider discovery 规则集中维护。
- [ ] 不把 Gemini 官方 inventory 策略自动套用到使用 Gemini wire protocol 的第三方模型。

当前入口：

- `src/frame/aicc/src/gemini.rs:744`：alias/version snapshot 过滤。
- `src/frame/aicc/src/gemini.rs:757`：最大主版本过滤。
- `src/frame/aicc/src/gemini.rs:4365`、`4406`、`4449`：具体名称算法。

### P0-GEM-4 移出硬编码价格

- [ ] 迁移 Gemini token 模型价格。
- [ ] 迁移图像模型名称、quality/size 对应价格。
- [ ] 视频、音乐、TTS 等默认成本统一进入 pricing resolver。

当前入口：

- `src/frame/aicc/src/gemini.rs:809`：token 价格。
- `src/frame/aicc/src/gemini.rs:918`：图像价格。

## 6. P0：SN AI Provider

- [ ] 将 `apply_sn_model_defaults()` 的 GPT nano/nono 名称特判迁移为 request defaults。
- [ ] 将 `strip_sn_sampling_options()` 的 GPT/Codex 规则迁移到公共规则执行器。
- [ ] 删除按 `provider_model.contains("pro")` 估算视频价格。
- [ ] endpoint 不再通过 base URL 是否以 `/responses` 结尾推导，改用显式 operation。
- [ ] SN inventory 中的模型分类、Provider 实际模型名和 Model Driver 归属使用统一 metadata/provider rule resolver。

当前入口：

- `src/frame/aicc/src/sn_ai_provider.rs:477`：模型默认参数。
- `src/frame/aicc/src/sn_ai_provider.rs:569`：sampling 参数删除。
- `src/frame/aicc/src/sn_ai_provider.rs:1004`：base URL 推导 endpoint。
- `src/frame/aicc/src/sn_ai_provider.rs:3022`：视频 `pro` 价格。

## 7. P1：Claude

- [ ] discovery 不再通过 `starts_with("claude-")` 作为最终 admission；使用限定的 Model Driver 匹配结果。
- [ ] token 价格不再通过 `contains("opus")` / `contains("haiku")` 判断。
- [ ] 删除或改写 test-only 的 Claude capability 名称分类器，测试应直接验证 driver metadata 解析结果。
- [ ] Anthropic `/messages`、header、content block 和 tool use 转换继续保留在 Claude protocol adapter，不配置化。

当前入口：

- `src/frame/aicc/src/claude.rs:390`：discovery 模型名前缀过滤。
- `src/frame/aicc/src/claude.rs:541`：token 价格。
- `src/frame/aicc/src/claude.rs:1174` 至 `1205`：测试侧 capability 名称分类。

## 8. P1：MiniMax

- [ ] discovery 不再用 `starts_with("MiniMax-M2")` 筛选 Anthropic-compatible 模型。
- [ ] 用 Provider 参数指定可调用 operation 和允许匹配的 Model Driver。
- [ ] `highspeed` token 价格分支迁移到 pricing rule。
- [ ] `.with_cost(0.01)`、`.with_latency(1400)` 等 fallback hint 迁移到 Provider 参数。
- [ ] MiniMax dialect 的请求/响应转换继续保留在 Claude protocol adapter 的 `ProtocolDialect::MiniMax` 实现中。

当前入口：

- `src/frame/aicc/src/minimax.rs:240`：inventory 名称过滤。
- `src/frame/aicc/src/minimax.rs:252`：硬编码成本和延迟。
- `src/frame/aicc/src/minimax.rs:273`：token 价格。

## 9. P1：fal

- [ ] 将四组独立模型列表统一为 Provider model rule：模型、AICC `api_type`、operation、价格和延迟在一条规则中关联。
- [ ] 默认 ESRGAN/rembg/deepfilternet/video-upscaler 模型迁移为 fal 专用内置参数数据。
- [ ] `build_request_body()` 的 method 到输入字段映射由 operation 选择，不依赖散落的 method match。
- [ ] 成功响应 cost 和 `estimate_cost()` 的硬编码表迁移到统一 pricing resolver。
- [ ] fal endpoint 仍由 adapter 根据 provider-native path 构造，不把任意 URL 开放给配置。

当前入口：

- `src/frame/aicc/src/fal.rs:34`：默认模型常量。
- `src/frame/aicc/src/fal.rs:345`：method 输入映射。
- `src/frame/aicc/src/fal.rs:484`：成功响应 cost。
- `src/frame/aicc/src/fal.rs:556`：estimate cost/latency。

## 10. 不应配置化的实现

以下逻辑继续保留在 Rust adapter 中：

- OpenAI/Claude/Gemini/fal 的 wire request/response 编码；
- 认证 header、token 获取和凭据保护；
- SSE、流式 chunk、异步 operation polling 和取消；
- HTTP 状态与 Provider error 的解析；
- 协议级字段 allowlist 和安全校验；
- ResourceRef 下载、上传、MIME 和大小限制；
- 已注册 operation 到具体 endpoint 的安全映射。

配置只选择已实现的 operation，并提供模型级数据；不能注入任意 URL、脚本或未知协议实现。

## 11. 测试 TODO

### P0 单元测试

- [ ] OpenAI-compatible Provider + Claude origin model：使用 OpenAI operation，应用 Claude Model Driver 语义。
- [ ] OpenAI-compatible Provider + 重命名 GPT：依据 origin identity 应用 GPT 请求限制，但调用保留原始 `provider_model_id`。
- [ ] Gemini 官方 Veo：选择 `predictLongRunning`。
- [ ] Gemini Omni：选择 `interactions`。
- [ ] OpenRouter Veo：选择 OpenRouter `/videos`，不能进入 Gemini 原厂分支。
- [ ] 同一 origin model 在官方 Provider 和聚合 Provider 得到不同 operation。
- [ ] Provider rule 只能删除能力，不能增加 Model Driver 未声明能力。
- [ ] 无匹配进入 conservative fallback；多 Model Driver 命中拒绝。
- [ ] Provider 实时价格覆盖 Provider 参数和 Model Driver 默认价格。
- [ ] `{}` 配置使用 adapter 默认调用参数解析结果。

### P1 集成与回归测试

- [ ] `api_type × method × provider × model_driver × operation` 覆盖矩阵。
- [ ] route trace 对 resolved operation 的解释与实际 HTTP endpoint 一致。
- [ ] exact model、logical model、variant 和 runtime fallback 使用相同规则解析。
- [ ] inventory refresh 前后规则 revision 可追踪，旧任务保持已经解析的 operation 和 Provider options。
- [ ] 增加静态审计：Provider 执行模块新增模型家族字符串分支时要求显式测试或迁移到规则层。
- [ ] 删除旧特判后运行 `cargo test -p aicc` 和完整 `cargo test`。
- [ ] 构建验证 `cd src && uv run buckyos-build.py --skip-web`。

## 12. 文档和协议联动

- [ ] 更新 `driver_metadata_schema.md`，删除 Provider-specific 字段。
- [ ] 更新 `provider_profile_schema.md`，补充专用 Provider rules 与配置型 Provider rules 进入同一现有 resolver 的方式。
- [ ] 更新 `aicc-models-mgr.md` 和 `aicc_provider_plan.md` 的 Provider/Model Driver 边界。
- [ ] 更新 `models.list`、route trace 和管理 API 的共享类型。
- [ ] 如果 Provider 参数支持云更新，补充 schema 校验、原子 activation、LKGS 和回滚验收。
- [ ] 更新维护与验收矩阵，明确主流专用 Provider 和配置型 Provider 的不同验收要求。
