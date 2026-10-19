# AICC DeepSeek / Kimi / MiniMax 最终测试报告

测试日期：2026-10-13

SN 补充测试日期：2026-10-19。

测试环境：Ubuntu，BuckyOS `/opt/buckyos`。凭证未写入报告。

## 最终汇总

| 层级 | 覆盖范围 | 最终结果 |
|---|---|---:|
| T1 | AICC 全量静态、路由与配置契约 | 189 / 189 通过 |
| T1.5 | Provider 全量回归基线 | 1605 / 1605 通过 |
| T1.5 | DeepSeek 全量 | 30 / 30 通过 |
| T1.5 | Kimi 官方 API 当前四个模型 | 35 / 35 通过 |
| T1.5 | MiniMax 全量协议矩阵 | 100 / 100 通过 |
| T1.5 | SN / OpenAI 受影响全量 | 472 / 472、393 / 393 通过 |
| T2 | DeepSeek：`deepseek-flash`、`deepseek-v4-pro` | 2 passed，2 review，0 failed |
| T2 | Kimi：4 个模型 × `llm` / `vision.caption` / `vision.ocr` | 12 / 12 通过 |
| T2 | MiniMax：22 个模型/API 单元 | 9 passed，10 review，3 provider_restricted，0 failed |
| T2 | SN：当前账户 22 个可执行模型、80 个模型/API 单元 | 75 passed，1 failed，4 review |

SN 本次只定向复测原失败的 23 个单元，最终 22 passed、1 failed、0 review、0 skipped；其他 57 个单元沿用既有结果，不代表新版本重新执行全量 T2。唯一失败为 `gpt-5.4-nano/vision.ocr` 未识别出 `4827`，其异步任务及结果读取成功，属于内容质量失败。其他 3 个 OCR 复测满足既定数字 rubric，不代表整行文字逐字正确。4 个既有待复核项仍为 1 个 TTS 听审与 3 个图生图语义复核，本次未扩大重测范围。目录未返回的 7 个已配置 alias 不计为已测，完整覆盖边界见 [SN 汇总](../../../sn_REPORT.md)。

SN 本次接受 31 个唯一 AICC 任务（23 个目标、8 个源图），全部终止且任务成功；6 个 computer-use 各包含两轮请求，合计 37 次成功上游推理的未知费用估算为 $0.37，实际 USD 扣费未知，不代表实际收费上限。21 个输出对象已清理，临时 SN 设置及实例已移除。幂等回读、GET 轮询和下载不计为新增推理；历史各轮财务与清理信息见链接。当前 Responses 单元没有异步缺口 skip，也未使用同步替代；既有视频/Realtime 协议缺口模型继续排除，不进行真实调用。

MiniMax 参数已按官方资料校准：M3.1 Flash Preview 使用 1M context、524288 最大输出、强制 thinking，并支持 `low` / `medium` / `high` / `xhigh` / `max` effort；M3 使用 1M context、524288 最大输出及可关闭/自适应 thinking；M2.x 使用 204800 context、强制 thinking，不暴露 effort。M3/M3.1 默认 `temperature=1`、`top_p=0.95`，M2.x 默认 `temperature=1`、`top_p=0.9`。

H3 与 H3-Max 的默认值和合法范围均由 MiniMax provider 配置注入：H3 为 4–15 秒、`768P` / `2K`，H3-Max 为 5–15 秒、`480P` / `768P`。运行时协议实现不再按模型名选择时长、分辨率或默认参数分支。

M3.1 按官方限定作为 M Plan / MiniMax Code 预览模型 exact-only 暴露。本轮所用中国区 API Key 的官方 `/anthropic/v1/models` 返回 M3 与 M2.x，不返回 M3.1，因此其 `llm`、`vision.caption`、`vision.ocr` 三个缺失 T2 单元均归类为 `provider_restricted`；未向无权限的推理端点发起调用，真实调用数与费用均为 0。

Kimi 官方 API 模型目录和账号 `/v1/models` 当前提供 `kimi-k3`、`kimi-k2.7-code`、`kimi-k2.7-code-highspeed`、`kimi-k2.6`。`kimi-k2.8-preview` 不在 Kimi API 账号库存中，因此未作为 `kimi` Provider 的 T1.5/T2 模型执行。

DeepSeek 与 MiniMax 的既有真实调用单元均无 failed；未启用 Judge 的视觉或媒体单元保留为 review。

## 详细报告链接

### T1

- [2026-10-19 全量：189 / 189](../reports/acceptance/aicc-t1-2026-10-19T09-00-57-355Z-6782ed83/summary.md)
- [最终全量：189 / 189](../reports/acceptance/aicc-t1-2026-10-13T10-23-24-980Z-18a39645/summary.md)

### T1.5

- [2026-10-19 SN/OpenAI 全量：865 / 865（SN 472、OpenAI 393）](../reports/acceptance/t15-20261019091556-1238917/summary.md)
- [2026-10-19 SN 单轮：466 passed、5 failed](../reports/acceptance/t15-20261019090524-1224633/summary.md)
- [Provider 全量：1605 / 1605](../reports/acceptance/t15-20261010101158-3080107/summary.md)
- [DeepSeek：30 / 30](../reports/acceptance/t15-20261008102309-22405/summary.md)
- [Kimi 官方四模型：35 / 35](../reports/acceptance/t15-20261010160418-3615574/summary.md)
- [MiniMax 最终协议回归：100 / 100](../reports/acceptance/t15-20261013103926-850006/summary.md)

### T2

- [SN 定向十六单元：人工复核 16/16 通过](../reports/acceptance/aicc-2026-10-19T09-42-55-268Z-f490ac88/summary.md)
- [SN 定向七单元：人工复核后 6 passed、1 failed](../reports/acceptance/aicc-2026-10-19T09-30-39-700Z-e72f3108/summary.md)
- [SN 全量 80 单元详细结果，含人工复核和审计补充](../reports/acceptance/aicc-2026-10-19T07-28-29-275Z-eb259b9a/summary.md)
- [SN 42 单元详细结果，含人工复核](../reports/acceptance/aicc-2026-10-19T07-48-16-711Z-bac3f0f4/summary.md)
- [SN 有效 computer-use 六单元结果](../reports/acceptance/aicc-2026-10-19T08-14-26-156Z-e3db2fbb/summary.md)
- [SN computer-use 四单元结果](../reports/acceptance/aicc-2026-10-19T08-15-30-112Z-b647e6ac/summary.md)
- [SN computer-use 最后两单元结果](../reports/acceptance/aicc-2026-10-19T08-17-49-401Z-601783fc/summary.md)

- [DeepSeek 与 Kimi / MiniMax 首轮目标矩阵](../reports/acceptance/aicc-2026-10-08T10-27-44-863Z-53cd8c14/summary.md)
- [DeepSeek Flash vision.caption 最终结果](../reports/acceptance/aicc-2026-10-08T14-04-29-790Z-ded511de/summary.md)
- [Kimi 官方四模型最终结果：12 / 12](../reports/acceptance/aicc-2026-10-10T16-05-07-946Z-b2b047ea/summary.md)
- [Kimi 旧模型与 MiniMax M2.x / M3 LLM](../reports/acceptance/aicc-2026-10-08T05-44-41-155Z-e37ceeba/summary.md)
- [MiniMax M3 视觉](../reports/acceptance/aicc-2026-10-08T06-04-51-424Z-fb8605b0/summary.md)
- [MiniMax 语音、图像与 H3/H3-Max 七个单元](../reports/acceptance/aicc-2026-10-08T10-49-33-585Z-a00bd81c/summary.md)
- [MiniMax H3/H3-Max 图生视频两个单元](../reports/acceptance/aicc-2026-10-08T10-53-03-815Z-e9a08ce1/summary.md)
- [MiniMax M3.1 三个缺失单元：3 provider_restricted，0 failed，0 real calls](../reports/acceptance/aicc-2026-10-13T10-48-36-117Z-d8b6d2cd/summary.md)
