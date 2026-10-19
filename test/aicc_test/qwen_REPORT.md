# AICC Qwen T1 / T1.5 / T2 测试报告

## 最终汇总

| 层级 | 最终覆盖 | 最终结果 | Cleanup |
| --- | --- | --- | --- |
| T1 | 全量 189 项 | 最终全量 189/189 通过 | 通过 |
| T1.5 | Qwen + SN 全量 715 项 | 最终全量 715/715 通过 | 通过 |
| T2 | 既有授权轮次原始结果 73 项；视觉能力矩阵修订后未重跑 | 22 自动通过、45 待复核、6 自动失败；人工复核口径为 62 通过、10 失败、1 个 Provider 未授权 | 通过 |

此前报告把不同轮次合并为“129 个有效单元格、5 个有效失败”，并把多个失败单元格按四类现象压缩描述；该口径无法与 73 项原始报告中的 10 个失败逐项核对，现已撤回。原始 10 个失败为：`qwen3.7-max`、`qwen3.7-max-2026-05-17`、`qwen3.7-max-2026-05-20` 各自的字幕和 OCR 共 6 项，`qwen3.8-2.4t-a95b` 的字幕和 OCR 共 2 项，`qwen3.7-flash-2026-07-15` OCR 超时 1 项，以及 `qwen3.7-max-2026-06-08` OCR 质量 1 项。`cmaas-qwen3.8-flash` 是另计的 Provider 未授权，不在这 10 项内。

视觉配置以阿里云百炼[视觉理解模型总览](https://help.aliyun.com/zh/model-studio/vision-model/)的任务推荐为准：通用图像/视频理解使用原生视觉模型并暴露 `vision.caption`；OCR 只对文档明确推荐的 `qwen3.5-ocr`、`qwen3.8-flash`、`qwen3.7-plus` 暴露 `vision.ocr`。`qwen3.6-plus` 仍保留通用视觉理解，但不再声明专用 OCR API。`qwen3.7-max` 基础别名及旧快照、`qwen3.8-2.4t-a95b` 按官方输入模态收窄为仅 LLM；只有明确支持图像/视频输入的 `qwen3.7-max-2026-06-08` 保留 `vision.caption`。

因此，上述 10 个历史失败都不再属于修订后的有效视觉测试矩阵：其中 8 个来自实际不支持视觉的模型，另 2 个来自未被官方列为 OCR 推荐模型的 OCR 单元格。由于本轮没有新的真实 T2 调用授权，修订后矩阵没有生成新的 T2 汇总数，不能把历史通过数直接当作当前有效覆盖数。

`qwen3.7-flash-2026-07-15` 的 60 秒错误确实发生在同步响应路径：ZoneGateway 等待 AICC 的 HTTP 响应头超过 60000 ms 后返回 HTTP 500。它是视觉理解/OCR 的 Responses 同步调用，不是图像生成同步接口遗漏异步 `NativeTask` 的同一问题；现有证据也不足以进一步断定耗时全部发生在 Qwen 服务端还是 AICC 内部。该 OCR 单元格虽已退出有效矩阵，Gateway/AICC 对长同步调用的超时处理仍是独立运行时风险。

本次真实调用报告中的费用由 Provider 返回 CNY，runner 无法可靠换算 USD，因此实际 USD 费用为未知，不能按零费用解读。当前 Qwen 在线目录另有 127 项配置漂移，不属于“此前未完成用例”的本次授权范围，尚未进行真实 T2。

T2 覆盖 Qwen 原生文本、视觉、图像和视频模型，阿里云百炼部署的 DeepSeek、GLM、Kimi、MiniMax 文本模型，以及带 `vanchin/`、`ZHIPU/`、`kimi/`、`MiniMax/` 命名空间的厂商直供文本和视觉模型。Qwen 官方目录未提供 Doubao 模型 ID，因此未将 Doubao 纳入 Qwen 聚合平台测试。实际调用费用以各轮报告记录的 Provider 币种为准；未知的美元折算额不记作零。

这些聚合模型并非都只有 LLM 能力：DeepSeek v4.1 Flash、GLM 5.3 Flash、Kimi K2.6/K2.7 Code/K2.7 Code Highspeed/K3、MiniMax M3 的视觉能力均已通过 T2。原厂能力由对应 `*.model.json` 定义；Qwen 的模型 ID 映射、调用协议、价格、Qwen 专属 variants 和差异化限制由 `qwen.provider.json` 定义。Kimi 与 MiniMax 的 `*.model.json` 未保留 Qwen 平台专属参数。

2026-10-09 的 Qwen `/models` 快照返回 262 个模型，并包含当时账号未开通的 `xiaomi/mimo-v2.5-pro`，说明该接口不是逐模型授权清单。厂商直供模型由 WebUI 显式选择；Qwen 自有的已知可用基础模型继续自动发现。`tongyi-xiaomi-analysis-flash` 和 `tongyi-xiaomi-analysis-pro` 虽默认提供免费试用，但仓库没有内置 Xiaomi Model Driver，按本次范围未加入 AICC 库存。

## T1 轮次报告

- [第 1 轮：180 通过，4 失败](reports/acceptance/aicc-t1-2026-10-09T09-39-14-345Z-3c3f0381/summary.md)
- [第 2 轮：2 通过，2 失败](reports/acceptance/aicc-t1-2026-10-09T09-46-11-535Z-8b688bbd/summary.md)
- [第 3 轮：0 通过，2 失败](reports/acceptance/aicc-t1-2026-10-09T09-48-41-434Z-02276eaa/summary.md)
- [第 4 轮：0 通过，2 失败](reports/acceptance/aicc-t1-2026-10-09T09-51-15-974Z-b33227c2/summary.md)
- [第 5 轮：0 通过，1 失败](reports/acceptance/aicc-t1-2026-10-09T09-54-34-420Z-533441d0/summary.md)
- [第 6 轮：0 通过，2 失败](reports/acceptance/aicc-t1-2026-10-09T10-03-23-752Z-8da70950/summary.md)
- [第 7 轮：2/2 通过](reports/acceptance/aicc-t1-2026-10-09T10-13-30-904Z-7d30b54c/summary.md)
- [第 8 轮（最终全量）：184/184 通过](reports/acceptance/aicc-t1-2026-10-09T10-15-22-826Z-50dd76a0/summary.md)
- [第 9 轮（最终全量复核）：184/184 通过](reports/acceptance/aicc-t1-2026-10-09T14-28-43-946Z-3cf275be/summary.md)
- [2026-10-18 全量：189/189 通过](reports/acceptance/aicc-t1-2026-10-18T05-09-32-758Z-47402616/summary.md)
- [2026-10-18 全量：189/189 通过](reports/acceptance/aicc-t1-2026-10-18T06-27-49-158Z-dce1e035/summary.md)
- [2026-10-18 全量：189/189 通过](reports/acceptance/aicc-t1-2026-10-18T08-21-04-357Z-ea94049f/summary.md)
- [2026-10-18 全量：189/189 通过](reports/acceptance/aicc-t1-2026-10-18T13-02-00-973Z-fcb1ada8/summary.md)
- [2026-10-18 全量：188 通过，1 失败](reports/acceptance/aicc-t1-2026-10-18T13-57-46-682Z-dd624b6f/summary.md)
- [2026-10-18 定向：1/1 通过](reports/acceptance/aicc-t1-2026-10-18T14-02-13-765Z-76ddd560/summary.md)
- [2026-10-18 定向：3/3 通过](reports/acceptance/aicc-t1-2026-10-18T14-56-10-062Z-a1fb8df4/summary.md)
- [2026-10-18 全量：189/189 通过](reports/acceptance/aicc-t1-2026-10-18T15-22-46-966Z-a45d9e7b/summary.md)
- [2026-10-18 全量：189/189 通过](reports/acceptance/aicc-t1-2026-10-18T16-19-20-044Z-61e6a0c7/summary.md)
- [2026-10-18 全量：189/189 通过](reports/acceptance/aicc-t1-2026-10-18T16-37-13-974Z-5ebb36a7/summary.md)
- [2026-10-19 全量：189/189 通过](reports/acceptance/aicc-t1-2026-10-19T03-23-38-746Z-29c35c28/summary.md)
- [2026-10-19 全量：189/189 通过](reports/acceptance/aicc-t1-2026-10-19T03-59-30-079Z-a2912bd0/summary.md)
- [2026-10-19 全量：189/189 通过](reports/acceptance/aicc-t1-2026-10-19T04-06-02-970Z-f5ddacae/summary.md)
- [2026-10-19 全量：189/189 通过](reports/acceptance/aicc-t1-2026-10-19T04-11-31-901Z-7e001b6c/summary.md)
- [2026-10-19 最终部署全量：189/189 通过](reports/acceptance/aicc-t1-2026-10-19T04-20-07-636Z-39077a7c/summary.md)
- [2026-10-19 视觉矩阵修订后全量：189/189 通过](reports/acceptance/aicc-t1-2026-10-19T05-11-42-891Z-817074c1/summary.md)

## T1.5 轮次报告

- [第 1 轮（全量）：1605/1605 通过](reports/acceptance/t15-20261009102046-1386856/summary.md)
- [第 2 轮（全量）：1625/1625 通过](reports/acceptance/t15-20261009111407-1464232/summary.md)
- [第 3 轮（Qwen）：97/97 通过](reports/acceptance/t15-20261009115106-1516149/summary.md)
- [第 4 轮（Qwen）：0 通过，1 失败](reports/acceptance/t15-20261009121833-1554414/summary.md)
- [第 5 轮（Qwen）：97/97 通过](reports/acceptance/t15-20261009121853-1554923/summary.md)
- [第 6 轮（Qwen）：97 通过，1 失败](reports/acceptance/t15-20261009123034-1571064/summary.md)
- [第 7 轮（Qwen 最终回归）：98/98 通过](reports/acceptance/t15-20261009123531-1577484/summary.md)
- [第 8 轮（Qwen 聚合协议启动门禁）：0 通过，1 失败](reports/acceptance/t15-20261009134028-1667305/summary.md)
- [第 9 轮（Qwen 聚合协议）：12/12 通过](reports/acceptance/t15-20261009134048-1668115/summary.md)
- [第 10 轮（Qwen 聚合库存门禁）：1/1 通过](reports/acceptance/t15-20261009134815-1680607/summary.md)
- [第 11 轮（全量启动门禁）：0 通过，1 失败](reports/acceptance/t15-20261009143337-1758434/summary.md)
- [第 12 轮（全量）：1381 通过，1 失败](reports/acceptance/t15-20261009143349-1758749/summary.md)
- [第 13 轮（Qwen variant 库存门禁）：0 通过，1 失败](reports/acceptance/t15-20261009150156-1797892/summary.md)
- [第 14 轮（Qwen variant 基础模型门禁）：0 通过，1 失败](reports/acceptance/t15-20261009150238-1798918/summary.md)
- [第 15 轮（Qwen 聚合 variant 协议）：112 通过，124 失败](reports/acceptance/t15-20261009150636-1806232/summary.md)
- [第 16 轮（Qwen 最终回归）：236/236 通过](reports/acceptance/t15-20261009151255-1814876/summary.md)
- [第 17 轮（Qwen 最新元数据复核）：236/236 通过](reports/acceptance/t15-20261009153158-1844755/summary.md)
- [第 18 轮（Qwen 最终部署复核）：236/236 通过](reports/acceptance/t15-20261009155732-1885366/summary.md)
- [2026-10-18 Qwen 定向：3/3 通过](reports/acceptance/t15-20261018065517-3222081/summary.md)
- [2026-10-18 Qwen + SN：352/352 通过](reports/acceptance/t15-20261018082600-3352909/summary.md)
- [2026-10-18 Qwen 全量：269/269 通过](reports/acceptance/t15-20261018130610-3732887/summary.md)
- [2026-10-18 Qwen 全量：275/275 通过](reports/acceptance/t15-20261018145823-3894113/summary.md)
- [2026-10-18 Qwen 全量：275/275 通过](reports/acceptance/t15-20261018162619-4033778/summary.md)
- [2026-10-18 Qwen + Doubao Speech：309/309 通过](reports/acceptance/t15-20261018164139-4060154/summary.md)
- [2026-10-19 Qwen 全量：275/275 通过](reports/acceptance/t15-20261019032820-725256/summary.md)
- [2026-10-19 Qwen 启动门禁：1 失败](reports/acceptance/t15-20261019040414-779192/summary.md)
- [2026-10-19 Qwen 启动门禁：1 失败](reports/acceptance/t15-20261019041018-791717/summary.md)
- [2026-10-19 Qwen 启动门禁：1 失败](reports/acceptance/t15-20261019041550-803995/summary.md)
- [2026-10-19 最终 Qwen 全量：275/275 通过](reports/acceptance/t15-20261019042427-822402/summary.md)
- [2026-10-19 视觉矩阵修订后 Qwen + SN 全量：715/715 通过](reports/acceptance/t15-20261019051623-902273/summary.md)

## T2 轮次报告

- [第 1 轮：0 通过，59 失败，2 跳过](reports/acceptance/aicc-2026-10-09T10-59-09-267Z-aed62ddf/summary.md)
- [第 2 轮：0 通过，2 跳过](reports/acceptance/aicc-2026-10-09T11-00-01-736Z-5a78ed8f/summary.md)
- [第 3 轮：2/2 通过](reports/acceptance/aicc-2026-10-09T11-00-13-855Z-8d5c179f/summary.md)
- [第 4 轮：1 通过，2 失败](reports/acceptance/aicc-2026-10-09T11-00-51-352Z-338061ef/summary.md)
- [第 5 轮：1 通过，2 待复核](reports/acceptance/aicc-2026-10-09T11-02-33-633Z-a8a4c04e/summary.md)
- [第 6 轮：1 通过，2 失败](reports/acceptance/aicc-2026-10-09T11-03-13-025Z-049beddc/summary.md)
- [第 7 轮：3/3 通过](reports/acceptance/aicc-2026-10-09T11-04-47-713Z-85fa9413/summary.md)
- [第 8 轮：4/4 通过](reports/acceptance/aicc-2026-10-09T11-44-17-580Z-6fe1b262/summary.md)
- [第 9 轮：3 通过，3 失败](reports/acceptance/aicc-2026-10-09T11-55-24-315Z-5d47408b/summary.md)
- [第 10 轮：2 通过，1 失败](reports/acceptance/aicc-2026-10-09T12-23-31-487Z-aa38be64/summary.md)
- [第 11 轮：1/1 通过](reports/acceptance/aicc-2026-10-09T12-25-50-312Z-1f09bab5/summary.md)
- [第 12 轮：0 通过，221 失败，75 跳过](reports/acceptance/aicc-2026-10-09T13-29-10-817Z-24090c20/summary.md)
- [第 13 轮：0 通过，221 失败，75 跳过](reports/acceptance/aicc-2026-10-09T13-29-23-415Z-37321c77/summary.md)
- [第 14 轮（百炼部署聚合模型）：12 通过，11 失败](reports/acceptance/aicc-2026-10-09T13-41-26-696Z-347b78de/summary.md)
- [第 15 轮（失败单元回归）：3/3 通过](reports/acceptance/aicc-2026-10-09T13-48-31-282Z-b86b239b/summary.md)
- [第 16 轮（厂商直供聚合模型）：0 通过，17 失败](reports/acceptance/aicc-2026-10-09T13-48-55-343Z-c08ce3c6/summary.md)
- [第 17 轮（已开通厂商直供文本及多模态）：17 通过，13 待复核，1 失败](reports/acceptance/aicc-2026-10-09T15-38-34-079Z-e05a977d/summary.md)
- [第 18 轮（厂商直供多模态最终复核）：14/14 通过](reports/acceptance/aicc-2026-10-09T15-43-32-579Z-7fa08d7b/summary.md)
- [2026-10-18 混合矩阵：160 失败，114 跳过](reports/acceptance/aicc-2026-10-18T06-10-16-048Z-1222211d/summary.md)
- [2026-10-18 Qwen + Doubao Agent Plan：3 通过，4 待复核](reports/acceptance/aicc-2026-10-18T06-15-37-799Z-17c37b57/summary.md)
- [2026-10-18 Qwen + GLM：1 通过，2 失败](reports/acceptance/aicc-2026-10-18T06-17-28-063Z-1ea46d5b/summary.md)
- [2026-10-18 Qwen 定向：1 失败，1 待复核](reports/acceptance/aicc-2026-10-18T07-24-54-029Z-2621734c/summary.md)
- [2026-10-18 Qwen 定向：1 失败](reports/acceptance/aicc-2026-10-18T07-27-08-006Z-88c38b0e/summary.md)
- [2026-10-18 Qwen 定向：1 跳过](reports/acceptance/aicc-2026-10-18T14-27-10-655Z-41db41ce/summary.md)
- [2026-10-18 Qwen 定向：1 失败](reports/acceptance/aicc-2026-10-18T14-27-33-200Z-2387a270/summary.md)
- [2026-10-18 Qwen 图像定向：2 跳过](reports/acceptance/aicc-2026-10-18T15-05-25-692Z-88065785/summary.md)
- [2026-10-18 Qwen 图像定向：2 待复核](reports/acceptance/aicc-2026-10-18T15-05-43-442Z-1e4770de/summary.md)
- [2026-10-19 在线目录漂移检查：127 失败，98 跳过](reports/acceptance/aicc-2026-10-19T03-13-07-483Z-f7c325aa/summary.md)
- [2026-10-19 在线目录漂移复核：127 失败，98 跳过](reports/acceptance/aicc-2026-10-19T03-13-19-171Z-4b61b319/summary.md)
- [2026-10-19 既有未完成矩阵预检：6 失败，29 跳过](reports/acceptance/aicc-2026-10-19T03-15-11-124Z-7275d21d/summary.md)
- [2026-10-19 真实调用授权门禁：73 跳过](reports/acceptance/aicc-2026-10-19T03-35-35-021Z-750b79f0/summary.md)
- [2026-10-19 本次真实 T2：22 自动通过，45 待复核，6 失败；人工复核后 62 通过、10 失败、1 个 Provider 未授权](reports/acceptance/aicc-2026-10-19T03-37-05-866Z-4001c57e/summary.md)
