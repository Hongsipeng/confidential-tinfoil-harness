# Tinfoil Chat Harness 接口文档（中文）

> 对应 [`openapi.yaml`](./openapi.yaml)，OpenAPI 3.1.1，协议版本 `1`。本文供阅读和集成时使用；字段类型、必填条件和约束以 YAML 为准。保留路径、字段、事件和错误码的原名，便于直接搜索代码。

## 1. 服务与安全边界

- 默认服务地址：`https://chat-api.tinfoil.sh`。
- 聊天 API 经 SDK 的远程证明和 EHBP 密封传输发送。请求正文只对客户端和 harness enclave 可见；网关可见请求头。因此内容密钥、消息、设置等私密数据放在正文，不放在请求头。
- `Authorization` 表示调用方身份，可为 Clerk 会话令牌、登出用户的 `free_` key，或 `tk_` API key。`X-Tinfoil-Inference-Key` 是客户端取得的聊天访问凭证；`Accept-Language` 表示语言偏好。
- 与运行有关的请求可使用 `X-Tinfoil-Harness-Instance` 指定承载实例；续接流使用 `Last-Event-ID` 指明已经处理的最后一个事件。
- 除公开接口或单独注明的接口外，调用需要身份；某些操作还需要聊天访问凭证。以每个 operation 的 `security` 为准。
- 云端聊天以账户内容密钥封存；设备本地聊天由客户端在每次 Turn 中传入记录，并在运行结束后取回。

## 2. 一次聊天的主链路

```text
POST /v1/session          获取模型、功能开关、默认设置与额度
POST /v1/threads/turn     提交一次用户操作，得到 SSE 事件流
POST /v1/threads/follow   流中断后用 runId + Last-Event-ID 续接
POST /v1/threads/cancel   需要时停止运行
```

`Turn` 是一次用户操作，`run` 是服务端执行该操作的过程。`clientRequestId` 是 UUID 幂等键：同一请求重发时返回已启动的运行，避免重复启动。普通发送的 `kind` 为 `send`，省略 `kind` 时也默认为 `send`。

### Turn 的主要字段

| 字段 | 含义 |
| --- | --- |
| `clientRequestId` **必填** | 本次操作的 UUID；重试时沿用。 |
| `threadId` **必填** | 聊天 ID。 |
| `kind` | `send`、`edit`、`regenerate`、`continue`、`retry`、`retryToolCall`、`ask`、`compact`。 |
| `content`、`quote` | 发送的文字和引用。 |
| `messageId`、`toolCallId` | 编辑、重试等操作的目标消息或工具调用。 |
| `attachments`、`widgets` | 本轮附件及客户端支持展示的组件。 |
| `local`、`thread` | 设备本地聊天标记及完整聊天记录。 |
| `key` | 云端聊天需要的账户内容密钥，采用 base64 编码。 |
| `profile`、`projectId`、`presetId` | 本轮使用的设置、项目和预设。 |
| `options` | 模型及本轮功能选项。 |
| `browser` | 浏览器工具执行者的身份与私密令牌，仅在允许的本地 Turn 中使用。 |
| `compactLatest` | 手动压缩时是否连最近一轮也摘要化；依赖会话功能开关。 |

`options` 可包含 `model`、`autoIntelligence`、`reasoningEffort`、`timezone`、`locale`、`thinking`、`webSearch`、`piiCheck`、`genUI`、`research` 和 `cacheSecret`。其中 `research` 是深度研究，受 `features.research` 和额度约束；`cacheSecret` 是客户端保存的 256 位随机值，以 64 位小写十六进制字符串表示，用来隔离模型提示词缓存。

### SSE 流与续接

`/v1/threads/turn` 和 `/v1/threads/follow` 返回 `text/event-stream`。每帧的 `data` 是一个 `HarnessEvent`，帧 `id` 是这次运行内的事件游标。客户端应按顺序处理事件，并跳过同一运行内已处理过的游标。断线后传 `FollowInput`（`threadId`、`runId`，需要时包含 `key`），并在 `Last-Event-ID` 中传最后处理的事件 ID。

长运行只保留最近事件。若游标早于保留范围，harness 会先发送检查点：按原顺序重放运行开始后的事件，合并连续的文本、思考或工具参数增量，并用工具活动快照代替旧增量。检查点只有最后一帧带游标；中途断开时可整体重放。`MESSAGES_SNAPSHOT` 最多包含 8 MiB 消息，较旧消息可能省略，并由 `hasOlder` 标记。

| 事件类型 | 中文含义 |
| --- | --- |
| `RUN_STARTED` | 运行开始，给出 `threadId`、`runId`、实例等标识。 |
| `MESSAGES_SNAPSHOT` | 消息快照，客户端用它建立本轮开始时的消息状态。 |
| `STATE_SNAPSHOT` / `STATE_DELTA` | 聊天与队列状态的完整快照 / 增量。 |
| `TEXT_MESSAGE_CHUNK` | 回复文本增量。 |
| `REASONING_MESSAGE_CHUNK` / `REASONING_SUMMARY` | 思考内容增量 / 摘要。 |
| `TOOL_CALL_START` / `TOOL_CALL_ARGS` / `TOOL_CALL_END` / `TOOL_CALL_RESULT` | 工具调用开始、参数增量、参数结束、结果。搜索工具也通过这些事件展示。 |
| `ACTIVITY_SNAPSHOT` / `ACTIVITY_DELTA` | 工具活动进度的快照 / 增量。 |
| `BROWSER_TOOL_REQUEST` | 已完整校验、可交给浏览器执行者执行的操作；不能仅凭部分工具参数执行。 |
| `COMPACTION_START` / `COMPACTION_END` | 上下文压缩开始 / 结束。 |
| `RUN_FINISHED` / `RUN_ERROR` | 运行结束 / 失败。 |

## 3. 接口索引

除 `GET /healthz`、`GET /metrics` 外，以下均为 `POST`。请求体一般为 `application/json`；标明“表单”的接口使用 `multipart/form-data`。正常响应的具体结构见 [原始 OpenAPI](./openapi.yaml) 中对应路径和 `components.responses`。

### 基础与会话

| 接口 | 功能 |
| --- | --- |
| `GET /healthz` | 健康检查，确认 harness 正在服务。 |
| `GET /metrics` | 输出 Prometheus 格式的汇总指标，需管理员权限。 |
| `POST /v1/session` | 打开应用会话，获取模型目录、预设、功能开关、额度和默认设置。 |
| `POST /v1/changes` | 读取账户已存储数据的修订版本，供客户端同步判断。 |

### 运行与队列

| 接口 | 功能 |
| --- | --- |
| `POST /v1/threads/turn` | 提交 `Turn`，启动或重新跟随一次运行，返回 SSE。内存紧张时可能返回 `SERVICE_BUSY`。 |
| `POST /v1/threads/browser/result` | 浏览器执行者提交一次操作结果；相同结果重试可接受，冲突结果拒绝。 |
| `POST /v1/threads/follow` | 按 `runId` 与游标续接运行；设备聊天完成后的运行也会暂留，供断线客户端取回结果。 |
| `POST /v1/threads/cancel` | 停止运行；后续排队 Turn 也会以取消状态结束。 |
| `POST /v1/threads/queue/send` | 立即启动队列中的 Turn，并停止当前回复。 |
| `POST /v1/threads/queue/remove` | 移除排队中的 Turn，当前回复继续。 |

### 聊天记录

| 接口 | 功能 |
| --- | --- |
| `POST /v1/threads/list` | 分页列出云端聊天及置顶聊天。 |
| `POST /v1/threads/search` | 搜索云端聊天；可能报告索引仍在建立。此接口搜索**聊天记录**，不是联网搜索。 |
| `POST /v1/threads/get` | 打开云端聊天。 |
| `POST /v1/threads/update` | 修改聊天设置和元数据。 |
| `POST /v1/threads/fork` | 从现有聊天分叉出新聊天。 |
| `POST /v1/threads/messages/delete` | 删除消息。 |
| `POST /v1/threads/messages/update` | 编辑已有回复。 |
| `POST /v1/threads/messages/answer` | 回答回复中的交互问题。 |
| `POST /v1/threads/delete` | 删除一条聊天。 |
| `POST /v1/threads/delete-all` | 删除账户的全部聊天。 |
| `POST /v1/threads/transcript` | 读取云端聊天的完整内容及修订信息，以便迁到设备。 |
| `POST /v1/threads/import` | 把设备聊天保存到云端。 |
| `POST /v1/threads/upgrade` | 转换旧版客户端保存的设备聊天。 |

### 分享、设置与项目

| 接口 | 功能 |
| --- | --- |
| `POST /v1/threads/share` | 创建聊天分享链接；解密密钥位于链接片段中。 |
| `POST /v1/threads/share/status` | 查询聊天是否有有效分享。 |
| `POST /v1/threads/share/revoke` | 撤销分享。 |
| `POST /v1/shares/open` | 打开分享的聊天。 |
| `POST /v1/profile/get` | 读取账户设置。 |
| `POST /v1/profile/update` | 更新账户设置。 |
| `POST /v1/profile/upgrade` | 转换旧版客户端设置。 |
| `POST /v1/profile/merge` | 合并设备设置与云端设置。 |
| `POST /v1/projects/list` | 分页列出项目。 |
| `POST /v1/projects/get` | 打开项目及其文档。 |
| `POST /v1/projects/create` | 创建项目。 |
| `POST /v1/projects/update` | 修改项目。 |
| `POST /v1/projects/delete` | 删除项目。 |
| `POST /v1/projects/delete-all` | 删除全部项目。 |
| `POST /v1/projects/documents/upload`（表单） | 向项目添加文档。 |
| `POST /v1/projects/documents/delete` | 从项目移除文档。 |

### 内容密钥、附件与其他能力

| 接口 | 功能 |
| --- | --- |
| `POST /v1/keys/current` | 查询当前内容密钥的登记信息。 |
| `POST /v1/keys/register` | 登记内容密钥。 |
| `POST /v1/keys/add-bundle` | 向内容密钥添加 passkey 密钥包。 |
| `POST /v1/keys/remove-bundle` | 删除 passkey 密钥包。 |
| `POST /v1/keys/migrate-all` | 用当前密钥重新封存已存储数据，并返回迁移报告。 |
| `POST /v1/attachments/upload`（表单） | 转换上传文件，生成聊天附件。 |
| `POST /v1/attachments/get` | 下载聊天附件。 |
| `POST /v1/attachments/get-public` | 下载分享聊天中的附件。 |
| `POST /v1/transcriptions`（表单） | 将录音转写成文字。 |
| `POST /v1/speech` | 把文字转换为音频并流式返回。 |
| `POST /v1/import`（表单） | 导入聊天归档；结果以逐行 JSON 返回。 |
| `POST /v1/import/status` | 查询导入任务进度。 |
| `POST /v1/export`（表单） | 将聊天导出为归档文件。 |
| `POST /v1/metadata/favicon` | 获取网站图标。 |
| `POST /v1/metadata/image` | 在 enclave 内获取公开图片，返回供本地显示的图片字节。 |
| `POST /v1/metadata/link` | 获取网页链接预览。 |

## 4. 主要数据对象

下表是阅读请求与事件时最常用的对象。完整字段表、必填约束、枚举和嵌套关系见 YAML 的 `components.schemas`。

| Schema | 说明 |
| --- | --- |
| `Session` | 会话初始化结果：协议、用户、额度、模型、默认模型、组件、预设、输入配置和 `features`。仅在对应功能开关为 `true` 时使用可选能力。 |
| `SessionModel` | 可用模型的 ID、名称、说明、徽标、推理能力等。 |
| `Turn` / `TurnOptions` | 一次聊天操作及本轮模型、联网搜索、PII、深度研究等选项。 |
| `FollowInput` / `RunRequest` | 续接运行与取消、队列操作的定位信息。 |
| `HarnessEvent` | SSE 事件联合类型。各具体事件 Schema 以事件名对应，例如 `RunStartedEvent`。 |
| `ThreadSummary` / `ThreadWindow` | 聊天列表摘要 / 打开聊天时的消息窗口。 |
| `ThreadRecord` / `ThreadInput` | 持久化聊天记录 / 设备聊天作为请求输入的格式。客户端应保留 `ThreadRecord` 中不认识的字段。 |
| `MessageView` / `MessageRecord` / `Block` | 展示中的消息、持久化消息、回复时间线中的内容或工具步骤。 |
| `Profile` / `ProfileView` / `ProfileInput` | 账户设置、带同步信息的视图，以及设备上传的设置。 |
| `Project` / `ProjectDocument` | 项目及项目文档。 |
| `Attachment` / `AttachmentView` / `SentAttachment` | 附件存储记录、展示视图与 Turn 中引用的附件。 |
| `KeyCurrent` / `KeyBundleInput` / `ResealReport` | 内容密钥登记信息、passkey 密钥包、重新封存结果。 |
| `ImportLine` / `ImportStatus` | 归档导入的逐行结果与异步任务状态。 |
| `ErrorResponse` / `ErrorBody` | HTTP 错误体及失败原因；`RUN_ERROR` 也携带同类错误字段。 |

### 记录版本与兼容性

- 协议尽量只增加路由、字段、事件和错误码，不改变已有含义。客户端忽略未知字段和事件；未知错误码按 `action` 处理。
- 响应中的枚举未来可能增加新值，客户端应容忍未知字符串。
- `ThreadRecord` 必须整体保存并原样传回，包括客户端暂不认识的字段。较旧消息能否省略，以 `ThreadInput` 的规则为准。
- 已知 `recordVersion` 为 `1` 和 `2`；版本 `2` 增加上下文压缩标记。未知版本返回 `RECORD_UNSUPPORTED`。
- 对请求中的未知字段，harness 一般忽略；`profile` patch 是例外，会拒绝未知字段。

## 5. 错误处理

HTTP 错误通常是 `{"error": {"code": "...", "message": "...", "action": "..."}}`；运行中失败通过 `RUN_ERROR` 事件报告。`message` 是给日志和开发者看的英文描述；客户端应按结构化的 `code`、`action` 选择用户提示和后续行为。

| `action` | 客户端应采取的行为 |
| --- | --- |
| `reauthenticate` | 刷新身份和聊天凭证后重发一次；仍失败则请用户登录。 |
| `reload` | 本地聊天或运行状态已过期，重新读取并展示。 |
| `retry` | 原请求重发可能成功；有 `retryAfter` 时先等待指定秒数。失败的 Turn 应由用户决定是否重试。 |
| `wait` | 等待额度在 `resetsAt` 恢复，并向用户说明。 |
| `unlock` | 设备缺少内容密钥或密钥过旧，请用户解锁或恢复。 |
| `revise` | 请求本身需要修改，例如更换文件、模型或缩短聊天。 |
| `stop` | 当前无法继续，展示错误且不提供直接重试。 |

`ErrorBody` 还可能包含 `kind`（额度类型）、`retryAfter`、`resetsAt`、下游 `status`、`keyId`、`upstreamCode` 和 `rateLimit`。客户端不认识某个错误码时仍按 `action` 处理；连 `action` 也不认识时，应显示失败并停止自动恢复。

## 6. 联网搜索在协议中的位置

联网搜索没有浏览器直接调用的 `/web-search` 路由。客户端通过 `Turn.options.webSearch` 表示本轮搜索偏好；harness 在运行中决定是否向模型提供 `web_search` / `web_fetch` 工具。工具调用及结果作为 SSE 的 `TOOL_CALL_*` 和活动事件返回。`Turn.options.research` 则启用更长的深度研究流程，只有 `Session.features.research` 开启时才能提交。

`POST /v1/threads/search` 与此不同：它只检索账户已保存的聊天记录。

## 7. 规范与测试样本

[`openapi.yaml`](./openapi.yaml) 是正式协议；[`fixtures`](./fixtures) 是 harness 录制的运行样本。每份样本包含 `description`、事件帧 `frames` 及客户端处理后应得到的 `expected`。前端测试将这些帧交给 reducer，检查最终消息、标题和运行状态是否一致。修改协议或事件后，应同时更新相应样本并检查客户端兼容性。

## 附录：全部 Schema 字段速查

本附录从 `openapi.yaml` 自动提取字段、必填性、类型和枚举；“含义”是中文速记。具体长度、格式、条件和业务限制仍以原始 YAML 为准。

### `Json`：任意 JSON 值

类型：任意 JSON。

### `JsonObject`：JSON 对象

类型：对象。

### `Identifier`：标识符

类型：字符串。

### `Timestamp`：时间戳

类型：字符串。

### `Keyed`：携带内容密钥的请求

| 字段 | 必填 | 类型或允许值 | 含义 |
| --- | --- | --- | --- |
| `key` | 否 | 字符串 | 内容密钥 |

### `KeyedId`：携带密钥与 ID 的请求

| 字段 | 必填 | 类型或允许值 | 含义 |
| --- | --- | --- | --- |
| `key` | 否 | 字符串 | 内容密钥 |
| `id` | 是 | `Identifier` | 标识符 |

### `ThreadIdRequest`：聊天 ID 请求

| 字段 | 必填 | 类型或允许值 | 含义 |
| --- | --- | --- | --- |
| `threadId` | 是 | `Identifier` | 聊天 ID |

### `PageRequest`：分页请求

| 字段 | 必填 | 类型或允许值 | 含义 |
| --- | --- | --- | --- |
| `url` | 是 | 字符串 | 网址 |

### `RemoteImage`：远程图片

| 字段 | 必填 | 类型或允许值 | 含义 |
| --- | --- | --- | --- |
| `image_bytes` | 是 | 字符串 | 参见原始 Schema 说明 |
| `image_content_type` | 是 | 固定值 `image/png` | 参见原始 Schema 说明 |

### `ErrorResponse`：错误响应

| 字段 | 必填 | 类型或允许值 | 含义 |
| --- | --- | --- | --- |
| `error` | 是 | `ErrorBody` | 错误 |

### `ErrorBody`：错误详情

| 字段 | 必填 | 类型或允许值 | 含义 |
| --- | --- | --- | --- |
| `code` | 是 | `ErrorCode` | 错误码 |
| `message` | 是 | 字符串 | 错误说明 |
| `action` | 是 | `ErrorAction` | 处理动作 |
| `kind` | 否 | 字符串 | 操作或类别 |
| `retryAfter` | 否 | 整数 | 建议等待秒数 |
| `resetsAt` | 否 | `Timestamp` | 额度恢复时间 |
| `status` | 否 | 整数 | 状态 |
| `keyId` | 否 | 字符串 | 密钥 ID |
| `upstreamCode` | 否 | 字符串 | 上游错误码 |
| `rateLimit` | 否 | `RateLimit` | 额度信息 |

### `ErrorAction`：错误处理动作

类型：`reauthenticate`、`reload`、`retry`、`wait`、`unlock`、`revise`、`stop` / 字符串。

### `ErrorCode`：错误码

类型：`ALREADY_APPLIED`、`ARCHIVE_TOO_LARGE`、`ATTACHMENT_TOO_LARGE`、`ATTACHMENT_UNSUPPORTED`、`ATTACHMENTS_UNAVAILABLE`、`BAD_REQUEST`、`BROWSER_TIMEOUT`、`CHAT_FLAGGED`、`COMPACTION_FAILED`、`CONTEXT_TOO_LARGE`、`CREDENTIAL_REQUIRED`、`IMPORT_INVALID`、`IMPORT_TOO_LARGE`、`KEY_MISMATCH`、`KEY_REQUIRED`、`MAX_TURNS`、`MISSING_ATTACHMENT`、`MODEL_UNAVAILABLE`、`NOTHING_TO_COMPACT`、`NOTHING_TO_READ`、`PAYLOAD_TOO_LARGE`、`PROJECTS_REQUIRE_CLOUD`、`QUEUE_FULL`、`QUOTA_EXHAUSTED`、`RATE_LIMITED`、`RECORD_UNSUPPORTED`、`RESEARCH_LIMIT_REACHED`、`REQUEST_TIMEOUT`、`REVISION_CONFLICT`、`RUN_IN_FLIGHT`、`RUN_LOG_FULL`、`RUN_NOT_FOUND`、`SEARCH_INDEX_FAILED`、`SERVICE_BUSY`、`SYNC_OUTCOME_UNKNOWN`、`TEMP_SPACE_BUSY`、`THREAD_EXISTS`、`THREAD_NOT_FOUND`、`THREAD_TOO_LARGE`、`TOOL_UNAVAILABLE`、`UNAUTHENTICATED`、`UPSTREAM_REFUSED` / 字符串。

### `Session`：应用会话

| 字段 | 必填 | 类型或允许值 | 含义 |
| --- | --- | --- | --- |
| `protocol` | 是 | 对象 | 协议版本 |
| `user` | 是 | 对象 | 用户信息 |
| `rateLimit` | 是 | `RateLimit` | 额度信息 |
| `models` | 是 | 数组〈`SessionModel`〉 | 模型目录 |
| `auto` | 是 | 对象 | 参见原始 Schema 说明 |
| `defaultModel` | 是 | 字符串 | 默认模型 |
| `widgets` | 是 | 数组〈字符串〉 | 组件 |
| `profileDefaults` | 是 | `Profile` | 参见原始 Schema 说明 |
| `composer` | 是 | `Composer` | 参见原始 Schema 说明 |
| `presets` | 是 | 数组〈`Preset`〉 | 预设列表 |
| `features` | 是 | 对象 | 服务端能力开关 |

### `SessionModel`：会话模型

| 字段 | 必填 | 类型或允许值 | 含义 |
| --- | --- | --- | --- |
| `id` | 是 | 字符串 | 标识符 |
| `label` | 是 | 字符串 | 参见原始 Schema 说明 |
| `shortLabel` | 是 | 字符串 | 参见原始 Schema 说明 |
| `icon` | 是 | 字符串 | 参见原始 Schema 说明 |
| `description` | 是 | 字符串 | 说明 |
| `badges` | 是 | 数组〈对象〉 | 参见原始 Schema 说明 |
| `reasoning` | 是 | 对象 / null | 参见原始 Schema 说明 |
| `deprecated` | 是 | 布尔值 | 参见原始 Schema 说明 |
| `auto` | 否 | 布尔值 | 参见原始 Schema 说明 |

### `Preset`：预设

| 字段 | 必填 | 类型或允许值 | 含义 |
| --- | --- | --- | --- |
| `id` | 是 | 字符串 | 标识符 |
| `name` | 是 | 字符串 | 名称 |
| `description` | 是 | 字符串 | 说明 |
| `systemPrompt` | 是 | 字符串 | 参见原始 Schema 说明 |
| `icon` | 是 | 字符串 | 参见原始 Schema 说明 |
| `builtIn` | 是 | 布尔值 | 参见原始 Schema 说明 |

### `Composer`：输入框能力

| 字段 | 必填 | 类型或允许值 | 含义 |
| --- | --- | --- | --- |
| `attachments` | 是 | 数组〈`FileGroup`〉 | 附件 |
| `projectDocuments` | 是 | 数组〈`FileGroup`〉 | 参见原始 Schema 说明 |
| `pasteAsFileChars` | 是 | 整数 | 参见原始 Schema 说明 |
| `dictationMaxSeconds` | 是 | 整数 | 参见原始 Schema 说明 |
| `dictationSegmentSeconds` | 是 | 整数 | 参见原始 Schema 说明 |

### `FileGroup`：文件类别

| 字段 | 必填 | 类型或允许值 | 含义 |
| --- | --- | --- | --- |
| `accept` | 是 | 数组〈字符串〉 | 参见原始 Schema 说明 |
| `maxBytes` | 是 | 整数 | 参见原始 Schema 说明 |
| `imageMaxSide` | 否 | 整数 | 参见原始 Schema 说明 |

### `RateLimit`：额度信息

| 字段 | 必填 | 类型或允许值 | 含义 |
| --- | --- | --- | --- |
| `kind` | 是 | `free_daily`、`hourly`、`null` / 字符串 | 操作或类别 |
| `maxRequests` | 否 | 整数 | 参见原始 Schema 说明 |
| `remaining` | 否 | 整数 | 剩余额度 |
| `resetsAt` | 否 | `Timestamp` | 额度恢复时间 |
| `inputTokens` | 否 | `TokenBudget` | 输入 Token 数 |
| `outputTokens` | 否 | `TokenBudget` | 输出 Token 数 |

### `TokenBudget`：Token 预算

| 字段 | 必填 | 类型或允许值 | 含义 |
| --- | --- | --- | --- |
| `max` | 是 | 整数 | 上限 |
| `used` | 是 | 整数 | 已使用 |
| `remaining` | 是 | 整数 | 剩余额度 |

### `Turn`：聊天操作

| 字段 | 必填 | 类型或允许值 | 含义 |
| --- | --- | --- | --- |
| `key` | 否 | 字符串 | 内容密钥 |
| `clientRequestId` | 是 | 字符串 | 幂等请求 ID |
| `threadId` | 是 | `Identifier` | 聊天 ID |
| `kind` | 否 | `send`、`edit`、`regenerate`、`continue`、`retry`、`retryToolCall`、`ask`、`compact` | 操作或类别 |
| `ephemeral` | 否 | 布尔值 | 不保存的临时操作 |
| `local` | 否 | 布尔值 | 设备本地聊天 |
| `thread` | 否 | `ThreadInput` | 聊天记录 |
| `profile` | 否 | `ProfileInput` | 账户设置 |
| `projectId` | 否 | 字符串 / null | 项目 ID |
| `presetId` | 否 | 字符串 / null | 预设 ID |
| `content` | 否 | 字符串 | 内容 |
| `quote` | 否 | 字符串 | 引用内容 |
| `messageId` | 否 | 字符串 | 消息 ID |
| `toolCallId` | 否 | 字符串 | 工具调用 ID |
| `attachments` | 否 | 数组〈`SentAttachment`〉 | 附件 |
| `widgets` | 否 | 数组〈字符串〉 | 组件 |
| `browser` | 否 | `BrowserAccess` | 参见原始 Schema 说明 |
| `compactLatest` | 否 | 布尔值 | 连最近一轮一起压缩 |
| `options` | 否 | `TurnOptions` | 本轮选项 |

### `BrowserAccess`：浏览器执行权限

| 字段 | 必填 | 类型或允许值 | 含义 |
| --- | --- | --- | --- |
| `executorId` | 是 | 字符串 | 浏览器执行者 ID |
| `token` | 是 | 字符串 | 执行令牌 |

### `BrowserAction`：浏览器操作

| 字段 | 必填 | 类型或允许值 | 含义 |
| --- | --- | --- | --- |
| `action` | 是 | `tabs`、`read`、`screenshot`、`click`、`hover`、`fill`、`select`、`press`、`scroll`、`navigate`、`back`、`forward`、`open`、`switch`、`close`、`wait` | 处理动作 |
| `tabId` | 否 | 整数 | 参见原始 Schema 说明 |
| `ref` | 否 | 字符串 | 参见原始 Schema 说明 |
| `text` | 否 | 字符串 | 文字 |
| `url` | 否 | 字符串 | 网址 |
| `key` | 否 | `Enter`、`Tab`、`Escape`、`ArrowDown`、`ArrowUp`、`ArrowLeft`、`ArrowRight`、`Backspace` | 内容密钥 |
| `direction` | 否 | `up`、`down` | 参见原始 Schema 说明 |
| `screenshotId` | 否 | 字符串 | 参见原始 Schema 说明 |
| `x` | 否 | 数字 | 参见原始 Schema 说明 |
| `y` | 否 | 数字 | 参见原始 Schema 说明 |

### `BrowserResult`：浏览器执行结果

| 字段 | 必填 | 类型或允许值 | 含义 |
| --- | --- | --- | --- |
| `text` | 是 | 字符串 | 文字 |
| `image` | 否 | 字符串 | 图片 |

### `BrowserResultInput`：浏览器结果提交

| 字段 | 必填 | 类型或允许值 | 含义 |
| --- | --- | --- | --- |
| `threadId` | 是 | `Identifier` | 聊天 ID |
| `runId` | 是 | 字符串 | 运行 ID |
| `executorId` | 是 | 字符串 | 浏览器执行者 ID |
| `token` | 是 | 字符串 | 执行令牌 |
| `executionId` | 是 | 字符串 | 浏览器操作 ID |
| `result` | 是 | `BrowserResult` | 结果 |

### `BrowserToolRequestEvent`：浏览器工具请求事件

| 字段 | 必填 | 类型或允许值 | 含义 |
| --- | --- | --- | --- |
| `type` | 是 | 固定值 `BROWSER_TOOL_REQUEST` | 事件类型 |
| `timestamp` | 否 | `Timestamp` | 事件时间 |
| `expiresAt` | 是 | 字符串 | 过期时间 |
| `executionId` | 是 | 字符串 | 浏览器操作 ID |
| `toolCallId` | 是 | 字符串 | 工具调用 ID |
| `executorId` | 是 | 字符串 | 浏览器执行者 ID |
| `operation` | 是 | `BrowserAction` | 具体操作 |

### `TurnOptions`：聊天操作选项

| 字段 | 必填 | 类型或允许值 | 含义 |
| --- | --- | --- | --- |
| `model` | 否 | 字符串 | 模型 ID |
| `autoIntelligence` | 否 | 字符串 | 自动智能等级 |
| `reasoningEffort` | 否 | 字符串 | 思考强度 |
| `timezone` | 否 | 字符串 | 时区 |
| `locale` | 否 | 字符串 | 语言区域 |
| `thinking` | 否 | 布尔值 | 思考开关 |
| `webSearch` | 否 | 布尔值 | 联网搜索开关 |
| `piiCheck` | 否 | 布尔值 | PII 检查开关 |
| `genUI` | 否 | 布尔值 | 生成式界面开关 |
| `research` | 否 | 布尔值 | 深度研究 |
| `cacheSecret` | 否 | 字符串 | 缓存隔离密钥 |

### `SentAttachment`：发送的附件

| 字段 | 必填 | 类型或允许值 | 含义 |
| --- | --- | --- | --- |
| `id` | 是 | `Identifier` | 标识符 |
| `kind` | 否 | `AttachmentKind` | 操作或类别 |
| `fileName` | 否 | 字符串 | 文件名 |
| `mimeType` | 否 | 字符串 | 媒体类型 |
| `size` | 否 | 整数 | 文件大小 |
| `thumbnail` | 否 | 字符串 | 缩略图 |
| `textContent` | 否 | 字符串 | 提取的文字 |
| `description` | 否 | 字符串 | 说明 |
| `pages` | 否 | 数组〈`DocumentPage`〉 | 文档页面 |
| `base64` | 否 | 字符串 | Base64 数据 |

### `FollowInput`：续接输入

| 字段 | 必填 | 类型或允许值 | 含义 |
| --- | --- | --- | --- |
| `key` | 否 | 字符串 | 内容密钥 |
| `threadId` | 是 | `Identifier` | 聊天 ID |
| `runId` | 是 | 字符串 | 运行 ID |

### `RunRequest`：运行操作请求

| 字段 | 必填 | 类型或允许值 | 含义 |
| --- | --- | --- | --- |
| `key` | 否 | 字符串 | 内容密钥 |
| `threadId` | 是 | `Identifier` | 聊天 ID |
| `runId` | 否 | 字符串 | 运行 ID |
| `queueId` | 否 | 字符串 | 排队项 ID |

### `HarnessEvent`：运行事件联合类型

类型：`RunStartedEvent` / `MessagesSnapshotEvent` / `StateSnapshotEvent` / `StateDeltaEvent` / `TextMessageChunkEvent` / `ReasoningMessageChunkEvent` / `ReasoningSummaryEvent` / `ToolCallStartEvent` / `ToolCallArgsEvent` / `ToolCallEndEvent` / `ToolCallResultEvent` / `BrowserToolRequestEvent` / `ActivitySnapshotEvent` / `ActivityDeltaEvent` / `CompactionStartEvent` / `CompactionEndEvent` / `RunFinishedEvent` / `RunErrorEvent`。

### `RunStartedEvent`：运行开始事件

| 字段 | 必填 | 类型或允许值 | 含义 |
| --- | --- | --- | --- |
| `type` | 是 | 固定值 `RUN_STARTED` | 事件类型 |
| `timestamp` | 否 | `Timestamp` | 事件时间 |
| `threadId` | 是 | 字符串 | 聊天 ID |
| `runId` | 是 | 字符串 | 运行 ID |
| `instance` | 是 | 字符串 | 运行所在实例 |
| `messageId` | 否 | 字符串 | 消息 ID |

### `MessagesSnapshotEvent`：消息快照事件

| 字段 | 必填 | 类型或允许值 | 含义 |
| --- | --- | --- | --- |
| `type` | 是 | 固定值 `MESSAGES_SNAPSHOT` | 事件类型 |
| `timestamp` | 否 | `Timestamp` | 事件时间 |
| `messages` | 是 | 数组〈`MessageView`〉 | 消息列表 |
| `hasOlder` | 是 | 布尔值 | 是否还有更早消息 |

### `StateSnapshotEvent`：状态快照事件

| 字段 | 必填 | 类型或允许值 | 含义 |
| --- | --- | --- | --- |
| `type` | 是 | 固定值 `STATE_SNAPSHOT` | 事件类型 |
| `timestamp` | 否 | `Timestamp` | 事件时间 |
| `snapshot` | 是 | 对象 | 完整快照 |

### `StateDeltaEvent`：状态增量事件

| 字段 | 必填 | 类型或允许值 | 含义 |
| --- | --- | --- | --- |
| `type` | 是 | 固定值 `STATE_DELTA` | 事件类型 |
| `timestamp` | 否 | `Timestamp` | 事件时间 |
| `threadId` | 否 | 字符串 | 聊天 ID |
| `runId` | 否 | 字符串 | 运行 ID |
| `delta` | 是 | 数组〈`StatePatch`〉 | 增量内容 |

### `StatePatch`：状态补丁

类型：对象 / 对象 / 对象 / 对象。

### `QueueItem`：排队操作

| 字段 | 必填 | 类型或允许值 | 含义 |
| --- | --- | --- | --- |
| `queueId` | 是 | 字符串 | 排队项 ID |
| `kind` | 是 | 字符串 | 操作或类别 |
| `content` | 是 | 字符串 / null | 内容 |
| `quote` | 是 | 字符串 / null | 引用内容 |
| `attachments` | 是 | 数组〈对象〉 | 附件 |
| `createdAt` | 是 | `Timestamp` | 创建时间 |

### `TextMessageChunkEvent`：文本增量事件

| 字段 | 必填 | 类型或允许值 | 含义 |
| --- | --- | --- | --- |
| `type` | 是 | 固定值 `TEXT_MESSAGE_CHUNK` | 事件类型 |
| `timestamp` | 否 | `Timestamp` | 事件时间 |
| `messageId` | 是 | 字符串 | 消息 ID |
| `role` | 是 | 固定值 `assistant` | 消息角色 |
| `delta` | 是 | 字符串 | 增量内容 |

### `ReasoningMessageChunkEvent`：思考增量事件

| 字段 | 必填 | 类型或允许值 | 含义 |
| --- | --- | --- | --- |
| `type` | 是 | 固定值 `REASONING_MESSAGE_CHUNK` | 事件类型 |
| `timestamp` | 否 | `Timestamp` | 事件时间 |
| `messageId` | 是 | 字符串 | 消息 ID |
| `delta` | 是 | 字符串 | 增量内容 |

### `ReasoningSummaryEvent`：思考摘要事件

| 字段 | 必填 | 类型或允许值 | 含义 |
| --- | --- | --- | --- |
| `type` | 是 | 固定值 `REASONING_SUMMARY` | 事件类型 |
| `timestamp` | 否 | `Timestamp` | 事件时间 |
| `messageId` | 是 | 字符串 | 消息 ID |
| `blockId` | 是 | 字符串 | 参见原始 Schema 说明 |
| `content` | 是 | 字符串 | 内容 |

### `ToolCallStartEvent`：工具开始事件

| 字段 | 必填 | 类型或允许值 | 含义 |
| --- | --- | --- | --- |
| `type` | 是 | 固定值 `TOOL_CALL_START` | 事件类型 |
| `timestamp` | 否 | `Timestamp` | 事件时间 |
| `toolCallId` | 是 | 字符串 | 工具调用 ID |
| `toolCallName` | 是 | 字符串 | 参见原始 Schema 说明 |
| `parentMessageId` | 否 | 字符串 | 参见原始 Schema 说明 |

### `ToolCallArgsEvent`：工具参数事件

| 字段 | 必填 | 类型或允许值 | 含义 |
| --- | --- | --- | --- |
| `type` | 是 | 固定值 `TOOL_CALL_ARGS` | 事件类型 |
| `timestamp` | 否 | `Timestamp` | 事件时间 |
| `toolCallId` | 是 | 字符串 | 工具调用 ID |
| `delta` | 是 | 字符串 | 增量内容 |

### `ToolCallEndEvent`：工具参数结束事件

| 字段 | 必填 | 类型或允许值 | 含义 |
| --- | --- | --- | --- |
| `type` | 是 | 固定值 `TOOL_CALL_END` | 事件类型 |
| `timestamp` | 否 | `Timestamp` | 事件时间 |
| `toolCallId` | 是 | 字符串 | 工具调用 ID |

### `ToolCallResultEvent`：工具结果事件

| 字段 | 必填 | 类型或允许值 | 含义 |
| --- | --- | --- | --- |
| `type` | 是 | 固定值 `TOOL_CALL_RESULT` | 事件类型 |
| `timestamp` | 否 | `Timestamp` | 事件时间 |
| `messageId` | 是 | 字符串 | 消息 ID |
| `toolCallId` | 是 | 字符串 | 工具调用 ID |
| `role` | 是 | 固定值 `tool` | 消息角色 |
| `content` | 是 | `Json` | 内容 |
| `metadata` | 否 | `JsonObject` | 元数据 |
| `display` | 否 | `ToolDisplay` | 展示信息 |

### `ActivitySnapshotEvent`：活动快照事件

| 字段 | 必填 | 类型或允许值 | 含义 |
| --- | --- | --- | --- |
| `type` | 是 | 固定值 `ACTIVITY_SNAPSHOT` | 事件类型 |
| `timestamp` | 否 | `Timestamp` | 事件时间 |
| `messageId` | 是 | 字符串 | 消息 ID |
| `activityType` | 是 | 固定值 `TOOL` | 参见原始 Schema 说明 |
| `content` | 是 | `ToolActivity` | 内容 |
| `display` | 否 | `ToolDisplay` | 展示信息 |

### `ActivityDeltaEvent`：活动增量事件

| 字段 | 必填 | 类型或允许值 | 含义 |
| --- | --- | --- | --- |
| `type` | 是 | 固定值 `ACTIVITY_DELTA` | 事件类型 |
| `timestamp` | 否 | `Timestamp` | 事件时间 |
| `messageId` | 是 | 字符串 | 消息 ID |
| `activityType` | 是 | 固定值 `TOOL` | 参见原始 Schema 说明 |
| `patch` | 是 | 数组〈`ActivityPatch`〉 | 参见原始 Schema 说明 |
| `display` | 否 | `ToolDisplay` | 展示信息 |

### `ToolActivity`：工具活动

| 字段 | 必填 | 类型或允许值 | 含义 |
| --- | --- | --- | --- |
| `toolCallId` | 是 | 字符串 | 工具调用 ID |
| `tool` | 是 | 字符串 | 参见原始 Schema 说明 |
| `progress` | 是 | 数字 | 进度 |
| `output` | 是 | 数组〈`Json`〉 | 参见原始 Schema 说明 |

### `ActivityPatch`：活动补丁

类型：对象 / 对象。

### `CompactionStartEvent`：压缩开始事件

| 字段 | 必填 | 类型或允许值 | 含义 |
| --- | --- | --- | --- |
| `type` | 是 | 固定值 `COMPACTION_START` | 事件类型 |
| `timestamp` | 否 | `Timestamp` | 事件时间 |
| `messageId` | 是 | 字符串 | 消息 ID |
| `beforeMessageId` | 是 | 字符串 | 参见原始 Schema 说明 |

### `CompactionEndEvent`：压缩结束事件

| 字段 | 必填 | 类型或允许值 | 含义 |
| --- | --- | --- | --- |
| `type` | 是 | 固定值 `COMPACTION_END` | 事件类型 |
| `timestamp` | 否 | `Timestamp` | 事件时间 |
| `messageId` | 是 | 字符串 | 消息 ID |
| `beforeMessageId` | 是 | 字符串 | 参见原始 Schema 说明 |
| `outcome` | 是 | `completed`、`failed` / 字符串 | 参见原始 Schema 说明 |
| `marker` | 否 | `MessageView` | 参见原始 Schema 说明 |

### `RunFinishedEvent`：运行结束事件

| 字段 | 必填 | 类型或允许值 | 含义 |
| --- | --- | --- | --- |
| `type` | 是 | 固定值 `RUN_FINISHED` | 事件类型 |
| `timestamp` | 否 | `Timestamp` | 事件时间 |
| `threadId` | 是 | 字符串 | 聊天 ID |
| `runId` | 是 | 字符串 | 运行 ID |
| `metadata` | 是 | `RunMetadata` | 元数据 |

### `RunErrorEvent`：运行错误事件

| 字段 | 必填 | 类型或允许值 | 含义 |
| --- | --- | --- | --- |
| `type` | 是 | 固定值 `RUN_ERROR` | 事件类型 |
| `timestamp` | 否 | `Timestamp` | 事件时间 |
| `threadId` | 否 | 字符串 | 聊天 ID |
| `runId` | 否 | 字符串 | 运行 ID |
| `code` | 是 | `ErrorCode` | 错误码 |
| `message` | 是 | 字符串 | 错误说明 |
| `action` | 是 | `ErrorAction` | 处理动作 |
| `kind` | 否 | 字符串 | 操作或类别 |
| `retryAfter` | 否 | 整数 | 建议等待秒数 |
| `resetsAt` | 否 | `Timestamp` | 额度恢复时间 |
| `status` | 否 | 整数 | 状态 |
| `keyId` | 否 | 字符串 | 密钥 ID |
| `upstreamCode` | 否 | 字符串 | 上游错误码 |
| `rateLimit` | 否 | `RateLimit` | 额度信息 |
| `metadata` | 否 | `RunMetadata` | 元数据 |

### `RunMetadata`：运行元数据

| 字段 | 必填 | 类型或允许值 | 含义 |
| --- | --- | --- | --- |
| `messages` | 否 | 数组〈`CommittedReply`〉 | 消息列表 |
| `thread` | 否 | `ThreadRecord` | 聊天记录 |
| `usage` | 否 | 对象 | 用量 |
| `rateLimit` | 否 | `RateLimit` | 额度信息 |
| `cancelled` | 否 | 布尔值 | 是否取消 |
| `truncated` | 否 | 布尔值 | 是否截断 |

### `CommittedReply`：已提交回复

| 字段 | 必填 | 类型或允许值 | 含义 |
| --- | --- | --- | --- |
| `id` | 是 | 字符串 | 标识符 |
| `model` | 否 | 字符串 | 模型 ID |
| `modelDisplayName` | 否 | 字符串 | 参见原始 Schema 说明 |
| `usage` | 否 | `MessageUsage` | 用量 |
| `isError` | 否 | 布尔值 | 参见原始 Schema 说明 |
| `isInterrupted` | 否 | 布尔值 | 参见原始 Schema 说明 |
| `annotations` | 否 | 数组〈`Annotation`〉 | 引用标注 |
| `research` | 否 | `Research` | 深度研究 |

### `TitleState`：标题状态

类型：`placeholder`、`generated`、`manual` / 字符串。

### `ContextUsage`：上下文用量

| 字段 | 必填 | 类型或允许值 | 含义 |
| --- | --- | --- | --- |
| `usedTokens` | 是 | 整数 | 参见原始 Schema 说明 |
| `limitTokens` | 是 | 整数 | 参见原始 Schema 说明 |
| `percentage` | 是 | 数字 | 参见原始 Schema 说明 |

### `ThreadSummary`：聊天摘要

| 字段 | 必填 | 类型或允许值 | 含义 |
| --- | --- | --- | --- |
| `id` | 是 | 字符串 | 标识符 |
| `title` | 是 | 字符串 | 标题 |
| `titleState` | 是 | `TitleState` | 标题状态 |
| `createdAt` | 是 | 字符串 / null | 创建时间 |
| `updatedAt` | 是 | 字符串 / null | 更新时间 |
| `activityAt` | 是 | 字符串 | 最近活动时间 |
| `projectId` | 是 | 字符串 / null | 项目 ID |
| `model` | 是 | 字符串 | 模型 ID |
| `webSearchEnabled` | 是 | 布尔值 | 聊天的联网搜索设置 |
| `pinned` | 是 | 布尔值 | 是否置顶 |
| `contextUsage` | 是 | 对象 / null | 上下文用量 |
| `activeRun` | 是 | 对象 / null | 当前运行 |
| `messageCount` | 是 | 整数 | 参见原始 Schema 说明 |
| `flagged` | 否 | 布尔值 | 是否标记 |

### `ThreadWindow`：聊天消息窗口

| 字段 | 必填 | 类型或允许值 | 含义 |
| --- | --- | --- | --- |
| `id` | 是 | 字符串 | 标识符 |
| `title` | 是 | 字符串 | 标题 |
| `titleState` | 是 | `TitleState` | 标题状态 |
| `createdAt` | 是 | 字符串 / null | 创建时间 |
| `updatedAt` | 是 | 字符串 / null | 更新时间 |
| `activityAt` | 是 | 字符串 | 最近活动时间 |
| `projectId` | 是 | 字符串 / null | 项目 ID |
| `model` | 是 | 字符串 | 模型 ID |
| `webSearchEnabled` | 是 | 布尔值 | 聊天的联网搜索设置 |
| `pinned` | 是 | 布尔值 | 是否置顶 |
| `contextUsage` | 是 | 对象 / null | 上下文用量 |
| `activeRun` | 是 | 对象 / null | 当前运行 |
| `messageCount` | 是 | 整数 | 参见原始 Schema 说明 |
| `flagged` | 否 | 布尔值 | 是否标记 |
| `revision` | 是 | 字符串 | 修订版本 |
| `presetId` | 是 | 字符串 / null | 预设 ID |
| `messages` | 是 | 数组〈`MessageView`〉 | 消息列表 |
| `hasOlder` | 是 | 布尔值 | 是否还有更早消息 |

### `MessageView`：消息视图

| 字段 | 必填 | 类型或允许值 | 含义 |
| --- | --- | --- | --- |
| `id` | 是 | 字符串 | 标识符 |
| `role` | 是 | `Role` | 消息角色 |
| `createdAt` | 是 | 字符串 | 创建时间 |
| `content` | 否 | 字符串 | 内容 |
| `timeline` | 否 | 数组〈`Block`〉 | 回复时间线 |
| `quote` | 否 | 字符串 | 引用内容 |
| `model` | 否 | 字符串 | 模型 ID |
| `modelDisplayName` | 否 | 字符串 | 参见原始 Schema 说明 |
| `isError` | 否 | 布尔值 | 参见原始 Schema 说明 |
| `isInterrupted` | 否 | 布尔值 | 参见原始 Schema 说明 |
| `usage` | 否 | `MessageUsage` | 用量 |
| `annotations` | 否 | 数组〈`Annotation`〉 | 引用标注 |
| `attachments` | 否 | 数组〈`AttachmentView`〉 | 附件 |
| `answers` | 否 | 数组〈`WidgetAnswers`〉 | 参见原始 Schema 说明 |
| `compaction` | 否 | `Compaction` | 参见原始 Schema 说明 |
| `research` | 否 | `Research` | 深度研究 |

### `Research`：深度研究信息

| 字段 | 必填 | 类型或允许值 | 含义 |
| --- | --- | --- | --- |
| `version` | 是 | 整数 | 参见原始 Schema 说明 |

### `Compaction`：压缩信息

| 字段 | 必填 | 类型或允许值 | 含义 |
| --- | --- | --- | --- |
| `messages` | 是 | 整数 | 消息列表 |
| `startedAt` | 是 | 数字 | 参见原始 Schema 说明 |
| `endedAt` | 是 | 数字 | 参见原始 Schema 说明 |
| `model` | 是 | 字符串 | 模型 ID |
| `version` | 是 | 整数 | 参见原始 Schema 说明 |

### `WidgetAnswers`：组件回答

| 字段 | 必填 | 类型或允许值 | 含义 |
| --- | --- | --- | --- |
| `toolCallId` | 是 | 字符串 | 工具调用 ID |
| `title` | 否 | 字符串 | 标题 |
| `questions` | 是 | 数组〈`AnsweredQuestion`〉 | 参见原始 Schema 说明 |

### `AnsweredQuestion`：已回答问题

| 字段 | 必填 | 类型或允许值 | 含义 |
| --- | --- | --- | --- |
| `id` | 是 | 字符串 | 标识符 |
| `prompt` | 是 | 字符串 | 参见原始 Schema 说明 |
| `answered` | 是 | 布尔值 | 参见原始 Schema 说明 |
| `selected` | 是 | 数组〈`QuestionOption`〉 | 参见原始 Schema 说明 |

### `QuestionOption`：问题选项

| 字段 | 必填 | 类型或允许值 | 含义 |
| --- | --- | --- | --- |
| `id` | 是 | 字符串 | 标识符 |
| `label` | 是 | 字符串 | 参见原始 Schema 说明 |

### `Role`：消息角色

类型：`user`、`assistant`、`compaction` / 字符串。

### `MessageUsage`：消息用量

| 字段 | 必填 | 类型或允许值 | 含义 |
| --- | --- | --- | --- |
| `promptTokens` | 是 | 整数 | 提示词 Token 数 |
| `completionTokens` | 是 | 整数 | 生成 Token 数 |

### `Block`：回复时间线步骤

| 字段 | 必填 | 类型或允许值 | 含义 |
| --- | --- | --- | --- |
| `id` | 是 | 字符串 | 标识符 |
| `type` | 是 | `content`、`thinking`、`tool_call` / 字符串 | 事件类型 |
| `content` | 否 | 字符串 | 内容 |
| `toolCallId` | 否 | 字符串 | 工具调用 ID |
| `name` | 否 | 字符串 | 名称 |
| `arguments` | 否 | 字符串 | 参见原始 Schema 说明 |
| `complete` | 否 | 布尔值 | 参见原始 Schema 说明 |
| `result` | 否 | `Json` | 结果 |
| `metadata` | 否 | `Json` | 元数据 |
| `progress` | 否 | 数组〈`Json`〉 | 进度 |
| `activity` | 否 | `Json` | 参见原始 Schema 说明 |
| `display` | 否 | `ToolDisplay` | 展示信息 |
| `startedAt` | 否 | 数字 | 参见原始 Schema 说明 |
| `endedAt` | 否 | 数字 | 参见原始 Schema 说明 |
| `duration` | 否 | 数字 | 参见原始 Schema 说明 |
| `answers` | 否 | 对象 | 参见原始 Schema 说明 |
| `invalidArguments` | 否 | 布尔值 | 参见原始 Schema 说明 |

### `ToolDisplay`：工具展示数据

| 字段 | 必填 | 类型或允许值 | 含义 |
| --- | --- | --- | --- |
| `query` | 否 | 字符串 | 查询词 |
| `url` | 否 | 字符串 | 网址 |
| `status` | 是 | `running`、`completed`、`failed` / 字符串 | 状态 |
| `sources` | 是 | 数组〈对象〉 | 来源列表 |

### `Annotation`：引用标注

| 字段 | 必填 | 类型或允许值 | 含义 |
| --- | --- | --- | --- |
| `type` | 是 | 固定值 `url_citation` | 事件类型 |
| `url_citation` | 否 | 对象 | 参见原始 Schema 说明 |

### `AttachmentKind`：附件类型

类型：`image`、`document`、`audio` / 字符串。

### `AttachmentView`：附件视图

| 字段 | 必填 | 类型或允许值 | 含义 |
| --- | --- | --- | --- |
| `id` | 是 | 字符串 | 标识符 |
| `kind` | 是 | `AttachmentKind` | 操作或类别 |
| `fileName` | 是 | 字符串 | 文件名 |
| `mimeType` | 是 | 字符串 | 媒体类型 |
| `size` | 否 | 整数 | 文件大小 |
| `thumbnail` | 否 | 字符串 | 缩略图 |
| `textContent` | 否 | 字符串 | 提取的文字 |
| `description` | 否 | 字符串 | 说明 |

### `Attachment`：附件记录

| 字段 | 必填 | 类型或允许值 | 含义 |
| --- | --- | --- | --- |
| `id` | 是 | 字符串 | 标识符 |
| `kind` | 是 | `AttachmentKind` | 操作或类别 |
| `fileName` | 是 | 字符串 | 文件名 |
| `mimeType` | 是 | 字符串 | 媒体类型 |
| `size` | 否 | 整数 | 文件大小 |
| `thumbnail` | 否 | 字符串 | 缩略图 |
| `textContent` | 否 | 字符串 | 提取的文字 |
| `description` | 否 | 字符串 | 说明 |
| `pages` | 否 | 数组〈`DocumentPage`〉 | 文档页面 |
| `base64` | 否 | 字符串 | Base64 数据 |
| `originalBase64` | 否 | 字符串 | 参见原始 Schema 说明 |

### `DocumentPage`：文档页面

| 字段 | 必填 | 类型或允许值 | 含义 |
| --- | --- | --- | --- |
| `image` | 是 | 字符串 | 图片 |
| `is_scanned` | 是 | 布尔值 | 参见原始 Schema 说明 |
| `page` | 是 | 整数 | 参见原始 Schema 说明 |
| `text` | 是 | 字符串 | 文字 |

### `ThreadRecord`：聊天持久化记录

| 字段 | 必填 | 类型或允许值 | 含义 |
| --- | --- | --- | --- |
| `id` | 是 | 字符串 | 标识符 |
| `title` | 是 | 字符串 | 标题 |
| `titleState` | 是 | `TitleState` | 标题状态 |
| `createdAt` | 否 | 字符串 | 创建时间 |
| `updatedAt` | 否 | 字符串 | 更新时间 |
| `model` | 是 | 字符串 | 模型 ID |
| `projectId` | 否 | 字符串 / null | 项目 ID |
| `presetId` | 是 | 字符串 / null | 预设 ID |
| `pinned` | 是 | 布尔值 | 是否置顶 |
| `webSearchEnabled` | 是 | 布尔值 | 聊天的联网搜索设置 |
| `messages` | 是 | 数组〈`MessageRecord`〉 | 消息列表 |
| `lastRequestId` | 否 | 字符串 | 参见原始 Schema 说明 |
| `lastRequestKind` | 否 | 字符串 | 参见原始 Schema 说明 |
| `activeRun` | 否 | 对象 / null | 当前运行 |
| `contextUsage` | 否 | `ContextUsage` | 上下文用量 |
| `flagged` | 否 | 布尔值 | 是否标记 |
| `harnessHiddenCount` | 否 | 整数 | 参见原始 Schema 说明 |
| `recordVersion` | 是 | 整数 | 记录版本 |

### `MessageRecord`：消息持久化记录

| 字段 | 必填 | 类型或允许值 | 含义 |
| --- | --- | --- | --- |
| `id` | 是 | 字符串 | 标识符 |
| `role` | 是 | `Role` | 消息角色 |
| `createdAt` | 是 | 字符串 | 创建时间 |
| `content` | 是 | 字符串 | 内容 |
| `quote` | 否 | 字符串 | 引用内容 |
| `model` | 否 | 字符串 | 模型 ID |
| `modelDisplayName` | 否 | 字符串 | 参见原始 Schema 说明 |
| `timeline` | 否 | 数组〈`Block`〉 | 回复时间线 |
| `attachments` | 否 | 数组〈`Attachment`〉 | 附件 |
| `annotations` | 否 | 数组〈`Annotation`〉 | 引用标注 |
| `usage` | 否 | `MessageUsage` | 用量 |
| `isError` | 否 | 布尔值 | 参见原始 Schema 说明 |
| `isInterrupted` | 否 | 布尔值 | 参见原始 Schema 说明 |
| `answers` | 否 | 数组〈`WidgetAnswers`〉 | 参见原始 Schema 说明 |
| `compaction` | 否 | `Compaction` | 参见原始 Schema 说明 |
| `research` | 否 | `Research` | 深度研究 |

### `ThreadInput`：设备聊天输入

| 字段 | 必填 | 类型或允许值 | 含义 |
| --- | --- | --- | --- |
| `recordVersion` | 否 | 整数 | 记录版本 |
| `title` | 否 | 字符串 | 标题 |
| `titleState` | 否 | 字符串 | 标题状态 |
| `model` | 否 | 字符串 | 模型 ID |
| `lastRequestId` | 否 | 字符串 | 参见原始 Schema 说明 |
| `pinned` | 否 | 布尔值 | 是否置顶 |
| `webSearchEnabled` | 否 | 布尔值 | 聊天的联网搜索设置 |
| `presetId` | 否 | 字符串 / null | 预设 ID |
| `messages` | 否 | 数组〈对象〉 | 消息列表 |

### `MessageRequest`：消息操作请求

| 字段 | 必填 | 类型或允许值 | 含义 |
| --- | --- | --- | --- |
| `key` | 否 | 字符串 | 内容密钥 |
| `local` | 否 | 布尔值 | 设备本地聊天 |
| `thread` | 否 | `ThreadInput` | 聊天记录 |
| `profile` | 否 | `ProfileInput` | 账户设置 |
| `threadId` | 是 | `Identifier` | 聊天 ID |
| `messageId` | 是 | `Identifier` | 消息 ID |
| `content` | 否 | 字符串 | 内容 |

### `DeviceThreadReply`：设备聊天返回

| 字段 | 必填 | 类型或允许值 | 含义 |
| --- | --- | --- | --- |
| `thread` | 是 | `ThreadRecord` | 聊天记录 |

### `SharedThread`：分享聊天

| 字段 | 必填 | 类型或允许值 | 含义 |
| --- | --- | --- | --- |
| `title` | 是 | 字符串 | 标题 |
| `createdAt` | 是 | 字符串 / null | 创建时间 |
| `messages` | 是 | 数组〈对象〉 | 消息列表 |

### `AttachmentRequest`：附件请求

| 字段 | 必填 | 类型或允许值 | 含义 |
| --- | --- | --- | --- |
| `type` | 否 | 字符串 | 事件类型 |
| `key` | 否 | 字符串 | 内容密钥 |
| `id` | 是 | `Identifier` | 标识符 |
| `threadId` | 否 | `Identifier` | 聊天 ID |
| `attKey` | 否 | 字符串 | 参见原始 Schema 说明 |
| `kind` | 否 | `AttachmentKind` | 操作或类别 |
| `fileName` | 否 | 字符串 | 文件名 |
| `mimeType` | 否 | 字符串 | 媒体类型 |
| `textContent` | 否 | 字符串 | 提取的文字 |
| `pages` | 否 | 数组〈`DocumentPage`〉 | 文档页面 |

### `Profile`：账户设置

| 字段 | 必填 | 类型或允许值 | 含义 |
| --- | --- | --- | --- |
| `themeMode` | 否 | `light`、`dark`、`system` / 字符串 | 参见原始 Schema 说明 |
| `chatFont` | 否 | `system`、`serif`、`mono`、`dyslexic` / 字符串 | 参见原始 Schema 说明 |
| `language` | 否 | 字符串 | 参见原始 Schema 说明 |
| `nickname` | 否 | 字符串 | 参见原始 Schema 说明 |
| `profession` | 否 | 字符串 | 参见原始 Schema 说明 |
| `traits` | 否 | 数组〈字符串〉 | 参见原始 Schema 说明 |
| `additionalContext` | 否 | 字符串 | 参见原始 Schema 说明 |
| `isUsingPersonalization` | 否 | 布尔值 | 参见原始 Schema 说明 |
| `isUsingCustomPrompt` | 否 | 布尔值 | 参见原始 Schema 说明 |
| `customSystemPrompt` | 否 | 字符串 | 参见原始 Schema 说明 |
| `customPromptPresets` | 否 | 数组〈`CustomPreset`〉 | 参见原始 Schema 说明 |
| `favoritePromptPresetIds` | 否 | 数组〈字符串〉 | 参见原始 Schema 说明 |
| `defaultPromptPresetId` | 否 | 字符串 | 参见原始 Schema 说明 |
| `selectedModel` | 否 | 字符串 | 已选模型 |
| `autoIntelligence` | 否 | 字符串 | 自动智能等级 |
| `reasoningEffort` | 否 | 字符串 | 思考强度 |
| `thinkingEnabled` | 否 | 布尔值 | 参见原始 Schema 说明 |
| `webSearchEnabled` | 否 | 布尔值 | 聊天的联网搜索设置 |
| `webSearchAvailable` | 否 | 布尔值 | 联网搜索可用状态 |
| `piiCheckEnabled` | 否 | 布尔值 | 参见原始 Schema 说明 |
| `genUIEnabled` | 否 | 布尔值 | 参见原始 Schema 说明 |
| `pixelateSidebarChatTitlesEnabled` | 否 | 布尔值 | 参见原始 Schema 说明 |
| `browserTabChatTitleEnabled` | 否 | 布尔值 | 参见原始 Schema 说明 |
| `enterToNewlineEnabled` | 否 | 布尔值 | 参见原始 Schema 说明 |
| `readAloudSpeed` | 否 | 数字 | 参见原始 Schema 说明 |
| `hasSeenOnboarding` | 否 | 布尔值 | 参见原始 Schema 说明 |
| `hasSeenWebSearchIntro` | 否 | 布尔值 | 参见原始 Schema 说明 |
| `dismissed` | 否 | 对象 | 参见原始 Schema 说明 |

### `ProfileView`：账户设置视图

| 字段 | 必填 | 类型或允许值 | 含义 |
| --- | --- | --- | --- |
| `themeMode` | 否 | `light`、`dark`、`system` / 字符串 | 参见原始 Schema 说明 |
| `chatFont` | 否 | `system`、`serif`、`mono`、`dyslexic` / 字符串 | 参见原始 Schema 说明 |
| `language` | 否 | 字符串 | 参见原始 Schema 说明 |
| `nickname` | 否 | 字符串 | 参见原始 Schema 说明 |
| `profession` | 否 | 字符串 | 参见原始 Schema 说明 |
| `traits` | 否 | 数组〈字符串〉 | 参见原始 Schema 说明 |
| `additionalContext` | 否 | 字符串 | 参见原始 Schema 说明 |
| `isUsingPersonalization` | 否 | 布尔值 | 参见原始 Schema 说明 |
| `isUsingCustomPrompt` | 否 | 布尔值 | 参见原始 Schema 说明 |
| `customSystemPrompt` | 否 | 字符串 | 参见原始 Schema 说明 |
| `customPromptPresets` | 否 | 数组〈`CustomPreset`〉 | 参见原始 Schema 说明 |
| `favoritePromptPresetIds` | 否 | 数组〈字符串〉 | 参见原始 Schema 说明 |
| `defaultPromptPresetId` | 否 | 字符串 | 参见原始 Schema 说明 |
| `selectedModel` | 否 | 字符串 | 已选模型 |
| `autoIntelligence` | 否 | 字符串 | 自动智能等级 |
| `reasoningEffort` | 否 | 字符串 | 思考强度 |
| `thinkingEnabled` | 否 | 布尔值 | 参见原始 Schema 说明 |
| `webSearchEnabled` | 否 | 布尔值 | 聊天的联网搜索设置 |
| `webSearchAvailable` | 否 | 布尔值 | 联网搜索可用状态 |
| `piiCheckEnabled` | 否 | 布尔值 | 参见原始 Schema 说明 |
| `genUIEnabled` | 否 | 布尔值 | 参见原始 Schema 说明 |
| `pixelateSidebarChatTitlesEnabled` | 否 | 布尔值 | 参见原始 Schema 说明 |
| `browserTabChatTitleEnabled` | 否 | 布尔值 | 参见原始 Schema 说明 |
| `enterToNewlineEnabled` | 否 | 布尔值 | 参见原始 Schema 说明 |
| `readAloudSpeed` | 否 | 数字 | 参见原始 Schema 说明 |
| `hasSeenOnboarding` | 否 | 布尔值 | 参见原始 Schema 说明 |
| `hasSeenWebSearchIntro` | 否 | 布尔值 | 参见原始 Schema 说明 |
| `dismissed` | 否 | 对象 | 参见原始 Schema 说明 |
| `updatedAt` | 否 | `Timestamp` | 更新时间 |

### `ProfileInput`：账户设置输入

类型：对象。

### `CustomPreset`：自定义预设

| 字段 | 必填 | 类型或允许值 | 含义 |
| --- | --- | --- | --- |
| `id` | 是 | 字符串 | 标识符 |
| `name` | 是 | 字符串 | 名称 |
| `description` | 否 | 字符串 | 说明 |
| `systemPrompt` | 否 | 字符串 | 参见原始 Schema 说明 |
| `model` | 否 | 字符串 | 模型 ID |
| `webSearchEnabled` | 否 | 布尔值 | 聊天的联网搜索设置 |
| `createdAt` | 否 | `Json` | 创建时间 |
| `updatedAt` | 否 | `Json` | 更新时间 |

### `PresetUpdate`：预设更新

| 字段 | 必填 | 类型或允许值 | 含义 |
| --- | --- | --- | --- |
| `id` | 是 | 字符串 | 标识符 |
| `name` | 否 | 字符串 | 名称 |
| `description` | 否 | 字符串 | 说明 |
| `systemPrompt` | 否 | 字符串 | 参见原始 Schema 说明 |
| `model` | 否 | 字符串 / null | 模型 ID |
| `webSearchEnabled` | 否 | 布尔值 / null | 聊天的联网搜索设置 |
| `updatedAt` | 否 | `Json` | 更新时间 |

### `Project`：项目

| 字段 | 必填 | 类型或允许值 | 含义 |
| --- | --- | --- | --- |
| `id` | 是 | 字符串 | 标识符 |
| `name` | 是 | 字符串 | 名称 |
| `description` | 是 | 字符串 | 说明 |
| `systemInstructions` | 是 | 字符串 | 参见原始 Schema 说明 |
| `color` | 是 | 字符串 / null | 参见原始 Schema 说明 |
| `createdAt` | 否 | `Timestamp` | 创建时间 |
| `updatedAt` | 否 | `Timestamp` | 更新时间 |

### `ProjectDocument`：项目文档

| 字段 | 必填 | 类型或允许值 | 含义 |
| --- | --- | --- | --- |
| `id` | 是 | 字符串 | 标识符 |
| `projectId` | 是 | 字符串 | 项目 ID |
| `filename` | 是 | 字符串 | 文件名 |
| `contentType` | 是 | 字符串 | 参见原始 Schema 说明 |
| `sizeBytes` | 是 | 整数 | 参见原始 Schema 说明 |
| `content` | 否 | 字符串 | 内容 |
| `thumbnailBase64` | 否 | 字符串 | 参见原始 Schema 说明 |
| `createdAt` | 否 | `Timestamp` | 创建时间 |
| `updatedAt` | 否 | `Timestamp` | 更新时间 |

### `KeyOrigin`：密钥来源

类型：`passkey`、`manual`、`recovery`、`start_fresh` / 字符串。

### `KeyRef`：密钥引用

| 字段 | 必填 | 类型或允许值 | 含义 |
| --- | --- | --- | --- |
| `key` | 是 | 字符串 | 内容密钥 |

### `KeyBundleInput`：密钥包输入

| 字段 | 必填 | 类型或允许值 | 含义 |
| --- | --- | --- | --- |
| `credential_id` | 是 | 字符串 | 参见原始 Schema 说明 |
| `kek_iv` | 是 | 字符串 | 参见原始 Schema 说明 |
| `encrypted_keys` | 是 | 字符串 | 参见原始 Schema 说明 |

### `KeyCurrent`：当前内容密钥

| 字段 | 必填 | 类型或允许值 | 含义 |
| --- | --- | --- | --- |
| `key_id` | 是 | 字符串 / null | 参见原始 Schema 说明 |
| `etag` | 否 | 字符串 | 参见原始 Schema 说明 |
| `bundles` | 是 | 对象 / null | 参见原始 Schema 说明 |
| `created_via` | 否 | `KeyOrigin` | 参见原始 Schema 说明 |
| `created_at` | 否 | 字符串 | 参见原始 Schema 说明 |
| `has_data` | 否 | 布尔值 | 参见原始 Schema 说明 |

### `ResealReport`：重新封存报告

| 字段 | 必填 | 类型或允许值 | 含义 |
| --- | --- | --- | --- |
| `status` | 是 | `running`、`complete`、`failed` / 字符串 | 状态 |
| `migrated` | 是 | 整数 | 参见原始 Schema 说明 |
| `remaining` | 是 | 整数 | 剩余额度 |
| `blocked` | 是 | 整数 | 参见原始 Schema 说明 |
| `partial` | 是 | 布尔值 | 参见原始 Schema 说明 |

### `ImportLine`：导入结果行

| 字段 | 必填 | 类型或允许值 | 含义 |
| --- | --- | --- | --- |
| `thread` | 否 | `ThreadRecord` | 聊天记录 |
| `done` | 否 | 对象 | 参见原始 Schema 说明 |
| `error` | 否 | `ErrorBody` | 错误 |

### `ImportStatus`：导入任务状态

| 字段 | 必填 | 类型或允许值 | 含义 |
| --- | --- | --- | --- |
| `jobId` | 是 | 字符串 | 参见原始 Schema 说明 |
| `status` | 是 | 字符串 | 状态 |
| `phase` | 否 | 字符串 | 参见原始 Schema 说明 |
| `imported` | 是 | 整数 | 参见原始 Schema 说明 |
| `failed` | 是 | 整数 | 参见原始 Schema 说明 |
| `total` | 是 | 整数 | 参见原始 Schema 说明 |
| `counts` | 否 | 对象 | 参见原始 Schema 说明 |
| `warnings` | 否 | 数组〈字符串〉 | 参见原始 Schema 说明 |
| `errors` | 否 | 数组〈字符串〉 | 参见原始 Schema 说明 |
| `failureReason` | 否 | 字符串 | 参见原始 Schema 说明 |
| `failureDetail` | 否 | 字符串 | 参见原始 Schema 说明 |

### `LinkPreview`：链接预览

| 字段 | 必填 | 类型或允许值 | 含义 |
| --- | --- | --- | --- |
| `url` | 是 | 字符串 | 网址 |
| `title` | 是 | 字符串 / null | 标题 |
| `description` | 是 | 字符串 / null | 说明 |
| `siteName` | 是 | 字符串 / null | 参见原始 Schema 说明 |
| `image` | 是 | 字符串 / null | 图片 |

