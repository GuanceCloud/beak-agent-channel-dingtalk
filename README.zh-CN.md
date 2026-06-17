# Beak Agent DingTalk Connector SDK

[English](README.md)

这是一个 Go SDK 包，用于把 Beak Channel Gateway 接入钉钉 bot account。

本仓库提供的是可被 Beak host `import` 的库代码，不是命令行工具。SDK 不读取用户编写的运行时配置文件，不维护本地状态目录，不拥有数据库持久化，也不要求用户登录服务器修改文件。Beak host 负责客户端 UI、credential 持久化、account state 持久化、DingTalk Stream 连接、事件路由、session 创建、message 写入、agent stream 订阅和 connector runtime 打包。SDK 只负责钉钉 connector 逻辑：credential schema、钉钉 Stream 事件标准化、文本消息去重、Stream ACK helper、token 处理，以及通过钉钉 sessionWebhook/Open API 发送文本消息。

## 范围

v1 支持：

- 通过 `beakdingtalk.NewConnector()` 暴露通用 `sdk.Connector` 实现。
- 基于 credential 的钉钉 bot account 接入。
- 由 Beak host 保存 credential 和 connector state。
- 由 Beak host 建立 DingTalk Stream 长连接，并把收到的 robot event body 传给 SDK。
- 文本、markdown、简单 richText 入站到 Beak session。
- mention 标准化中 `isInAtList` 映射为 `mentioned_me`，`isAtAll` 只映射为 `mention_all`。
- chat identity 标准化中，钉钉 `conversationId` 会进入 `chat_identity.id`，`conversationTitle` 会作为群聊 `chat_display_name`。
- 只有明确 @bot 但正文为空的消息，仍会进入 Beak，用于 follow-up。
- Beak agent 文本输出优先通过钉钉 `sessionWebhook` 回发；没有可用 `sessionWebhook` 时 fallback 到 robot Open API。
- Open API fallback 的 access token 会缓存在 host-owned account state 中。
- 提供 DingTalk Stream ACK frame helper，供 Beak-host-owned Stream runtime 使用。
- 单聊和群聊标准化。
- 一个已连接 bot account 中的一个群聊对应一个 Beak session。
- 一个已连接 bot account 中的一个单聊对应一个 Beak session。
- 如果同一个群里接入多个 bot account，每个 bot account 都创建或复用自己的 Beak session。
- 每个 bot account 连接创建一个 channel-link session，但不创建 task。

v1 不支持：

- media、voice、typing status。
- AI Card 或互动卡片。
- SDK 自己维护 DingTalk Stream WebSocket 客户端。
- 修改 Beak host 代码。
- 把钉钉 connector 做成 CLI。
- 让 SDK 维护本地配置文件或本地状态目录。

## OpenClaw 参考实现对齐

上游 [`DingTalk-Real-AI/dingtalk-openclaw-connector`](https://github.com/DingTalk-Real-AI/dingtalk-openclaw-connector) 的连接层使用 `dingtalk-stream` / `DWClient`。Stream callback 会先 ACK，robot event data 再交给 message handler；最近一次事件里的 `sessionWebhook` 会在可用时用于回复链路的文本发送。

本 Go SDK 保留同样的平台契约，但把长连接归属放到 Beak host。SDK 通过 `HandleEvent` 接收 event body，只把钉钉域名下的 `sessionWebhook` 存入 account state，并在 `Send` 时优先使用它；没有可用 `sessionWebhook` 时再 fallback 到钉钉 robot Open API。

`sessionWebhook` 是钉钉提供的临时回复 URL，不是 Beak 入站 webhook endpoint。

## 包结构

- `sdk`：通用 Beak Connector Plugin SDK 接口和消息类型。
- 根包：钉钉 connector 实现。
- `internal/dingtalk`：钉钉 Open API HTTP client 和 Stream event 模型。
- `state`：account 维度的 connector state helper。
- `examples/basic`：最小 host-side import skeleton。

## 公开入口

```go
import (
	beakdingtalk "github.com/GuanceCloud/beak-agent-channel-dingtalk"
	"github.com/GuanceCloud/beak-agent-channel-dingtalk/sdk"
)

func DingTalkConnector() sdk.Connector {
	return beakdingtalk.NewConnector()
}
```

该 connector 同时实现 `beakdingtalk.EventConnector`：

```go
type EventConnector interface {
	sdk.Connector
	HandleEvent(ctx context.Context, runtime sdk.Runtime, account sdk.ChannelAccount, body []byte) (*beakdingtalk.EventResult, error)
}
```

Beak host 在 DingTalk Stream 收到 robot 事件后，可以 type assert 这个接口，把原始 event data 传给 SDK。

如果 host 暴露钉钉 HTTP callback endpoint，connector 也实现：

```go
type WebhookRequestConnector interface {
	sdk.Connector
	HandleWebhookRequest(ctx context.Context, runtime sdk.Runtime, account sdk.ChannelAccount, req *http.Request) (*sdk.WebhookResponse, error)
}
```

`HandleWebhookRequest` 使用 `client_secret` 校验钉钉 `timestamp` / `sign` header，处理 event body，并返回平台 ack response。

## Credential Schema

`connector.CredentialSchema(ctx)` 要求 Beak UI 采集：

- `client_id`：必填，钉钉应用 AppKey/clientId。
- `client_secret`：必填，敏感字段，钉钉应用 AppSecret/clientSecret。
- `robot_code`：可选，发送 robot 消息时使用；不填时默认使用 `client_id`。
- `chatbot_user_id`：可选，当前 bot 的加密 user id，用于过滤 self echo。
- `chatbot_corp_id`：可选，当前 bot 的 corp id 元数据。

Beak host 必须在入库前加密 credential JSON。SDK 不把 credential 或 state 写入本地文件。

`ValidateCredential(ctx, req)` 会用 `client_id` / `client_secret` 换取钉钉 access token，并返回归一化 credential/state。`robot_code` 可用时会写入标准 `bot_identity` / `bot_identities`；后续 Stream event 里的 `chatbot_user_id` 也会补进标准 bot identity state，用于 self-echo 过滤。

## Runtime 边界

Beak host 注入 `sdk.Runtime`：

```go
runtime := sdk.Runtime{
	WorkspaceUUID: workspaceUUID,
	Channel: sdk.Channel{
		UUID:          channelUUID,
		WorkspaceUUID: workspaceUUID,
		Platform:      "dingtalk",
	},
	Accounts:     accounts,
	Gateway:      gateway,
	AccountStore: accountStore,
}
```

`Start(ctx, runtime)` 负责校验 account wiring，为每个 account 创建或复用 channel-link session，保存 account state，然后返回。钉钉入站事件由 Beak host 的 DingTalk Stream runtime 收到后调用 `HandleEvent`。`Start` 不启动 CLI，不读取配置文件，不订阅 Beak agent stream，也不拥有本地事件服务器。

## DingTalk Stream 事件处理

Beak host 负责用用户保存的 `client_id` / `client_secret` 建立 DingTalk Stream 连接。收到 robot event 后，把钉钉返回的 event data JSON 交给 SDK：

```go
connector := beakdingtalk.NewConnector()

eventConnector, ok := any(connector).(beakdingtalk.EventConnector)
if !ok {
	return errors.New("dingtalk connector does not handle events")
}

result, err := eventConnector.HandleEvent(ctx, runtime, account, eventBody)
if err != nil {
	return err
}
if result.Ignored {
	return nil
}
```

`HandleEvent` 支持：

- 钉钉 Stream robot event data。
- 钉钉 Stream envelope：`{"headers":{"messageId":"..."},"data":"{...}"}`。
- `conversationType=1` 标准化为 direct chat。
- `conversationType=2` 标准化为 group chat。
- text、markdown、简单 richText 文本提取。
- 按 `msgId` 或 Stream delivery message id 去重。
- 捕获钉钉域名下的 `sessionWebhook`、`sessionWebhook` 过期时间和 `isInAtList` 元数据，并标准化映射 `mentioned_me`。
- `isAtAll` 只会上报为 `mention_all`，不等价于 `mentioned_me`。
- 只有事件没有明确提及当前 bot 时，空正文才会被忽略。
- 配置 `chatbot_user_id` 时过滤 self echo。
- 通过 `sdk.Gateway.EnsureChatSession` 创建或复用 session。
- 通过 `sdk.Gateway.CreateMessage` 写入 Beak message。

## 发送文本和 Markdown

Gateway 可以通过 `connector.Send` 把 agent 输出发回钉钉。`Format` / `Title` 是三个 SDK 的通用字段，Beak host 应该从统一出站模型逐字段复制，不要按平台分支改写；钉钉会把 `Format="markdown"` 映射成钉钉 markdown 消息，默认仍是纯文本：

```go
_, err := connector.Send(ctx, runtime, sdk.OutboundMessage{
	AccountUUID: accountUUID,
	ChatType:    sdk.ChatTypeGroup,
	ChatID:      "open-conversation-id",
	Text:        "reply text",
	Format:      "markdown", // 可选
	Title:       "Reply",    // 可选 markdown title
	MessageUUID: messageUUID,
})
```

markdown 出站未传 `Title` 时，钉钉发送会使用空标题；SDK 不会从正文内容截断生成标题。

如果最近一次入站事件为该 chat 存过有效且通过域名校验的 `sessionWebhook`，`connector.Send` 会优先用该 `sessionWebhook` 回复；markdown 会使用 webhook `msgtype=markdown`。否则 SDK 会获取并缓存 access token，并用 `msgKey=sampleMarkdown` 发送 markdown：

```text
POST /v1.0/oauth2/accessToken
```

请求 body：

```json
{
  "appKey": "<client_id>",
  "appSecret": "<client_secret>"
}
```

群聊发送：

```text
POST /v1.0/robot/groupMessages/send
```

核心 body：

```json
{
  "robotCode": "<robot_code_or_client_id>",
  "openConversationId": "<chat_id>",
  "msgKey": "sampleText",
  "msgParam": "{\"content\":\"reply text\"}"
}
```

单聊发送：

```text
POST /v1.0/robot/oToMessages/batchSend
```

核心 body：

```json
{
  "robotCode": "<robot_code_or_client_id>",
  "userIds": ["<chat_id>"],
  "msgKey": "sampleText",
  "msgParam": "{\"content\":\"reply text\"}"
}
```

所有发送请求都会带：

```text
x-acs-dingtalk-access-token: <access_token>
```

如果需要强制走 Open API，即使当前 chat 有可用 `sessionWebhook`，可以在 outbound message 设置 `Raw["force_openapi"]=true`。

## Session 规则

Gateway session identity 必须包含已连接 bot account 和 IM 平台 chat identity。

标准 session key：

```text
workspace_uuid + platform + account_uuid + chat_type + chat_id
```

推荐 Beak session 字段：

```text
platform=dingtalk
session_type=manual
source_type=im_chat
source_id=dingtalk:<account_uuid>:<chat_type>:<chat_id>
```

单聊：

```text
chat_type=direct
chat_id=<senderStaffId_or_senderId>
source_id=dingtalk:<account_uuid>:direct:<chat_id>
```

群聊：

```text
chat_type=group
chat_id=<conversationId>
source_id=dingtalk:<account_uuid>:group:<chat_id>
```

同一个群里有多个 bot account 时，必须是多个 session：

```text
source_id=dingtalk:account_a:group:cid_group
source_id=dingtalk:account_b:group:cid_group
```

## State 规则

Beak host 保存 account state，SDK 通过 `sdk.AccountStore` 读取并回写：

- `channel_link_session`：该 bot account 对应的连接 session。
- `peer_sessions`：chat identity 到 Beak session uuid 的缓存。
- `session_webhooks`：chat identity 到钉钉 sessionWebhook 和过期时间的缓存。
- `inbound_seen`：入站消息 dedupe key。
- `sent_beak_messages`：预留给出站 message dedupe。
- `stream_cursors`：预留给 Beak stream cursor。
- `access_token` / `access_token_expires_at`：Open API fallback 使用的 access token 缓存。
- `chatbot_user_id` / `chatbot_corp_id`：从钉钉事件中观察到的 bot identity。
- `robot_code`：用于 account ownership 校验和 Open API 发送的机器人 code。
- `bot_identity` / `bot_identities`：统一 SDK 契约里的标准 bot 身份缓存。

`peer_sessions` 是 chat 维度缓存，不要把 message id、delivery id 或未来可能出现的 thread/topic id 拼进这个 key。

## Beak Host 集成步骤

1. 在 Beak 客户端展示 `CredentialSchema`，让用户填写钉钉 bot 的 `client_id` 和 `client_secret`。
2. Beak host 加密保存 credential JSON 到 `channel_accounts`。
3. Beak host 启动 connector runtime，调用 `Start(ctx, runtime)` 创建或复用 channel-link session。
4. Beak host 用 account credential 建立 DingTalk Stream 连接。
5. 收到 robot event 后，定位对应 `channel_account`，调用 `HandleEvent(ctx, runtime, account, eventBody)`。
6. SDK 标准化出 `chat_type`、`chat_id`、`sender_id`、`text`，并调用 Gateway 创建或复用 Beak session。
7. Beak host 写入 user message 后触发 agent。
8. Beak host 从 agent stream 取到文本回复后调用 `Send(ctx, runtime, outbound)`。

## 验证

```sh
go test ./...
go build ./...
```
