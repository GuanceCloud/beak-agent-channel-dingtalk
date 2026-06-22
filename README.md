# Beak Agent DingTalk Connector SDK

[中文](README.zh-CN.md)

This is a Go SDK package for connecting DingTalk bot accounts to Beak Channel Gateway.

The package is importable library code for Beak host. It is not a CLI. The SDK does not read user-authored runtime files, does not keep a local state directory, does not own database persistence, and does not ask users to modify files on a server. Beak host owns the client UI, credential persistence, account state persistence, DingTalk Stream connection, event routing, session creation, message writes, agent stream subscription, and connector runtime packaging. The SDK owns DingTalk connector logic: credential schema, DingTalk Stream event normalization, text message dedupe, Stream ACK helper construction, token handling, and text delivery through DingTalk sessionWebhook/Open API.

## Scope

v1 supports:

- Exposing a common `sdk.Connector` through `beakdingtalk.NewConnector()`.
- Credential-based DingTalk bot account onboarding.
- Beak-host-owned credential and connector state persistence.
- Beak-host-owned DingTalk Stream connection, with robot event bodies passed into the SDK.
- Inbound text, markdown, and simple richText messages into Beak sessions.
- Mention normalization where `isInAtList` maps to `mentioned_me` and `isAtAll` maps only to `mention_all`.
- Chat identity normalization where DingTalk `conversationId` becomes `chat_identity.id` and `conversationTitle` becomes the group `chat_display_name`.
- Explicit bot mentions with empty text are still delivered to Beak for follow-up handling.
- Outbound Beak agent text replies through DingTalk `sessionWebhook` when available, with Open API fallback when no valid `sessionWebhook` is stored.
- Common `Acknowledge` surface for Beak host compatibility; current DingTalk SDK returns `unsupported` because no safe user-visible lightweight ACK mode is exposed.
- Access token caching in host-owned account state for Open API fallback.
- DingTalk Stream ACK frame helper for Beak-host-owned Stream runtimes.
- Direct and group chat normalization.
- One Beak session per connected bot account plus group chat.
- One Beak session per connected bot account plus direct chat.
- Multiple bot accounts in the same DingTalk group create or reuse separate Beak sessions.
- One channel-link session per bot account connection, without creating a task.

v1 does not support:

- Media, voice, or typing status.
- AI Card or interactive cards.
- SDK-owned DingTalk Stream WebSocket client.
- Beak host code changes.
- A DingTalk connector CLI.
- SDK-owned local runtime files or local state directories.

## OpenClaw Reference Alignment

The upstream [`DingTalk-Real-AI/dingtalk-openclaw-connector`](https://github.com/DingTalk-Real-AI/dingtalk-openclaw-connector) implementation uses `dingtalk-stream` / `DWClient` in its connection layer. The Stream callback is ACKed immediately, the robot event data is parsed and passed to the message handler, and `sessionWebhook` from the latest event is used for reply-path text delivery when present.

This Go SDK keeps the same platform contract but moves the long-running Stream connection to Beak host. The SDK receives event bodies through `HandleEvent`, stores only DingTalk-domain `sessionWebhook` URLs in account state, and uses them in `Send` before falling back to DingTalk robot Open API.

`sessionWebhook` is DingTalk's temporary reply URL. It is not a Beak inbound webhook endpoint.

## Package Layout

- `sdk`: common Beak Connector Plugin SDK interfaces and message types.
- root package: DingTalk connector implementation.
- `internal/dingtalk`: DingTalk Open API HTTP client and Stream event model.
- `state`: account-scoped connector state helpers.
- `examples/basic`: minimal host-side import skeleton.

## Public Entry

```go
import (
	beakdingtalk "github.com/GuanceCloud/beak-agent-channel-dingtalk"
	"github.com/GuanceCloud/beak-agent-channel-dingtalk/sdk"
)

func DingTalkConnector() sdk.Connector {
	return beakdingtalk.NewConnector()
}
```

The connector also implements `beakdingtalk.EventConnector`:

```go
type EventConnector interface {
	sdk.Connector
	HandleEvent(ctx context.Context, runtime sdk.Runtime, account sdk.ChannelAccount, body []byte) (*beakdingtalk.EventResult, error)
}
```

Beak host can type assert this interface when its DingTalk Stream runtime receives a robot event.

For raw DingTalk HTTP callback handling, the connector also implements:

```go
type WebhookRequestConnector interface {
	sdk.Connector
	HandleWebhookRequest(ctx context.Context, runtime sdk.Runtime, account sdk.ChannelAccount, req *http.Request) (*sdk.WebhookResponse, error)
}
```

`HandleWebhookRequest` verifies DingTalk `timestamp` / `sign` headers using `client_secret`, processes the event body, and returns a platform ack response.

## Credential Schema

`connector.CredentialSchema(ctx)` asks Beak UI to collect:

- `client_id`: required DingTalk application AppKey/clientId.
- `client_secret`: required secret DingTalk application AppSecret/clientSecret.
- `robot_code`: optional robotCode override. Defaults to `client_id`.
- `chatbot_user_id`: optional encrypted chatbot user id for self-echo filtering.
- `chatbot_corp_id`: optional DingTalk corp id metadata for this bot.

Beak host must encrypt credential JSON before storing it. The SDK never writes credential or state to local files.

`ValidateCredential(ctx, req)` exchanges `client_id` / `client_secret` for a DingTalk access token and returns normalized credential/state. It also writes standard `bot_identity` / `bot_identities` entries from `robot_code` when available. Later Stream events can add `chatbot_user_id` to the same standard identity state for self-echo filtering.

## Runtime Boundary

Beak host injects `sdk.Runtime`:

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

`Start(ctx, runtime)` validates account wiring, creates or reuses one channel-link session per account, persists account state, and then returns. Inbound DingTalk events are received by the Beak host DingTalk Stream runtime and passed to `HandleEvent`. `Start` does not launch a CLI, read runtime files, subscribe to Beak agent streams, or own a local event server.

## DingTalk Stream Events

Beak host establishes the DingTalk Stream connection using the saved `client_id` and `client_secret`. When a robot event arrives, pass the event data JSON to the SDK:

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

`HandleEvent` supports:

- DingTalk Stream robot event data.
- DingTalk Stream envelopes: `{"headers":{"messageId":"..."},"data":"{...}"}`.
- `conversationType=1` normalized as direct chat.
- `conversationType=2` normalized as group chat.
- Text, markdown, and simple richText extraction.
- Dedupe by `msgId` or Stream delivery message id.
- DingTalk-domain `sessionWebhook`, `sessionWebhook` expiry, `isInAtList` metadata capture, and standard `mentioned_me` mapping.
- `isAtAll` is reported as `mention_all` only; it does not imply `mentioned_me`.
- Empty text is ignored only when the event did not explicitly mention the current bot.
- Self-echo filtering when `chatbot_user_id` is available.
- `sdk.Gateway.EnsureChatSession` for session creation or reuse.
- `sdk.Gateway.CreateMessage` for Beak message writes.

## Sending Text and Markdown

Gateway can call `connector.Send` to return agent output to DingTalk. `Format` / `Title` are common across all SDKs and should be copied from the host outbound model without platform branching; DingTalk maps `Format="markdown"` to DingTalk markdown messages, while plain text remains the default:

```go
_, err := connector.Send(ctx, runtime, sdk.OutboundMessage{
	AccountUUID: accountUUID,
	ChatType:    sdk.ChatTypeGroup,
	ChatID:      "open-conversation-id",
	Text:        "reply text",
	Format:      "markdown", // optional
	Title:       "Reply",    // optional markdown title
	MessageUUID: messageUUID,
})
```

When `Title` is omitted for markdown output, the SDK derives DingTalk's required non-empty `title` from the first non-empty content line and truncates it to 20 runes. If the content is also empty, it falls back to `Beak`. Beak host still does not need a DingTalk-specific title branch.

If the latest inbound event stored a valid DingTalk-domain `sessionWebhook` for this chat, `connector.Send` replies through that `sessionWebhook`; markdown uses webhook `msgtype=markdown`. Otherwise the SDK gets and caches an access token and sends markdown with `msgKey=sampleMarkdown`:

```text
POST /v1.0/oauth2/accessToken
```

Request body:

```json
{
  "appKey": "<client_id>",
  "appSecret": "<client_secret>"
}
```

Group delivery:

```text
POST /v1.0/robot/groupMessages/send
```

Core body:

```json
{
  "robotCode": "<robot_code_or_client_id>",
  "openConversationId": "<chat_id>",
  "msgKey": "sampleText",
  "msgParam": "{\"content\":\"reply text\"}"
}
```

Direct delivery:

```text
POST /v1.0/robot/oToMessages/batchSend
```

Core body:

```json
{
  "robotCode": "<robot_code_or_client_id>",
  "userIds": ["<chat_id>"],
  "msgKey": "sampleText",
  "msgParam": "{\"content\":\"reply text\"}"
}
```

All delivery calls include:

```text
x-acs-dingtalk-access-token: <access_token>
```

To force Open API delivery even when a valid `sessionWebhook` exists, set `Raw["force_openapi"]=true` on the outbound message.

## Lightweight Acknowledgement

The connector exposes the same `Acknowledge` method as the other SDKs so Beak host can call a common adapter method:

```go
result, err := connector.Acknowledge(ctx, runtime, sdk.OutboundAck{
	AccountUUID: accountUUID,
	ChatType:    sdk.ChatTypeGroup,
	ChatID:      "open-conversation-id",
	Intent:      "processing",
	Action:      "start",
})
```

Current result:

```text
Status="unsupported"
```

DingTalk Stream ACK only confirms event delivery to DingTalk. It is not a user-visible processing hint in the chat, so the SDK does not expose it as an `AckModes` value and does not send a normal text fallback.

## Session Rules

Gateway session identity must include the connected bot account and IM platform chat identity.

Standard session key:

```text
workspace_uuid + platform + account_uuid + chat_type + chat_id
```

Recommended Beak session fields:

```text
platform=dingtalk
session_type=manual
source_type=im_chat
source_id=dingtalk:<account_uuid>:<chat_type>:<chat_id>
```

Direct chat:

```text
chat_type=direct
chat_id=<senderStaffId_or_senderId>
source_id=dingtalk:<account_uuid>:direct:<chat_id>
```

Group chat:

```text
chat_type=group
chat_id=<conversationId>
source_id=dingtalk:<account_uuid>:group:<chat_id>
```

Multiple bot accounts in the same group must produce separate sessions:

```text
source_id=dingtalk:account_a:group:cid_group
source_id=dingtalk:account_b:group:cid_group
```

## State Rules

Beak host stores account state. The SDK reads and writes through `sdk.AccountStore`:

- `channel_link_session`: connection session for this bot account.
- `peer_sessions`: chat identity to Beak session uuid cache.
- `session_webhooks`: chat identity to DingTalk sessionWebhook and expiry cache.
- `inbound_seen`: inbound dedupe keys.
- `sent_beak_messages`: reserved outbound message dedupe state.
- `stream_cursors`: reserved Beak stream cursors.
- `stream_last_event_at` / `stream_last_activity_at`: standard runtime health timestamps written when `HandleEvent` successfully processes an event.
- `access_token` / `access_token_expires_at`: access token cache for Open API fallback.
- `chatbot_user_id` / `chatbot_corp_id`: bot identity observed from DingTalk events.
- `robot_code`: bot robot code used for ownership checks and Open API send.
- `bot_identity` / `bot_identities`: standard bot identity cache used by the unified SDK contract.

`peer_sessions` is chat-scoped. Do not include message id, delivery id, or future thread/topic ids in this cache key.

DingTalk Stream connection ownership stays in Beak host. The SDK does not own the main Stream client, heartbeat, or reconnect loop; host stream runtime should write connection and reconnect health, while the SDK writes event activity timestamps.

## Beak Host Integration

1. Show `CredentialSchema` in the Beak client and let the user fill DingTalk `client_id` and `client_secret`.
2. Encrypt and persist credential JSON in `channel_accounts`.
3. Start connector runtime and call `Start(ctx, runtime)` to create or reuse the channel-link session.
4. Establish DingTalk Stream using account credentials.
5. Locate the matching `channel_account` when a robot event arrives, then call `HandleEvent(ctx, runtime, account, eventBody)`.
6. The SDK normalizes `chat_type`, `chat_id`, `sender_id`, and `text`, then calls Gateway to create or reuse a Beak session.
7. Beak host writes the user message.
8. Beak host may call `Acknowledge(ctx, runtime, ack)` for the common processing hint path; DingTalk currently returns `unsupported`.
9. Beak host triggers the agent.
10. Beak host consumes agent stream text and calls `Send(ctx, runtime, outbound)`.

## Verification

```sh
go test ./...
go build ./...
```
