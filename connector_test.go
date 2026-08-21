package beakdingtalk

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/GuanceCloud/beak-agent-channel-dingtalk/internal/dingtalk"
	"github.com/GuanceCloud/beak-agent-channel-dingtalk/sdk"
	beakstate "github.com/GuanceCloud/beak-agent-channel-dingtalk/state"
)

func TestDingTalkConnectorMetadataAndSchema(t *testing.T) {
	var connector sdk.Connector = NewConnector()
	if _, ok := connector.(EventConnector); !ok {
		t.Fatal("NewConnector should expose EventConnector for host-owned DingTalk Stream routing")
	}
	if _, ok := connector.(sdk.HostStreamConnector); !ok {
		t.Fatal("NewConnector should expose HostStreamConnector for host-owned DingTalk Stream routing")
	}
	if _, ok := connector.(WebhookRequestConnector); !ok {
		t.Fatal("NewConnector should expose WebhookRequestConnector for HTTP callback routing")
	}

	metadata := connector.Metadata()
	if metadata.ID != ID || metadata.Platform != Platform || metadata.Label != "DingTalk" {
		t.Fatalf("metadata=%+v", metadata)
	}
	if !metadata.Capabilities.Text || !metadata.Capabilities.DirectChat || !metadata.Capabilities.GroupChat || !metadata.Capabilities.Media {
		t.Fatalf("capabilities=%+v", metadata.Capabilities)
	}
	if !metadata.Capabilities.Stream || metadata.Capabilities.Webhook {
		t.Fatalf("stream/webhook capabilities=%+v", metadata.Capabilities)
	}
	if metadata.Capabilities.RuntimeOwnership != sdk.RuntimeOwnershipHostStream {
		t.Fatalf("runtime ownership=%q, want %q", metadata.Capabilities.RuntimeOwnership, sdk.RuntimeOwnershipHostStream)
	}
	if len(metadata.Capabilities.AckModes) != 0 {
		t.Fatalf("ack modes=%+v", metadata.Capabilities.AckModes)
	}
	if len(metadata.Capabilities.LoginModes) != 1 || metadata.Capabilities.LoginModes[0] != sdk.LoginModeCredential {
		t.Fatalf("login modes=%+v", metadata.Capabilities.LoginModes)
	}

	schema := connector.CredentialSchema(context.Background())
	if schema.Type != "object" || schema.AdditionalProperties {
		t.Fatalf("schema=%+v", schema)
	}
	if len(schema.LoginModes) != 1 || schema.LoginModes[0] != sdk.LoginModeCredential {
		t.Fatalf("schema login modes=%+v", schema.LoginModes)
	}
	if _, ok := schema.Properties["client_id"]; !ok {
		t.Fatalf("missing client_id schema=%+v", schema.Properties)
	}
	if !schema.Properties["client_secret"].Secret {
		t.Fatalf("client_secret should be secret")
	}
	if _, ok := schema.Properties["base_url"]; ok {
		t.Fatalf("base_url must not be exposed in credential schema")
	}
}

func TestDingTalkConnectorAcknowledgeUnsupported(t *testing.T) {
	result, err := NewConnector().Acknowledge(context.Background(), sdk.Runtime{
		Account: sdk.ChannelAccount{
			UUID:     "account-1",
			Platform: "dingtalk",
			Credential: map[string]any{
				"client_id":     "client-1",
				"client_secret": "secret-1",
				"robot_code":    "robot-1",
			},
		},
	}, sdk.OutboundAck{
		AccountUUID:     "account-1",
		ChatType:        sdk.ChatTypeGroup,
		ChatID:          "cid-1",
		TargetMessageID: "msg-1",
		Action:          "start",
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "unsupported" || result.Mode != "unsupported" || result.AccountUUID != "account-1" {
		t.Fatalf("result=%+v", result)
	}
}

func TestDingTalkInboundMediaKindSupportsFiles(t *testing.T) {
	kind, ok := dingtalkInboundMediaKind("file")
	if !ok || kind != sdk.MediaKindFile {
		t.Fatalf("file media kind = %q/%t, want %q/true", kind, ok, sdk.MediaKindFile)
	}
	if _, ok := dingtalkInboundMediaKind("text"); ok {
		t.Fatal("text message must not be treated as an attachment")
	}
}

func TestDownloadDingTalkAttachmentsSupportsRichTextPictureURLAndDownloadCode(t *testing.T) {
	client := dingtalk.NewClient("https://api.dingtalk.test", "client-1", "secret-1", "robot-1")
	client.AccessToken = "token-1"
	client.HTTPClient = &http.Client{Transport: testRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		switch request.URL.String() {
		case "https://api.dingtalk.test/v1.0/robot/messageFiles/download":
			if request.Header.Get("x-acs-dingtalk-access-token") != "token-1" {
				t.Fatalf("download token header = %q", request.Header.Get("x-acs-dingtalk-access-token"))
			}
			return testJSONResponse(map[string]any{"downloadUrl": "https://media.dingtalk.test/report.pdf"})
		case "https://media.dingtalk.test/image.png":
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader("image-data")),
				Header:     make(http.Header),
				Request:    request,
			}, nil
		case "https://media.dingtalk.test/report.pdf":
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader("report-data")),
				Header:     make(http.Header),
				Request:    request,
			}, nil
		default:
			t.Fatalf("download URL = %s", request.URL.String())
			return nil, nil
		}
	})}
	event := &dingtalk.StreamEvent{
		MsgID:   "message-1",
		MsgType: "richText",
		Raw: map[string]any{
			"richText": map[string]any{
				"richTextList": []any{
					map[string]any{"text": "请查看"},
					map[string]any{
						"pictureUrl":  "https://media.dingtalk.test/image.png",
						"fileName":    "inline.png",
						"contentType": "image/png",
					},
					map[string]any{
						"msgType":      "file",
						"downloadCode": "download-report",
						"fileName":     "report.pdf",
						"contentType":  "application/pdf",
					},
				},
			},
		},
	}

	attachments, cleanup, attachmentErrors := downloadDingTalkAttachments(t.Context(), client, event)
	if cleanup == nil {
		t.Fatal("cleanup is nil")
	}
	defer cleanup()
	if len(attachmentErrors) != 0 {
		t.Fatalf("attachment errors = %#v", attachmentErrors)
	}
	if len(attachments) != 2 {
		t.Fatalf("attachments = %#v", attachments)
	}
	attachment := attachments[0]
	if attachment.Kind != sdk.MediaKindImage || attachment.FileName != "inline.png" ||
		attachment.ContentType != "image/png" || attachment.PlatformResourceID != "" {
		t.Fatalf("attachment = %#v", attachment)
	}
	body, err := os.ReadFile(attachment.Path)
	if err != nil || string(body) != "image-data" {
		t.Fatalf("downloaded body = %q error=%v", body, err)
	}
	fileAttachment := attachments[1]
	if fileAttachment.Kind != sdk.MediaKindFile || fileAttachment.FileName != "report.pdf" ||
		fileAttachment.PlatformResourceID != "download-report" {
		t.Fatalf("file attachment = %#v", fileAttachment)
	}
	fileBody, err := os.ReadFile(fileAttachment.Path)
	if err != nil || string(fileBody) != "report-data" {
		t.Fatalf("downloaded file body = %q error=%v", fileBody, err)
	}
	path := attachment.Path
	filePath := fileAttachment.Path
	cleanup()
	cleanup = func() {}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("temporary attachment still exists: %v", err)
	}
	if _, err := os.Stat(filePath); !os.IsNotExist(err) {
		t.Fatalf("temporary file attachment still exists: %v", err)
	}
}

func TestBuildInboundMessageReferencedMessage(t *testing.T) {
	event := &dingtalk.StreamEvent{
		ConversationType:  dingtalk.ConversationTypeGroup,
		ConversationID:    "cid-1",
		ConversationTitle: "ops",
		SenderStaffID:     "staff-1",
		SenderNick:        "Alice",
		MsgID:             "msg-1",
		MsgType:           "text",
		Raw: map[string]any{
			"text": map[string]any{
				"content":    "current",
				"isReplyMsg": true,
				"repliedMsg": map[string]any{
					"msgId":      "quoted-1",
					"msgType":    "text",
					"senderId":   "staff-2",
					"senderNick": "Bob",
					"content": map[string]any{
						"text": "quoted text",
					},
				},
			},
		},
	}
	inbound := BuildInboundMessage("workspace-1", "channel-1", "account-1", event, "current")
	if inbound.Text != "current" {
		t.Fatalf("text=%q", inbound.Text)
	}
	if inbound.ReferencedMessage == nil {
		t.Fatal("referenced_message is nil")
	}
	if inbound.ReferencedMessage.MessageID != "quoted-1" || inbound.ReferencedMessage.Text != "quoted text" || inbound.ReferencedMessage.SenderID != "staff-2" || inbound.ReferencedMessage.SenderDisplayName != "Bob" {
		t.Fatalf("referenced_message=%+v", inbound.ReferencedMessage)
	}
}

func TestBuildInboundMessageReferencedMessageTopLevelReply(t *testing.T) {
	event := &dingtalk.StreamEvent{
		ConversationType:  dingtalk.ConversationTypeGroup,
		ConversationID:    "cid-1",
		ConversationTitle: "ops",
		SenderStaffID:     "staff-1",
		SenderNick:        "Alice",
		MsgID:             "msg-1",
		MsgType:           "reply",
		Raw: map[string]any{
			"content":    "current",
			"isReplyMsg": true,
			"repliedMsg": map[string]any{
				"msgId":      "quoted-1",
				"msgType":    "text",
				"senderId":   "staff-2",
				"senderNick": "Bob",
				"content": map[string]any{
					"text": "quoted text",
				},
			},
		},
	}
	if got := event.Text(); got != "current" {
		t.Fatalf("event text=%q", got)
	}
	inbound := BuildInboundMessage("workspace-1", "channel-1", "account-1", event, event.Text())
	if inbound.ReferencedMessage == nil {
		t.Fatal("referenced_message is nil")
	}
	if inbound.ReferencedMessage.MessageID != "quoted-1" || inbound.ReferencedMessage.Text != "quoted text" {
		t.Fatalf("referenced_message=%+v", inbound.ReferencedMessage)
	}
}

func TestBuildInboundMessageReferencedMessagePlainContentString(t *testing.T) {
	event := &dingtalk.StreamEvent{
		ConversationType:  dingtalk.ConversationTypeGroup,
		ConversationID:    "cid-1",
		ConversationTitle: "ops",
		SenderStaffID:     "staff-1",
		SenderNick:        "Alice",
		MsgID:             "msg-1",
		MsgType:           "reply",
		Raw: map[string]any{
			"content":    "current",
			"isReplyMsg": true,
			"repliedMsg": map[string]any{
				"msgId":      "quoted-1",
				"msgType":    "text",
				"senderId":   "staff-2",
				"senderNick": "Bob",
				"content":    "quoted plain text",
			},
		},
	}
	inbound := BuildInboundMessage("workspace-1", "channel-1", "account-1", event, event.Text())
	if inbound.ReferencedMessage == nil {
		t.Fatal("referenced_message is nil")
	}
	if inbound.ReferencedMessage.Text != "quoted plain text" {
		t.Fatalf("referenced_message=%+v", inbound.ReferencedMessage)
	}
}

func TestBuildInboundMessageReferencedMessageNestedRichText(t *testing.T) {
	event := &dingtalk.StreamEvent{
		ConversationType:  dingtalk.ConversationTypeGroup,
		ConversationID:    "cid-1",
		ConversationTitle: "ops",
		SenderStaffID:     "staff-1",
		SenderNick:        "Alice",
		MsgID:             "msg-1",
		MsgType:           "reply",
		Raw: map[string]any{
			"content":    "current",
			"isReplyMsg": true,
			"repliedMsg": map[string]any{
				"msgId":      "quoted-1",
				"msgType":    "richText",
				"senderId":   "staff-2",
				"senderNick": "Bob",
				"content": map[string]any{
					"richText": map[string]any{
						"richTextList": []any{
							map[string]any{"text": "quoted "},
							map[string]any{"content": "rich text"},
							map[string]any{"text": "ignored", "skillData": map[string]any{"type": "card"}},
						},
					},
				},
			},
		},
	}
	inbound := BuildInboundMessage("workspace-1", "channel-1", "account-1", event, event.Text())
	if inbound.ReferencedMessage == nil {
		t.Fatal("referenced_message is nil")
	}
	if inbound.ReferencedMessage.Text != "quoted rich text" {
		t.Fatalf("referenced_message=%+v", inbound.ReferencedMessage)
	}
}

func TestDingTalkConnectorValidateCredentialFetchesAccessToken(t *testing.T) {
	var sawToken bool
	httpClient := &http.Client{Transport: testRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.URL.Path != "/v1.0/oauth2/accessToken" {
			t.Fatalf("unexpected request: %s %s", req.Method, req.URL.Path)
		}
		sawToken = true
		if req.Method != http.MethodPost {
			t.Fatalf("token method=%s", req.Method)
		}
		var body map[string]string
		if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if body["appKey"] != "client_validate" || body["appSecret"] != "secret_validate" {
			t.Fatalf("token body=%+v", body)
		}
		return testJSONResponse(map[string]any{"accessToken": "access-token-1", "expireIn": 3600})
	})}

	result, err := NewConnector().ValidateCredential(context.Background(), sdk.CredentialValidationRequest{
		Credential: map[string]any{
			"client_id":     "client_validate",
			"client_secret": "secret_validate",
			"robot_code":    "robot_validate",
		},
		Runtime: sdk.Runtime{HTTPClient: httpClient},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !sawToken {
		t.Fatal("token endpoint was not called")
	}
	if !result.Valid || result.AccountKey != "robot_validate" || result.DisplayName != "robot_validate" {
		t.Fatalf("result=%+v", result)
	}
	if result.Credential["robot_code"] != "robot_validate" || result.State["access_token"] != "access-token-1" {
		t.Fatalf("credential=%+v state=%+v", result.Credential, result.State)
	}
	identity, ok := result.State["bot_identity"].(map[string]any)
	if !ok || identity["id"] != "robot_validate" || identity["id_type"] != "robot_code" {
		t.Fatalf("bot_identity=%+v state=%+v", result.State["bot_identity"], result.State)
	}
	if result.Metadata["robot_code"] != "robot_validate" {
		t.Fatalf("metadata=%+v", result.Metadata)
	}
}

func TestDingTalkConnectorValidateCredentialReturnsInvalidOnTokenFailure(t *testing.T) {
	httpClient := &http.Client{Transport: testRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.URL.Path != "/v1.0/oauth2/accessToken" {
			t.Fatalf("unexpected request: %s", req.URL.Path)
		}
		response, err := testJSONResponse(map[string]any{"code": "InvalidAppSecret", "message": "bad secret"})
		response.StatusCode = http.StatusBadRequest
		return response, err
	})}

	result, err := NewConnector().ValidateCredential(context.Background(), sdk.CredentialValidationRequest{
		Credential: map[string]any{"client_id": "client_bad", "client_secret": "bad", "robot_code": "robot_bad"},
		Runtime:    sdk.Runtime{HTTPClient: httpClient},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Valid || result.AccountKey != "robot_bad" || !strings.Contains(result.Error, "bad secret") {
		t.Fatalf("result=%+v", result)
	}
}

func TestDingTalkConnectorValidateCredentialReturnsGoErrorOnTokenTimeout(t *testing.T) {
	var calls int
	httpClient := &http.Client{Transport: testRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.URL.Path != "/v1.0/oauth2/accessToken" {
			t.Fatalf("unexpected request: %s", req.URL.Path)
		}
		calls++
		return nil, &url.Error{Op: "Post", URL: req.URL.String(), Err: context.DeadlineExceeded}
	})}

	result, err := NewConnector().ValidateCredential(context.Background(), sdk.CredentialValidationRequest{
		Credential: map[string]any{
			"client_id":     "client_timeout",
			"client_secret": "secret_timeout",
			"robot_code":    "robot_timeout",
		},
		Runtime: sdk.Runtime{HTTPClient: httpClient},
	})
	if err == nil || !strings.Contains(err.Error(), "timed out while waiting for DingTalk API response") {
		t.Fatalf("result=%+v error=%v, want transient Go error", result, err)
	}
	if result != nil {
		t.Fatalf("result=%+v, want nil on transient failure", result)
	}
	if calls != credentialValidationAttempts {
		t.Fatalf("token calls=%d, want %d", calls, credentialValidationAttempts)
	}
	if strings.Contains(err.Error(), "Client.Timeout exceeded") || strings.Contains(err.Error(), "context deadline exceeded") {
		t.Fatalf("timeout leaked low-level error: %q", err)
	}
}

func TestDingTalkConnectorValidateCredentialRetriesTransientFailure(t *testing.T) {
	var calls int
	httpClient := &http.Client{Transport: testRoundTripFunc(func(*http.Request) (*http.Response, error) {
		calls++
		if calls == 1 {
			response, err := testJSONResponse(map[string]any{"message": "temporary"})
			response.StatusCode = http.StatusServiceUnavailable
			return response, err
		}
		return testJSONResponse(map[string]any{"accessToken": "token-recovered", "expireIn": 3600})
	})}
	result, err := NewConnector().ValidateCredential(context.Background(), sdk.CredentialValidationRequest{
		Credential: map[string]any{"client_id": "client_retry", "client_secret": "secret_retry", "robot_code": "robot_retry"},
		Runtime:    sdk.Runtime{HTTPClient: httpClient},
	})
	if err != nil || result == nil || !result.Valid {
		t.Fatalf("result=%+v error=%v", result, err)
	}
	if calls != 2 {
		t.Fatalf("token calls=%d, want 2", calls)
	}
}

func TestDingTalkConnectorValidateCredentialDoesNotTreatMalformedSuccessAsInvalid(t *testing.T) {
	var calls int
	httpClient := &http.Client{Transport: testRoundTripFunc(func(*http.Request) (*http.Response, error) {
		calls++
		return testJSONResponse(map[string]any{"code": "systemError", "message": "temporary platform failure"})
	})}
	result, err := NewConnector().ValidateCredential(context.Background(), sdk.CredentialValidationRequest{
		Credential: map[string]any{"client_id": "client_malformed", "client_secret": "secret_malformed", "robot_code": "robot_malformed"},
		Runtime:    sdk.Runtime{HTTPClient: httpClient},
	})
	if err == nil || result != nil || !strings.Contains(err.Error(), "systemError") {
		t.Fatalf("result=%+v error=%v, want transient Go error", result, err)
	}
	if calls != credentialValidationAttempts {
		t.Fatalf("token calls=%d, want %d", calls, credentialValidationAttempts)
	}
}

func newTestEventConnector(t *testing.T) EventConnector {
	t.Helper()
	connector, ok := NewConnector().(EventConnector)
	if !ok {
		t.Fatal("NewConnector should expose EventConnector")
	}
	return connector
}

func newTestWebhookRequestConnector(t *testing.T) WebhookRequestConnector {
	t.Helper()
	connector, ok := NewConnector().(WebhookRequestConnector)
	if !ok {
		t.Fatal("NewConnector should expose WebhookRequestConnector")
	}
	return connector
}

func TestDingTalkConnectorStartEnsuresChannelLink(t *testing.T) {
	connector := NewConnector()
	gateway := &fakeSDKGateway{}
	store := newFakeSDKAccountStore()
	err := connector.Start(context.Background(), sdk.Runtime{
		WorkspaceUUID: "workspace-1",
		Channel:       sdk.Channel{UUID: "channel-1", WorkspaceUUID: "workspace-1", Platform: Platform},
		Account:       sdkAccount("account-1", "client-1", "secret-1", ""),
		Gateway:       gateway,
		AccountStore:  store,
	})
	if err != nil {
		t.Fatalf("Start error=%v", err)
	}
	if gateway.channelPlatform != Platform {
		t.Fatalf("channel platform=%q", gateway.channelPlatform)
	}
	if gateway.channelLinkAccountUUID != "account-1" {
		t.Fatalf("channel link account=%q", gateway.channelLinkAccountUUID)
	}
	if state := store.state("account-1"); state["channel_link_session"] != "link-account-1" {
		t.Fatalf("state=%+v", state)
	}
}

func TestDingTalkConnectorEventCreatesMessageAndDedupes(t *testing.T) {
	connector := newTestEventConnector(t)
	gateway := &fakeSDKGateway{}
	store := newFakeSDKAccountStore()
	account := sdkAccount("account-1", "client-1", "secret-1", "")
	body := []byte(`{
		"conversationType":"2",
		"conversationId":"cid-group",
		"conversationTitle":"Team",
		"senderStaffId":"staff-1",
		"senderNick":"Alice",
		"msgId":"msg-1",
		"msgtype":"text",
		"isInAtList":true,
		"text":{"content":"hello group","at":{"atUserIds":["staff-2"],"atDingtalkIds":["ding-1"],"atMobiles":["13800000000"],"isAtAll":true}},
		"chatbotUserId":"bot-1",
		"chatbotCorpId":"corp-1",
		"robotCode":"robot-1",
		"sessionWebhook":"https://oapi.dingtalk.com/robot/sendBySession?session=s1",
		"sessionWebhookExpiredTime":4102444800000
	}`)
	runtime := sdk.Runtime{
		WorkspaceUUID: "workspace-1",
		Channel:       sdk.Channel{UUID: "channel-1", WorkspaceUUID: "workspace-1", Platform: Platform},
		Account:       account,
		Gateway:       gateway,
		AccountStore:  store,
	}
	result, err := connector.HandleEvent(context.Background(), runtime, account, body)
	if err != nil {
		t.Fatal(err)
	}
	if result.Ignored || result.SessionUUID != "session-1" || result.MessageUUID != "message-1" {
		t.Fatalf("result=%+v", result)
	}
	if result.Inbound == nil || result.Inbound.ChatType != sdk.ChatTypeGroup || result.Inbound.ChatID != "cid-group" || result.Inbound.AccountUUID != "account-1" {
		t.Fatalf("inbound=%+v", result.Inbound)
	}
	if !result.Inbound.MentionedMe || !result.Inbound.MentionAll || len(result.Inbound.Mentions) != 6 {
		t.Fatalf("inbound mentions=%+v mentioned_me=%v mention_all=%v", result.Inbound.Mentions, result.Inbound.MentionedMe, result.Inbound.MentionAll)
	}
	if result.Inbound.SenderDisplayName != "Alice" {
		t.Fatalf("sender_display_name=%q", result.Inbound.SenderDisplayName)
	}
	if result.Inbound.ChatDisplayName != "Team" || result.Inbound.ChatIdentity.DisplayName != "Team" {
		t.Fatalf("chat identity=%+v display_name=%q", result.Inbound.ChatIdentity, result.Inbound.ChatDisplayName)
	}
	gateway.mu.Lock()
	if len(gateway.chatSessions) != 1 {
		t.Fatalf("chatSessions=%+v", gateway.chatSessions)
	}
	chatReq := gateway.chatSessions[0]
	if chatReq.AccountUUID != "account-1" || chatReq.ChatType != sdk.ChatTypeGroup || chatReq.ChatID != "cid-group" || chatReq.SenderID != "staff-1" {
		t.Fatalf("chatReq=%+v", chatReq)
	}
	if chatReq.ChatDisplayName != "Team" || chatReq.ChatIdentity.DisplayName != "Team" || chatReq.ChatIdentity.ID != "cid-group" {
		t.Fatalf("chatReq identity=%+v display_name=%q", chatReq.ChatIdentity, chatReq.ChatDisplayName)
	}
	if len(gateway.messages) != 1 {
		t.Fatalf("messages=%+v", gateway.messages)
	}
	if gateway.messages[0].SenderID != "im:dingtalk:group:cid-group:user:staff-1" || gateway.messages[0].Content != "hello group" {
		t.Fatalf("message=%+v", gateway.messages[0])
	}
	if gateway.messages[0].DedupeKey != "account-1:message:msg-1" {
		t.Fatalf("dedupe key=%q", gateway.messages[0].DedupeKey)
	}
	if gateway.messages[0].Metadata["chat_display_name"] != "Team" || gateway.messages[0].Metadata["sender_display_name"] != "Alice" {
		t.Fatalf("message metadata=%+v", gateway.messages[0].Metadata)
	}
	if identity, ok := gateway.messages[0].Metadata["chat_identity"].(sdk.ChatIdentity); !ok || identity.ID != "cid-group" || identity.DisplayName != "Team" {
		t.Fatalf("message chat_identity=%+v metadata=%+v", identity, gateway.messages[0].Metadata)
	}
	gateway.mu.Unlock()

	state := store.state("account-1")
	if state["chatbot_user_id"] != "bot-1" || state["chatbot_corp_id"] != "corp-1" {
		t.Fatalf("stored state=%+v", state)
	}
	identities, ok := state["bot_identities"].([]map[string]any)
	if !ok || len(identities) != 2 || identities[0]["id"] != "bot-1" || identities[0]["id_type"] != "chatbot_user_id" || identities[1]["id"] != "robot-1" || identities[1]["id_type"] != "robot_code" {
		t.Fatalf("bot_identities=%+v state=%+v", state["bot_identities"], state)
	}
	if state["session_webhooks"] == nil {
		t.Fatalf("missing session_webhooks state=%+v", state)
	}
	account.State = state
	runtime.Account = account
	duplicate, err := connector.HandleEvent(context.Background(), runtime, account, body)
	if err != nil {
		t.Fatal(err)
	}
	if !duplicate.Ignored || duplicate.Reason != "duplicate" {
		t.Fatalf("duplicate=%+v", duplicate)
	}
	gateway.mu.Lock()
	defer gateway.mu.Unlock()
	if len(gateway.messages) != 1 {
		t.Fatalf("duplicate created message=%+v", gateway.messages)
	}
}

func TestDingTalkConnectorEventIgnoresUntrustedSessionWebhook(t *testing.T) {
	connector := newTestEventConnector(t)
	gateway := &fakeSDKGateway{}
	store := newFakeSDKAccountStore()
	account := sdkAccount("account-1", "client-1", "secret-1", "")
	_, err := connector.HandleEvent(context.Background(), sdk.Runtime{
		WorkspaceUUID: "workspace-1",
		Channel:       sdk.Channel{UUID: "channel-1", WorkspaceUUID: "workspace-1", Platform: Platform},
		Account:       account,
		Gateway:       gateway,
		AccountStore:  store,
	}, account, []byte(`{
		"conversationType":"2",
		"conversationId":"cid-group",
		"senderStaffId":"staff-1",
		"msgId":"msg-untrusted-webhook",
		"msgtype":"text",
		"text":{"content":"hello group"},
		"sessionWebhook":"https://example.test/robot/sendBySession?session=s1",
		"sessionWebhookExpiredTime":4102444800000
	}`))
	if err != nil {
		t.Fatal(err)
	}
	state := store.state("account-1")
	webhooks, _ := state["session_webhooks"].(map[string]beakstate.Webhook)
	if len(webhooks) != 0 {
		t.Fatalf("untrusted webhook should not be stored: %+v", webhooks)
	}
}

func TestDingTalkConnectorMentionAllDoesNotMentionBot(t *testing.T) {
	connector := newTestEventConnector(t)
	gateway := &fakeSDKGateway{}
	account := sdkAccount("account-1", "client-1", "secret-1", "")
	result, err := connector.HandleEvent(context.Background(), sdk.Runtime{
		WorkspaceUUID: "workspace-1",
		Channel:       sdk.Channel{UUID: "channel-1", WorkspaceUUID: "workspace-1", Platform: Platform},
		Account:       account,
		Gateway:       gateway,
		AccountStore:  newFakeSDKAccountStore(),
	}, account, []byte(`{
		"conversationType":"2",
		"conversationId":"cid-group",
		"senderStaffId":"staff-1",
		"senderNick":"Alice",
		"msgId":"msg-at-all",
		"msgtype":"text",
		"isInAtList":false,
		"text":{"content":"hello all","at":{"isAtAll":true}},
		"robotCode":"robot-1"
	}`))
	if err != nil {
		t.Fatal(err)
	}
	if result.Inbound == nil || !result.Inbound.MentionAll || result.Inbound.MentionedMe {
		t.Fatalf("inbound=%+v", result.Inbound)
	}
}

func TestDingTalkConnectorOnlyBotMentionWithEmptyTextIsDelivered(t *testing.T) {
	connector := newTestEventConnector(t)
	gateway := &fakeSDKGateway{}
	account := sdkAccount("account-1", "client-1", "secret-1", "")
	result, err := connector.HandleEvent(context.Background(), sdk.Runtime{
		WorkspaceUUID: "workspace-1",
		Channel:       sdk.Channel{UUID: "channel-1", WorkspaceUUID: "workspace-1", Platform: Platform},
		Account:       account,
		Gateway:       gateway,
		AccountStore:  newFakeSDKAccountStore(),
	}, account, []byte(`{
		"conversationType":"2",
		"conversationId":"cid-group",
		"senderStaffId":"staff-1",
		"senderNick":"Alice",
		"msgId":"msg-only-bot",
		"msgtype":"text",
		"isInAtList":true,
		"text":{"content":"","at":{}},
		"chatbotUserId":"bot-1",
		"robotCode":"robot-1"
	}`))
	if err != nil {
		t.Fatal(err)
	}
	if result.Ignored || result.Inbound == nil || !result.Inbound.MentionedMe || strings.TrimSpace(result.Inbound.Text) != "" {
		t.Fatalf("result=%+v", result)
	}
	gateway.mu.Lock()
	defer gateway.mu.Unlock()
	if len(gateway.messages) != 1 || strings.TrimSpace(gateway.messages[0].Content) != "" {
		t.Fatalf("messages=%+v", gateway.messages)
	}
}

func TestDingTalkConnectorSelfEchoUsesStoredStandardBotIdentity(t *testing.T) {
	connector := newTestEventConnector(t)
	gateway := &fakeSDKGateway{}
	store := newFakeSDKAccountStore()
	account := sdkAccount("account-1", "client-1", "secret-1", "")
	delete(account.State, "chatbot_user_id")
	if err := store.SaveChannelAccountState(context.Background(), "account-1", map[string]any{
		"bot_identity": map[string]any{
			"id":      "bot-stored",
			"id_type": "chatbot_user_id",
		},
	}); err != nil {
		t.Fatal(err)
	}
	result, err := connector.HandleEvent(context.Background(), sdk.Runtime{
		WorkspaceUUID: "workspace-1",
		Channel:       sdk.Channel{UUID: "channel-1", WorkspaceUUID: "workspace-1", Platform: Platform},
		Account:       account,
		Gateway:       gateway,
		AccountStore:  store,
	}, account, []byte(`{
		"conversationType":"2",
		"conversationId":"cid-group",
		"senderId":"bot-stored",
		"senderNick":"Beak Bot",
		"msgId":"msg-self-stored",
		"msgtype":"text",
		"isInAtList":true,
		"text":{"content":"self echo","at":{}},
		"robotCode":"robot-1"
	}`))
	if err != nil {
		t.Fatal(err)
	}
	if result == nil || !result.Ignored || result.Reason != "self_echo" {
		t.Fatalf("result=%+v", result)
	}
	gateway.mu.Lock()
	defer gateway.mu.Unlock()
	if len(gateway.messages) != 0 {
		t.Fatalf("messages=%+v", gateway.messages)
	}
}

func TestDingTalkConnectorWebhookRequestVerifiesSignature(t *testing.T) {
	connector := newTestWebhookRequestConnector(t)
	gateway := &fakeSDKGateway{}
	account := sdkAccount("account-1", "client-1", "secret-1", "")
	body := []byte(`{
		"conversationType":"2",
		"conversationId":"cid-group",
		"senderStaffId":"staff-1",
		"msgId":"msg-http-1",
		"msgtype":"text",
		"text":{"content":"hello http"}
	}`)
	req, err := http.NewRequest(http.MethodPost, "https://beak.test/dingtalk", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	timestamp := fmt.Sprintf("%d", time.Now().UTC().UnixMilli())
	req.Header.Set("timestamp", timestamp)
	req.Header.Set("sign", dingtalkWebhookSignature(timestamp, "secret-1"))

	response, err := connector.HandleWebhookRequest(context.Background(), sdk.Runtime{
		WorkspaceUUID: "workspace-1",
		Channel:       sdk.Channel{UUID: "channel-1", WorkspaceUUID: "workspace-1", Platform: Platform},
		Account:       account,
		Gateway:       gateway,
		AccountStore:  newFakeSDKAccountStore(),
	}, account, req)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK || len(response.Body) != 0 {
		t.Fatalf("response=%+v body=%q", response, string(response.Body))
	}
	if len(gateway.messages) != 1 || gateway.messages[0].Content != "hello http" {
		t.Fatalf("messages=%+v", gateway.messages)
	}
}

func TestDingTalkConnectorWebhookRequestRejectsBadSignature(t *testing.T) {
	connector := newTestWebhookRequestConnector(t)
	account := sdkAccount("account-1", "client-1", "secret-1", "")
	req, err := http.NewRequest(http.MethodPost, "https://beak.test/dingtalk", bytes.NewReader([]byte(`{"text":{"content":"hello"}}`)))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("timestamp", fmt.Sprintf("%d", time.Now().UTC().UnixMilli()))
	req.Header.Set("sign", "bad-signature")

	if _, err := connector.HandleWebhookRequest(context.Background(), sdk.Runtime{}, account, req); err == nil || !strings.Contains(err.Error(), "signature mismatch") {
		t.Fatalf("HandleWebhookRequest() error=%v, want signature mismatch", err)
	}
}

func TestDingTalkConnectorSendUsesSessionWebhook(t *testing.T) {
	var sawWebhook bool
	httpClient := &http.Client{Transport: testRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		sawWebhook = true
		if r.URL.String() != "https://oapi.dingtalk.com/robot/sendBySession?session=s1" {
			t.Fatalf("unexpected request: %s", r.URL.String())
		}
		var body struct {
			MsgType string `json:"msgtype"`
			Text    struct {
				Content string `json:"content"`
			} `json:"text"`
			At struct {
				AtUserIDs     []string `json:"atUserIds"`
				AtDingtalkIDs []string `json:"atDingtalkIds"`
				IsAtAll       bool     `json:"isAtAll"`
			} `json:"at"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if body.MsgType != "text" || body.Text.Content != "reply @ding-1 @staff-1 @all" {
			t.Fatalf("body=%+v", body)
		}
		if len(body.At.AtUserIDs) != 1 || body.At.AtUserIDs[0] != "staff-1" || len(body.At.AtDingtalkIDs) != 1 || body.At.AtDingtalkIDs[0] != "ding-1" || !body.At.IsAtAll {
			t.Fatalf("at=%+v", body.At)
		}
		return testJSONResponse(map[string]any{"errcode": 0, "errmsg": "ok"})
	})}
	account := sdkAccount("account-1", "client-1", "secret-1", "https://api.dingtalk.test")
	account.State = map[string]any{
		"session_webhooks": map[string]any{
			"group:cid-group": map[string]any{
				"url":        "https://oapi.dingtalk.com/robot/sendBySession?session=s1",
				"expires_at": time.Now().UTC().Add(time.Hour).Format(time.RFC3339Nano),
			},
		},
	}
	result, err := NewConnector().Send(context.Background(), sdk.Runtime{
		HTTPClient: httpClient,
		Account:    account,
	}, sdk.OutboundMessage{
		AccountUUID: "account-1",
		ChatType:    sdk.ChatTypeGroup,
		ChatID:      "cid-group",
		Text:        "reply",
		MentionAll:  true,
		Mentions: []sdk.MentionIdentity{
			{ID: "staff-1", IDType: "staff_id"},
			{ID: "ding-1", IDType: "dingtalk_id"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !sawWebhook || result.Raw["delivery_method"] != "session_webhook" {
		t.Fatalf("sawWebhook=%v result=%+v", sawWebhook, result)
	}
}

func TestDingTalkConnectorSendMarkdownUsesSessionWebhook(t *testing.T) {
	httpClient := &http.Client{Transport: testRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.String() != "https://oapi.dingtalk.com/robot/sendBySession?session=s1" {
			t.Fatalf("unexpected request: %s", r.URL.String())
		}
		var body struct {
			MsgType  string `json:"msgtype"`
			Markdown struct {
				Title string `json:"title"`
				Text  string `json:"text"`
			} `json:"markdown"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if body.MsgType != "markdown" || body.Markdown.Title != "日志查询" || body.Markdown.Text != "# 日志查询\n- 错误日志" {
			t.Fatalf("body=%+v", body)
		}
		return testJSONResponse(map[string]any{"errcode": 0, "errmsg": "ok"})
	})}
	account := sdkAccount("account-1", "client-1", "secret-1", "https://api.dingtalk.test")
	account.State = map[string]any{
		"session_webhooks": map[string]any{
			"group:cid-group": map[string]any{
				"url":        "https://oapi.dingtalk.com/robot/sendBySession?session=s1",
				"expires_at": time.Now().UTC().Add(time.Hour).Format(time.RFC3339Nano),
			},
		},
	}
	result, err := NewConnector().Send(context.Background(), sdk.Runtime{
		HTTPClient: httpClient,
		Account:    account,
	}, sdk.OutboundMessage{
		AccountUUID: "account-1",
		ChatType:    sdk.ChatTypeGroup,
		ChatID:      "cid-group",
		Text:        "# 日志查询\n- 错误日志",
		Format:      "markdown",
		Title:       "日志查询",
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Raw["delivery_method"] != "session_webhook" || result.Raw["msg_type"] != "markdown" {
		t.Fatalf("result=%+v", result)
	}
}

func TestDingTalkConnectorSendMarkdownSessionWebhookDerivesTitleFromContent(t *testing.T) {
	httpClient := &http.Client{Transport: testRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.String() != "https://oapi.dingtalk.com/robot/sendBySession?session=s1" {
			t.Fatalf("unexpected request: %s", r.URL.String())
		}
		var body struct {
			MsgType  string `json:"msgtype"`
			Markdown struct {
				Title string `json:"title"`
				Text  string `json:"text"`
			} `json:"markdown"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if body.MsgType != "markdown" || body.Markdown.Title != "第一行标题" || body.Markdown.Text != "# 第一行标题\n第二行内容" {
			t.Fatalf("body=%+v", body)
		}
		return testJSONResponse(map[string]any{"errcode": 0, "errmsg": "ok"})
	})}
	account := sdkAccount("account-1", "client-1", "secret-1", "https://api.dingtalk.test")
	account.State = map[string]any{
		"session_webhooks": map[string]any{
			"group:cid-group": map[string]any{
				"url":        "https://oapi.dingtalk.com/robot/sendBySession?session=s1",
				"expires_at": time.Now().UTC().Add(time.Hour).Format(time.RFC3339Nano),
			},
		},
	}
	result, err := NewConnector().Send(context.Background(), sdk.Runtime{
		HTTPClient: httpClient,
		Account:    account,
	}, sdk.OutboundMessage{
		AccountUUID: "account-1",
		ChatType:    sdk.ChatTypeGroup,
		ChatID:      "cid-group",
		Text:        "# 第一行标题\n第二行内容",
		Format:      "markdown",
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Raw["delivery_method"] != "session_webhook" || result.Raw["msg_type"] != "markdown" {
		t.Fatalf("result=%+v", result)
	}
}

func TestDingTalkConnectorSendMarkdownOpenAPI(t *testing.T) {
	httpClient := &http.Client{Transport: testRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/v1.0/oauth2/accessToken":
			return testJSONResponse(map[string]any{"accessToken": "token-1", "expireIn": 7200})
		case "/v1.0/robot/groupMessages/send":
			var body struct {
				MsgKey   string `json:"msgKey"`
				MsgParam string `json:"msgParam"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			if body.MsgKey != "sampleMarkdown" {
				t.Fatalf("body=%+v", body)
			}
			var param map[string]string
			if err := json.Unmarshal([]byte(body.MsgParam), &param); err != nil {
				t.Fatal(err)
			}
			if param["title"] != "日志查询" || param["text"] != "# 日志查询\n- 错误日志" {
				t.Fatalf("param=%+v", param)
			}
			return testJSONResponse(map[string]any{"processQueryKey": "pqk-markdown"})
		default:
			t.Fatalf("unexpected request: %s", r.URL.Path)
		}
		return nil, nil
	})}
	result, err := NewConnector().Send(context.Background(), sdk.Runtime{
		HTTPClient: httpClient,
		Account:    sdkAccount("account-1", "client-1", "secret-1", "https://api.dingtalk.test"),
	}, sdk.OutboundMessage{
		AccountUUID: "account-1",
		ChatType:    sdk.ChatTypeGroup,
		ChatID:      "cid-group",
		Text:        "# 日志查询\n- 错误日志",
		Raw:         map[string]any{"msg_type": "markdown", "title": "日志查询"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.MessageID != "pqk-markdown" || result.Raw["delivery_method"] != "openapi" || result.Raw["msg_type"] != "markdown" {
		t.Fatalf("result=%+v", result)
	}
}

func TestDingTalkConnectorSendMarkdownDerivesTitleFromContent(t *testing.T) {
	httpClient := &http.Client{Transport: testRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/v1.0/oauth2/accessToken":
			return testJSONResponse(map[string]any{"accessToken": "token-1", "expireIn": 7200})
		case "/v1.0/robot/groupMessages/send":
			var body struct {
				MsgKey   string `json:"msgKey"`
				MsgParam string `json:"msgParam"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			if body.MsgKey != "sampleMarkdown" {
				t.Fatalf("body=%+v", body)
			}
			var param map[string]string
			if err := json.Unmarshal([]byte(body.MsgParam), &param); err != nil {
				t.Fatal(err)
			}
			if param["title"] != "这个明显就是用 正文内容截断之后作为标题" || param["text"] != "# 这个明显就是用 正文内容截断之后作为标题\n- 错误日志" {
				t.Fatalf("param=%+v", param)
			}
			return testJSONResponse(map[string]any{"processQueryKey": "pqk-markdown-default-title"})
		default:
			t.Fatalf("unexpected request: %s", r.URL.Path)
		}
		return nil, nil
	})}
	result, err := NewConnector().Send(context.Background(), sdk.Runtime{
		HTTPClient: httpClient,
		Account:    sdkAccount("account-1", "client-1", "secret-1", "https://api.dingtalk.test"),
	}, sdk.OutboundMessage{
		AccountUUID: "account-1",
		ChatType:    sdk.ChatTypeGroup,
		ChatID:      "cid-group",
		Text:        "# 这个明显就是用 正文内容截断之后作为标题\n- 错误日志",
		Format:      "markdown",
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.MessageID != "pqk-markdown-default-title" || result.Raw["delivery_method"] != "openapi" || result.Raw["msg_type"] != "markdown" {
		t.Fatalf("result=%+v", result)
	}
}

func TestDingTalkOutboundMarkdownFormatAliases(t *testing.T) {
	req := sdk.OutboundMessage{Raw: map[string]any{"content_type": "text/markdown"}}
	if got := dingtalkOutboundFormat(req); got != "markdown" {
		t.Fatalf("dingtalkOutboundFormat() = %q, want markdown", got)
	}
}

func TestDingTalkConnectorSendPersistsTokenForEmptyState(t *testing.T) {
	var tokenCalls int
	httpClient := &http.Client{Transport: testRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/v1.0/oauth2/accessToken":
			tokenCalls++
			return testJSONResponse(map[string]any{"accessToken": "token-empty-state", "expireIn": 7200})
		case "/v1.0/robot/groupMessages/send":
			if got := r.Header.Get("x-acs-dingtalk-access-token"); got != "token-empty-state" {
				t.Fatalf("access token=%q", got)
			}
			return testJSONResponse(map[string]any{"processQueryKey": "pqk-empty-state"})
		default:
			t.Fatalf("unexpected request: %s", r.URL.Path)
		}
		return nil, nil
	})}
	store := newFakeSDKAccountStore()
	result, err := NewConnector().Send(context.Background(), sdk.Runtime{
		HTTPClient:   httpClient,
		Account:      sdkAccount("account-1", "client-1", "secret-1", "https://api.dingtalk.test"),
		AccountStore: store,
	}, sdk.OutboundMessage{
		AccountUUID: "account-1",
		ChatType:    sdk.ChatTypeGroup,
		ChatID:      "cid-group",
		Text:        "reply",
	})
	if err != nil {
		t.Fatal(err)
	}
	if tokenCalls != 1 || result.MessageID != "pqk-empty-state" {
		t.Fatalf("tokenCalls=%d result=%+v", tokenCalls, result)
	}
	state := store.state("account-1")
	if state["access_token"] != "token-empty-state" || state["access_token_expires_at"] == nil {
		t.Fatalf("saved state=%+v", state)
	}
}

func TestDingTalkConnectorRejectsMismatchedRobotCode(t *testing.T) {
	connector := newTestEventConnector(t)
	gateway := &fakeSDKGateway{}
	account := sdkAccount("account-1", "client-1", "secret-1", "")
	result, err := connector.HandleEvent(context.Background(), sdk.Runtime{
		WorkspaceUUID: "workspace-1",
		Channel:       sdk.Channel{UUID: "channel-1", WorkspaceUUID: "workspace-1", Platform: Platform},
		Account:       account,
		Gateway:       gateway,
		AccountStore:  newFakeSDKAccountStore(),
	}, account, []byte(`{
		"conversationType":"2",
		"conversationId":"cid-group",
		"senderStaffId":"staff-1",
		"msgId":"msg-1",
		"msgtype":"text",
		"robotCode":"other-robot",
		"text":{"content":"wrong robot"}
	}`))
	if err != nil {
		t.Fatal(err)
	}
	if result == nil || !result.Ignored || result.Reason != "robot_code_mismatch" {
		t.Fatalf("result=%+v", result)
	}
	if len(gateway.messages) != 0 {
		t.Fatalf("messages=%+v", gateway.messages)
	}
}

func TestDingTalkConnectorEventDirectChat(t *testing.T) {
	connector := newTestEventConnector(t)
	gateway := &fakeSDKGateway{}
	account := sdkAccount("account-1", "client-1", "secret-1", "")
	result, err := connector.HandleEvent(context.Background(), sdk.Runtime{
		WorkspaceUUID: "workspace-1",
		Channel:       sdk.Channel{UUID: "channel-1", WorkspaceUUID: "workspace-1", Platform: Platform},
		Account:       account,
		Gateway:       gateway,
		AccountStore:  newFakeSDKAccountStore(),
	}, account, []byte(`{
		"conversationType":"1",
		"conversationId":"cid-direct",
		"senderStaffId":"staff-1",
		"msgId":"msg-direct",
		"msgtype":"text",
		"text":{"content":"hello direct"}
	}`))
	if err != nil {
		t.Fatal(err)
	}
	if result.Inbound == nil || result.Inbound.ChatType != sdk.ChatTypeDirect || result.Inbound.ChatID != "staff-1" {
		t.Fatalf("inbound=%+v", result.Inbound)
	}
}

func TestDingTalkConnectorSendUsesRequestedAccount(t *testing.T) {
	httpClient := &http.Client{Transport: testRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/v1.0/oauth2/accessToken":
			var body map[string]string
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			if body["appKey"] != "client-2" || body["appSecret"] != "secret-2" {
				t.Fatalf("token body=%+v", body)
			}
			return testJSONResponse(map[string]any{"accessToken": "token-2", "expireIn": 7200})
		case "/v1.0/robot/groupMessages/send":
			if got := r.Header.Get("x-acs-dingtalk-access-token"); got != "token-2" {
				t.Fatalf("token=%q", got)
			}
			var body struct {
				RobotCode          string `json:"robotCode"`
				OpenConversationID string `json:"openConversationId"`
				MsgKey             string `json:"msgKey"`
				MsgParam           string `json:"msgParam"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			if body.RobotCode != "robot-2" || body.OpenConversationID != "cid-group" || body.MsgKey != "sampleText" {
				t.Fatalf("send body=%+v", body)
			}
			var param map[string]string
			if err := json.Unmarshal([]byte(body.MsgParam), &param); err != nil {
				t.Fatal(err)
			}
			if param["content"] != "reply @ding-2 @staff-2 @all" {
				t.Fatalf("msgParam=%+v", param)
			}
			return testJSONResponse(map[string]any{"processQueryKey": "pqk-2"})
		default:
			t.Fatalf("unexpected request: %s", r.URL.Path)
		}
		return nil, nil
	})}

	connector := NewConnector()
	result, err := connector.Send(context.Background(), sdk.Runtime{
		HTTPClient: httpClient,
		Accounts: []sdk.ChannelAccount{
			sdkAccount("account-1", "client-1", "secret-1", "https://api.dingtalk.test"),
			sdkAccount("account-2", "client-2", "secret-2", "https://api.dingtalk.test"),
		},
	}, sdk.OutboundMessage{
		AccountUUID: "account-2",
		ChatType:    sdk.ChatTypeGroup,
		ChatID:      "cid-group",
		Text:        "reply",
		MessageUUID: "message-uuid",
		MentionAll:  true,
		Mentions: []sdk.MentionIdentity{
			{ID: "ding-2", IDType: "dingtalk_id"},
			{ID: "staff-2", IDType: "staff_id"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.AccountUUID != "account-2" || result.Platform != Platform || result.MessageID != "pqk-2" {
		t.Fatalf("result=%+v", result)
	}
}

func sdkAccount(uuid, clientID, clientSecret, baseURL string) sdk.ChannelAccount {
	credential := map[string]any{
		"account_id":      uuid,
		"client_id":       clientID,
		"client_secret":   clientSecret,
		"robot_code":      strings.Replace(clientID, "client", "robot", 1),
		"chatbot_user_id": "bot-self",
		"chatbot_corp_id": "corp-1",
	}
	if baseURL != "" {
		credential["base_url"] = baseURL
	}
	return sdk.ChannelAccount{
		UUID:          uuid,
		WorkspaceUUID: "workspace-1",
		ChannelUUID:   "channel-1",
		Platform:      Platform,
		Credential:    credential,
		State:         map[string]any{},
	}
}

type fakeSDKGateway struct {
	mu                     sync.Mutex
	channelPlatform        string
	channelLinkAccountUUID string
	chatSessions           []sdk.EnsureChatSessionRequest
	messages               []sdk.CreateMessageRequest
}

func (g *fakeSDKGateway) EnsureChannel(ctx context.Context, req sdk.EnsureChannelRequest) (string, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.channelPlatform = req.Platform
	return "channel-1", nil
}

func (g *fakeSDKGateway) EnsureChannelLinkSession(ctx context.Context, req sdk.EnsureChannelLinkSessionRequest) (string, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.channelLinkAccountUUID = req.AccountUUID
	return "link-" + req.AccountUUID, nil
}

func (g *fakeSDKGateway) EnsureChatSession(ctx context.Context, req sdk.EnsureChatSessionRequest) (string, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.chatSessions = append(g.chatSessions, req)
	return "session-1", nil
}

func (g *fakeSDKGateway) CreateMessage(ctx context.Context, req sdk.CreateMessageRequest) (string, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.messages = append(g.messages, req)
	return "message-1", nil
}

func (g *fakeSDKGateway) StreamSession(ctx context.Context, req sdk.StreamSessionRequest, handle func(sdk.StreamEvent) error) error {
	return nil
}

func (g *fakeSDKGateway) AgentParticipantID() string {
	return "agent:agent-1"
}

func (g *fakeSDKGateway) BridgeParticipantID(platform string) string {
	return sdk.BridgeParticipantID(platform)
}

type fakeSDKAccountStore struct {
	mu     sync.Mutex
	states map[string]map[string]any
}

func newFakeSDKAccountStore() *fakeSDKAccountStore {
	return &fakeSDKAccountStore{states: make(map[string]map[string]any)}
}

func (s *fakeSDKAccountStore) SaveChannelAccountState(ctx context.Context, accountUUID string, state map[string]any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.states[accountUUID] = state
	return nil
}

func (s *fakeSDKAccountStore) LoadChannelAccountState(ctx context.Context, accountUUID string) (map[string]any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.states[accountUUID], nil
}

func (s *fakeSDKAccountStore) state(accountUUID string) map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.states[accountUUID]
}

type testRoundTripFunc func(*http.Request) (*http.Response, error)

func (f testRoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func testJSONResponse(body any) (*http.Response, error) {
	var builder strings.Builder
	if err := json.NewEncoder(&builder).Encode(body); err != nil {
		return nil, err
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(builder.String())),
	}, nil
}
