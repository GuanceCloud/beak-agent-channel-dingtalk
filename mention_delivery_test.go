package beakdingtalk

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/GuanceCloud/beak-agent-channel-dingtalk/sdk"
)

// Exercise the real connector and HTTP encoder. A typed MentionIdentity alone
// does not prove that DingTalk receives a native notification.
func TestNativeMentionDelivery(t *testing.T) {
	for _, tc := range []struct {
		name, format, chatType, webhook, status, reason string
		mention, forceAPI, failNotice, failBody         bool
		attachments, partialAttachments, attachmentOnly bool
		wantTypes                                       []string
	}{
		{name: "markdown", format: "markdown", chatType: "group", webhook: "valid", mention: true, status: "sent", wantTypes: []string{"markdown", "text"}},
		{name: "text", format: "text", chatType: "group", webhook: "valid", mention: true, status: "sent", wantTypes: []string{"text"}},
		{name: "direct", format: "markdown", chatType: "direct", webhook: "valid", mention: true, wantTypes: []string{"markdown"}},
		{name: "no target", format: "markdown", chatType: "group", webhook: "valid", wantTypes: []string{"markdown"}},
		{name: "notice rejected", format: "markdown", chatType: "group", webhook: "valid", mention: true, failNotice: true, status: "failed", reason: "notification_send_failed", wantTypes: []string{"markdown", "text"}},
		{name: "body rejected", format: "markdown", chatType: "group", webhook: "valid", mention: true, failBody: true, wantTypes: []string{"markdown"}},
		{name: "expired markdown", format: "markdown", chatType: "group", webhook: "expired", mention: true, status: "unsupported", reason: "session_webhook_unavailable", wantTypes: []string{"sampleMarkdown"}},
		{name: "missing text", format: "text", chatType: "group", mention: true, status: "unsupported", reason: "session_webhook_unavailable", wantTypes: []string{"sampleText"}},
		{name: "untrusted webhook", format: "markdown", chatType: "group", webhook: "untrusted", mention: true, status: "unsupported", reason: "session_webhook_unavailable", wantTypes: []string{"sampleMarkdown"}},
		{name: "forced OpenAPI", format: "markdown", chatType: "group", webhook: "valid", mention: true, forceAPI: true, status: "unsupported", reason: "session_webhook_disabled", wantTypes: []string{"sampleMarkdown"}},
		{name: "two attachments", format: "markdown", chatType: "group", webhook: "valid", mention: true, attachments: true, status: "sent", wantTypes: []string{"sampleMarkdown", "sampleFile", "sampleFile", "text"}},
		{name: "partial attachments", format: "markdown", chatType: "group", webhook: "valid", mention: true, attachments: true, partialAttachments: true, status: "sent", wantTypes: []string{"sampleMarkdown", "sampleFile", "text"}},
		{name: "attachment only", format: "markdown", chatType: "group", webhook: "valid", mention: true, attachments: true, attachmentOnly: true, status: "sent", wantTypes: []string{"sampleFile", "sampleFile", "text"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			account := sdkAccount("account-1", "client-1", "secret-1", "https://api.dingtalk.test")
			account.State = map[string]any{"access_token": "test-token", "access_token_expires_at": time.Now().Add(time.Hour).Format(time.RFC3339Nano)}
			if tc.webhook != "" {
				expires := time.Now().Add(time.Hour)
				if tc.webhook == "expired" {
					expires = time.Now().Add(-time.Hour)
				}
				url := "https://oapi.dingtalk.com/robot/sendBySession?session=test"
				if tc.webhook == "untrusted" {
					url = "https://untrusted.test/robot/sendBySession?session=test"
				}
				key := "chat"
				if tc.chatType == "group" {
					key = "group:chat"
				}
				account.State["session_webhooks"] = map[string]any{key: map[string]any{"url": url, "expires_at": expires.Format(time.RFC3339Nano)}}
			}
			var types []string
			var notifications int
			client := &http.Client{Transport: testRoundTripFunc(func(r *http.Request) (*http.Response, error) {
				switch r.URL.Path {
				case "/gettoken":
					return testJSONResponse(map[string]any{"errcode": 0, "access_token": "test-media-token"})
				case "/media/upload":
					return testJSONResponse(map[string]any{"errcode": 0, "media_id": "test-media"})
				case "/robot/sendBySession", "/v1.0/robot/groupMessages/send", "/v1.0/robot/oToMessages/batchSend":
				default:
					t.Fatalf("unexpected endpoint: %s", r.URL.Path)
				}
				var body map[string]any
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Fatal(err)
				}
				kind, _ := body["msgtype"].(string)
				if kind == "" {
					kind, _ = body["msgKey"].(string)
				}
				types = append(types, kind)
				// The native identity belongs in at, never in an appended @ID.
				for _, key := range []string{"text", "markdown", "msgParam"} {
					encoded, _ := json.Marshal(body[key])
					if strings.Contains(string(encoded), "@41") {
						t.Errorf("fake mention in %s: %s", key, encoded)
					}
				}
				if at, ok := body["at"].(map[string]any); ok {
					notifications++
					if kind != "text" || tc.chatType != "group" {
						t.Errorf("native mention on %s/%s", tc.chatType, kind)
					}
					if !reflect.DeepEqual(at["atUserIds"], []any{"41"}) {
						t.Errorf("wrong target: %v", at)
					}
				}
				if tc.failBody || (tc.failNotice && len(types) == 2) {
					return testJSONResponse(map[string]any{"errcode": 400, "errmsg": "rejected"})
				}
				return testJSONResponse(map[string]any{"errcode": 0, "processQueryKey": "receipt"})
			})}
			req := sdk.OutboundMessage{AccountUUID: "account-1", ChatType: tc.chatType, ChatID: "chat", Text: "# Answer", Format: tc.format, Raw: map[string]any{"force_openapi": tc.forceAPI}}
			if tc.mention {
				req.Mentions = []sdk.MentionIdentity{{ID: "41", IDType: "user_id"}}
			}
			if tc.attachments {
				path := filepath.Join(t.TempDir(), "report.md")
				if err := os.WriteFile(path, []byte("report"), 0600); err != nil {
					t.Fatal(err)
				}
				req.Attachments = []sdk.MediaAttachment{
					{ID: "one", Kind: "file", Path: path, FileName: "one.md"},
					{ID: "two", Kind: "file", Path: path, FileName: "two.md"},
				}
				if tc.partialAttachments {
					req.Attachments[1].Path = path + ".missing"
				}
				if tc.attachmentOnly {
					req.Text = ""
				}
			}
			result, err := NewConnector().Send(context.Background(), sdk.Runtime{Account: account, HTTPClient: client}, req)
			if (err != nil) != tc.failBody {
				t.Fatalf("Send error=%v; body failure=%v", err, tc.failBody)
			}
			if !reflect.DeepEqual(types, tc.wantTypes) {
				t.Errorf("requests=%v, want %v", types, tc.wantTypes)
			}
			wantNotifications := 0
			if tc.status == "sent" || tc.status == "failed" {
				wantNotifications = 1
			}
			if notifications != wantNotifications {
				t.Errorf("notifications=%d, want %d", notifications, wantNotifications)
			}
			if tc.failBody {
				return
			}
			if tc.attachments {
				if len(result.AttachmentResults) != 2 || result.AttachmentResults[0].Status != sdk.AttachmentStatusSent {
					t.Fatalf("attachments=%+v", result.AttachmentResults)
				}
				want := sdk.AttachmentStatusSent
				if tc.partialAttachments {
					want = sdk.AttachmentStatusFailed
				}
				if result.AttachmentResults[1].Status != want {
					t.Fatalf("second attachment=%+v", result.AttachmentResults[1])
				}
			}
			encoded, _ := json.Marshal(result)
			var receipt struct {
				Raw struct {
					MentionDelivery *struct{ Status, Reason string } `json:"mention_delivery"`
				} `json:"raw"`
			}
			if err := json.Unmarshal(encoded, &receipt); err != nil {
				t.Fatal(err)
			}
			if tc.status == "" {
				if receipt.Raw.MentionDelivery != nil {
					t.Errorf("unexpected mention result: %s", encoded)
				}
			} else if receipt.Raw.MentionDelivery == nil || receipt.Raw.MentionDelivery.Status != tc.status || receipt.Raw.MentionDelivery.Reason != tc.reason {
				t.Errorf("mention result=%s, want %s/%s", encoded, tc.status, tc.reason)
			}
		})
	}
}
