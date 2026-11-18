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

// Exercise the real connector and HTTP encoder: outbound mention inputs must
// not add at fields, body markers, or separate notification requests.
func TestReplyDeliveryWithoutMentions(t *testing.T) {
	for _, tc := range []struct {
		name, format, chatType, webhook                 string
		mention, forceAPI, failNotice, failBody         bool
		attachments, partialAttachments, attachmentOnly bool
		allAttachmentsFail, failToken                   bool
		wantTypes                                       []string
	}{
		{name: "markdown", format: "markdown", chatType: "group", webhook: "valid", mention: true, wantTypes: []string{"markdown"}},
		{name: "markdown notice rejected", format: "markdown", chatType: "group", webhook: "valid", mention: true, failNotice: true, wantTypes: []string{"markdown"}},
		{name: "markdown notice rejected with attachments", format: "markdown", chatType: "group", webhook: "valid", mention: true, attachments: true, failNotice: true, wantTypes: []string{"markdown", "sampleFile", "sampleFile"}},
		{name: "text", format: "text", chatType: "group", webhook: "valid", mention: true, wantTypes: []string{"text"}},
		{name: "direct", format: "markdown", chatType: "direct", webhook: "valid", mention: true, wantTypes: []string{"markdown"}},
		{name: "no target", format: "markdown", chatType: "group", webhook: "valid", wantTypes: []string{"markdown"}},
		{name: "attachment only notice rejected", format: "markdown", chatType: "group", webhook: "valid", mention: true, attachments: true, attachmentOnly: true, failNotice: true, wantTypes: []string{"sampleFile", "sampleFile"}},
		{name: "body rejected", format: "markdown", chatType: "group", webhook: "valid", mention: true, failBody: true, wantTypes: []string{"markdown"}},
		{name: "expired markdown", format: "markdown", chatType: "group", webhook: "expired", mention: true, wantTypes: []string{"sampleMarkdown"}},
		{name: "missing text", format: "text", chatType: "group", mention: true, wantTypes: []string{"sampleText"}},
		{name: "untrusted webhook", format: "markdown", chatType: "group", webhook: "untrusted", mention: true, wantTypes: []string{"sampleMarkdown"}},
		{name: "forced OpenAPI", format: "markdown", chatType: "group", webhook: "valid", mention: true, forceAPI: true, wantTypes: []string{"sampleMarkdown"}},
		{name: "text with two attachments", format: "text", chatType: "group", webhook: "valid", mention: true, attachments: true, wantTypes: []string{"text", "sampleFile", "sampleFile"}},
		{name: "text with partial attachments", format: "text", chatType: "group", webhook: "valid", mention: true, attachments: true, partialAttachments: true, wantTypes: []string{"text", "sampleFile"}},
		{name: "text with all attachments failed", format: "text", chatType: "group", webhook: "valid", mention: true, attachments: true, allAttachmentsFail: true, wantTypes: []string{"text"}},
		{name: "text rejected before attachments", format: "text", chatType: "group", webhook: "valid", mention: true, attachments: true, failBody: true, wantTypes: []string{"text"}},
		{name: "media token rejected before text", format: "text", chatType: "group", webhook: "valid", mention: true, attachments: true, failToken: true},
		{name: "expired text with attachments", format: "text", chatType: "group", webhook: "expired", mention: true, attachments: true, wantTypes: []string{"sampleText", "sampleFile", "sampleFile"}},
		{name: "two attachments", format: "markdown", chatType: "group", webhook: "valid", mention: true, attachments: true, wantTypes: []string{"markdown", "sampleFile", "sampleFile"}},
		{name: "partial attachments", format: "markdown", chatType: "group", webhook: "valid", mention: true, attachments: true, partialAttachments: true, wantTypes: []string{"markdown", "sampleFile"}},
		{name: "markdown with all attachments failed", format: "markdown", chatType: "group", webhook: "valid", mention: true, attachments: true, allAttachmentsFail: true, wantTypes: []string{"markdown"}},
		{name: "markdown rejected before attachments", format: "markdown", chatType: "group", webhook: "valid", mention: true, attachments: true, failBody: true, wantTypes: []string{"markdown"}},
		{name: "media token rejected before markdown", format: "markdown", chatType: "group", webhook: "valid", mention: true, attachments: true, failToken: true},
		{name: "expired markdown with attachments", format: "markdown", chatType: "group", webhook: "expired", mention: true, attachments: true, wantTypes: []string{"sampleMarkdown", "sampleFile", "sampleFile"}},
		{name: "attachment only", format: "markdown", chatType: "group", webhook: "valid", mention: true, attachments: true, attachmentOnly: true, wantTypes: []string{"sampleFile", "sampleFile"}},
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
			if tc.failToken {
				delete(account.State, "access_token")
			}
			var types []string
			var notifications int
			client := &http.Client{Transport: testRoundTripFunc(func(r *http.Request) (*http.Response, error) {
				switch r.URL.Path {
				case "/v1.0/oauth2/accessToken":
					return testJSONResponse(map[string]any{"code": "InvalidClient", "message": "rejected"})
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
				if kind == "markdown" {
					wantText := "# Answer\n\n**Result**\n- item"
					if body["markdown"].(map[string]any)["text"] != wantText {
						t.Errorf("markdown body=%v, want %q", body["markdown"], wantText)
					}
				}
				// Keep the body intact and never synthesize mention markers.
				for _, key := range []string{"markdown", "msgParam"} {
					encoded, _ := json.Marshal(body[key])
					if strings.Contains(string(encoded), "@41") {
						t.Errorf("fake mention in %s: %s", key, encoded)
					}
				}
				if _, ok := body["at"]; ok {
					notifications++
					t.Errorf("unexpected outbound at field on %s", kind)
				}

				if tc.failBody || (tc.failNotice && kind == "text") {
					return testJSONResponse(map[string]any{"errcode": 400, "errmsg": "rejected"})
				}
				return testJSONResponse(map[string]any{"errcode": 0, "processQueryKey": "receipt"})
			})}
			req := sdk.OutboundMessage{AccountUUID: "account-1", ChatType: tc.chatType, ChatID: "chat", Text: "# Answer\n\n**Result**\n- item", Format: tc.format, Raw: map[string]any{"force_openapi": tc.forceAPI}}
			if tc.mention {
				req.Mentions = []sdk.MentionIdentity{{ID: "41", IDType: "user_id"}}
				req.MentionAll = true
				req.Raw["atUserIds"] = []string{"41"}
				req.Raw["atMobiles"] = []string{"13800000000"}
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
				if tc.allAttachmentsFail {
					req.Attachments[0].Path = path + ".missing"
					req.Attachments[1].Path = path + ".missing"
				}
				if tc.attachmentOnly {
					req.Text = ""
				}
			}
			result, err := NewConnector().Send(context.Background(), sdk.Runtime{Account: account, HTTPClient: client}, req)
			if (err != nil) != (tc.failBody || tc.failToken) {
				t.Fatalf("Send error=%v; body failure=%v", err, tc.failBody)
			}
			if !reflect.DeepEqual(types, tc.wantTypes) {
				t.Errorf("requests=%v, want %v", types, tc.wantTypes)
			}
			if notifications != 0 {
				t.Errorf("unexpected native mention requests: %d", notifications)
			}

			if tc.failBody || tc.failToken {
				return
			}
			if tc.attachments {
				firstStatus := sdk.AttachmentStatusSent
				if tc.allAttachmentsFail {
					firstStatus = sdk.AttachmentStatusFailed
				}
				if len(result.AttachmentResults) != 2 || result.AttachmentResults[0].Status != firstStatus {
					t.Fatalf("attachments=%+v", result.AttachmentResults)
				}
				want := sdk.AttachmentStatusSent
				if tc.partialAttachments || tc.allAttachmentsFail {
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
			if receipt.Raw.MentionDelivery != nil {
				t.Errorf("unexpected mention delivery status: %s", encoded)
			}
		})
	}
}
