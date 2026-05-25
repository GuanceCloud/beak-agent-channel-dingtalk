package dingtalk

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestClientTokenAndSendGroupText(t *testing.T) {
	var sawToken bool
	var sawSend bool
	httpClient := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/v1.0/oauth2/accessToken":
			sawToken = true
			if r.Method != http.MethodPost {
				t.Fatalf("token method=%s", r.Method)
			}
			var body map[string]string
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			if body["appKey"] != "client-1" || body["appSecret"] != "secret-1" {
				t.Fatalf("token body=%+v", body)
			}
			return jsonResponse(map[string]any{"accessToken": "token-1", "expireIn": 7200})
		case "/v1.0/robot/groupMessages/send":
			sawSend = true
			if r.Method != http.MethodPost {
				t.Fatalf("send method=%s", r.Method)
			}
			if got := r.Header.Get("x-acs-dingtalk-access-token"); got != "token-1" {
				t.Fatalf("token header=%q", got)
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
			if body.RobotCode != "robot-1" || body.OpenConversationID != "cid-group" || body.MsgKey != "sampleText" {
				t.Fatalf("send body=%+v", body)
			}
			var param map[string]string
			if err := json.Unmarshal([]byte(body.MsgParam), &param); err != nil {
				t.Fatal(err)
			}
			if param["content"] != "hello" {
				t.Fatalf("msgParam=%+v", param)
			}
			return jsonResponse(map[string]any{"processQueryKey": "pqk-1"})
		default:
			t.Fatalf("unexpected request: %s", r.URL.Path)
		}
		return nil, nil
	})}

	client := NewClient("https://api.dingtalk.test", "client-1", "secret-1", "robot-1")
	client.HTTPClient = httpClient
	resp, err := client.SendText(context.Background(), SendTextRequest{
		ChatType: ChatTypeGroup,
		ChatID:   "cid-group",
		Text:     "hello",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !sawToken || !sawSend {
		t.Fatalf("sawToken=%v sawSend=%v", sawToken, sawSend)
	}
	if resp.ProcessQueryKey != "pqk-1" {
		t.Fatalf("resp=%+v", resp)
	}
}

func TestClientSendDirectTextWithCachedToken(t *testing.T) {
	httpClient := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path != "/v1.0/robot/oToMessages/batchSend" {
			t.Fatalf("unexpected request: %s", r.URL.Path)
		}
		if got := r.Header.Get("x-acs-dingtalk-access-token"); got != "cached-token" {
			t.Fatalf("token header=%q", got)
		}
		var body struct {
			RobotCode string   `json:"robotCode"`
			UserIDs   []string `json:"userIds"`
			MsgKey    string   `json:"msgKey"`
			MsgParam  string   `json:"msgParam"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if body.RobotCode != "client-1" || len(body.UserIDs) != 1 || body.UserIDs[0] != "user-1" || body.MsgKey != "sampleText" {
			t.Fatalf("send body=%+v", body)
		}
		return jsonResponse(map[string]any{"processQueryKey": "pqk-direct"})
	})}

	client := NewClient("https://api.dingtalk.test", "client-1", "secret-1", "")
	client.HTTPClient = httpClient
	client.AccessToken = "cached-token"
	resp, err := client.SendText(context.Background(), SendTextRequest{
		ChatType: ChatTypeDirect,
		ChatID:   "user-1",
		Text:     "hello direct",
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.ProcessQueryKey != "pqk-direct" {
		t.Fatalf("resp=%+v", resp)
	}
}

func TestParseStreamEventText(t *testing.T) {
	event, err := ParseStreamEvent([]byte(`{
		"conversationType":"2",
		"conversationId":"cid-group",
		"conversationTitle":"Team",
		"senderStaffId":"staff-1",
		"senderNick":"Alice",
		"msgId":"msg-1",
		"msgtype":"text",
		"text":{"content":" hello group "}
	}`))
	if err != nil {
		t.Fatal(err)
	}
	if got := event.Text(); got != "hello group" {
		t.Fatalf("text=%q", got)
	}
	chat := event.ChatIdentity()
	if chat.ChatType != ChatTypeGroup || chat.ChatID != "cid-group" || chat.SenderID != "staff-1" || chat.StateKey() != "group:cid-group" {
		t.Fatalf("chat=%+v", chat)
	}
	if got := event.DedupeKey("account-1"); got != "account-1:message:msg-1" {
		t.Fatalf("dedupe=%q", got)
	}
}

func TestParseStreamEnvelopeAndRichText(t *testing.T) {
	event, err := ParseStreamEvent([]byte(`{
		"headers":{"messageId":"delivery-1"},
		"data":"{\"conversationType\":\"1\",\"senderId\":\"user-1\",\"msgtype\":\"richText\",\"richText\":{\"richTextList\":[{\"text\":\"hello\"},{\"text\":\" world\"}]}}"
	}`))
	if err != nil {
		t.Fatal(err)
	}
	if event.DeliveryMessageID != "delivery-1" {
		t.Fatalf("delivery=%q", event.DeliveryMessageID)
	}
	if got := event.Text(); got != "hello world" {
		t.Fatalf("text=%q", got)
	}
	chat := event.ChatIdentity()
	if chat.ChatType != ChatTypeDirect || chat.ChatID != "user-1" || chat.SenderID != "user-1" {
		t.Fatalf("chat=%+v", chat)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func jsonResponse(body any) (*http.Response, error) {
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
