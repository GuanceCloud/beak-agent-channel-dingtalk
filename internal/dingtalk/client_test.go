package dingtalk

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
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

func TestClientUploadMediaUsesDeclaredFilename(t *testing.T) {
	path := t.TempDir() + "/temporary-upload"
	if err := os.WriteFile(path, []byte("media"), 0o600); err != nil {
		t.Fatal(err)
	}
	client := NewClient("https://api.dingtalk.test", "client-1", "secret-1", "robot-1")
	client.AccessToken = "token-1"
	client.HTTPClient = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path != "/v1.0/robot/messageFiles/upload" {
			t.Fatalf("unexpected request: %s", r.URL.Path)
		}
		if err := r.ParseMultipartForm(1024 * 1024); err != nil {
			t.Fatal(err)
		}
		file, header, err := r.FormFile("file")
		if err != nil {
			t.Fatal(err)
		}
		_ = file.Close()
		if header.Filename != "report.pdf" {
			t.Fatalf("uploaded filename=%q", header.Filename)
		}
		return jsonResponse(map[string]any{"downloadCode": "download-1"})
	})}

	code, err := client.UploadMedia(context.Background(), path, "file", "robot-1", "report.pdf")
	if err != nil || code != "download-1" {
		t.Fatalf("download code=%q error=%v", code, err)
	}
}

func TestClientDownloadURLWritesTemporaryFile(t *testing.T) {
	client := NewClient("https://api.dingtalk.test", "client-1", "secret-1", "robot-1")
	client.HTTPClient = &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.URL.String() != "http://media.dingtalk.test/image.png" {
			t.Fatalf("download URL = %s", request.URL.String())
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader("image-data")),
			Header:     make(http.Header),
			Request:    request,
		}, nil
	})}

	path, cleanup, err := client.DownloadURL(t.Context(), "http://media.dingtalk.test/image.png")
	if err != nil {
		t.Fatal(err)
	}
	if cleanup == nil {
		t.Fatal("cleanup is nil")
	}
	body, err := os.ReadFile(path)
	if err != nil || string(body) != "image-data" {
		t.Fatalf("downloaded body = %q error=%v", body, err)
	}
	cleanup()
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("temporary file still exists: %v", err)
	}
}

func TestClientDownloadURLRejectsUnsafeURLsAndRedirects(t *testing.T) {
	client := NewClient("https://api.dingtalk.test", "client-1", "secret-1", "robot-1")
	requestCount := 0
	client.HTTPClient = &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		requestCount++
		return &http.Response{
			StatusCode: http.StatusFound,
			Body:       io.NopCloser(strings.NewReader("")),
			Header: http.Header{
				"Location": []string{"https://127.0.0.1/private"},
			},
			Request: request,
		}, nil
	})}

	if _, _, err := client.DownloadURL(t.Context(), "ftp://media.dingtalk.test/image.png"); err == nil ||
		!strings.Contains(err.Error(), "scheme is not allowed") {
		t.Fatalf("FTP URL error = %v", err)
	}
	if _, _, err := client.DownloadURL(t.Context(), "http://127.0.0.1/image.png"); err == nil ||
		!strings.Contains(err.Error(), "host is not allowed") {
		t.Fatalf("loopback URL error = %v", err)
	}
	if requestCount != 0 {
		t.Fatalf("unsafe initial URL sent %d requests", requestCount)
	}
	if _, _, err := client.DownloadURL(t.Context(), "https://media.dingtalk.test/image.png"); err == nil ||
		!strings.Contains(err.Error(), "host is not allowed") {
		t.Fatalf("private redirect error = %v", err)
	}
	if requestCount != 1 {
		t.Fatalf("redirect test sent %d requests, want 1", requestCount)
	}
}

func TestClientDownloadURLRedactsSignedQueryFromTransportError(t *testing.T) {
	client := NewClient("https://api.dingtalk.test", "client-1", "secret-1", "robot-1")
	client.HTTPClient = &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		return nil, errors.New("network unavailable")
	})}
	_, _, err := client.DownloadURL(t.Context(), "http://media.dingtalk.test/image.png?signature=secret-token")
	if err == nil {
		t.Fatal("DownloadURL() error=nil")
	}
	if strings.Contains(err.Error(), "signature") || strings.Contains(err.Error(), "secret-token") {
		t.Fatalf("transport error leaks signed URL: %v", err)
	}
	if !strings.Contains(err.Error(), "http://media.dingtalk.test/image.png") {
		t.Fatalf("transport error missing sanitized URL: %v", err)
	}
}

func TestValidateMediaDownloadURLNormalizesProtocolRelativeURL(t *testing.T) {
	parsed, err := validateMediaDownloadURL("//media.dingtalk.test/image.png")
	if err != nil {
		t.Fatal(err)
	}
	if parsed.String() != "https://media.dingtalk.test/image.png" {
		t.Fatalf("normalized URL = %q", parsed.String())
	}
	if _, err := validateMediaDownloadURL("https://media.dingtalk.test:8443/image.png"); err == nil ||
		!strings.Contains(err.Error(), "port is not allowed") {
		t.Fatalf("non-standard port error = %v", err)
	}
}

func TestMediaNetworkDialerRejectsPrivateDNSAddress(t *testing.T) {
	dialCalls := 0
	dialer := mediaNetworkDialer{
		lookupIP: func(context.Context, string) ([]net.IPAddr, error) {
			return []net.IPAddr{{IP: net.ParseIP("10.0.0.8")}}, nil
		},
		dialContext: func(context.Context, string, string) (net.Conn, error) {
			dialCalls++
			return nil, nil
		},
	}
	if _, err := dialer.DialContext(t.Context(), "tcp", "media.dingtalk.test:80"); err == nil ||
		!strings.Contains(err.Error(), "host is not allowed") {
		t.Fatalf("private DNS address error = %v", err)
	}
	if dialCalls != 0 {
		t.Fatalf("private DNS address reached dialer %d times", dialCalls)
	}
}

func TestMediaNetworkDialerPinsPublicDNSAddress(t *testing.T) {
	clientConnection, serverConnection := net.Pipe()
	defer serverConnection.Close()
	var dialAddress string
	dialer := mediaNetworkDialer{
		lookupIP: func(context.Context, string) ([]net.IPAddr, error) {
			return []net.IPAddr{{IP: net.ParseIP("8.8.8.8")}}, nil
		},
		dialContext: func(_ context.Context, network, address string) (net.Conn, error) {
			if network != "tcp" {
				t.Fatalf("network = %q", network)
			}
			dialAddress = address
			return clientConnection, nil
		},
	}
	connection, err := dialer.DialContext(t.Context(), "tcp", "media.dingtalk.test:80")
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	if dialAddress != "8.8.8.8:80" {
		t.Fatalf("dial address = %q", dialAddress)
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

func TestClientTokenWithExpiryCachesToken(t *testing.T) {
	var tokenCalls int
	httpClient := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path != "/v1.0/oauth2/accessToken" {
			t.Fatalf("unexpected request: %s", r.URL.Path)
		}
		tokenCalls++
		return jsonResponse(map[string]any{"accessToken": "token-1", "expireIn": 7200})
	})}
	client := NewClient("https://api.dingtalk.test", "client-1", "secret-1", "robot-1")
	client.HTTPClient = httpClient
	now := time.Now().UTC()
	token, expiresAt, err := client.TokenWithExpiry(context.Background(), now)
	if err != nil {
		t.Fatal(err)
	}
	if token != "token-1" || !expiresAt.After(now) {
		t.Fatalf("token=%q expiresAt=%s", token, expiresAt)
	}
	token, _, err = client.TokenWithExpiry(context.Background(), now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if token != "token-1" || tokenCalls != 1 {
		t.Fatalf("token=%q tokenCalls=%d", token, tokenCalls)
	}
}

func TestClientSendWebhookText(t *testing.T) {
	httpClient := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.String() != "https://oapi.dingtalk.com/robot/sendBySession?session=s1" {
			t.Fatalf("unexpected request: %s", r.URL.String())
		}
		var body struct {
			MsgType string `json:"msgtype"`
			Text    struct {
				Content string `json:"content"`
			} `json:"text"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if body.MsgType != "text" || body.Text.Content != "reply" {
			t.Fatalf("body=%+v", body)
		}
		return jsonResponse(map[string]any{"errcode": 0, "errmsg": "ok"})
	})}
	client := NewClient("https://api.dingtalk.test", "client-1", "secret-1", "robot-1")
	client.HTTPClient = httpClient
	resp, err := client.SendWebhookText(context.Background(), "https://oapi.dingtalk.com/robot/sendBySession?session=s1", "reply")
	if err != nil {
		t.Fatal(err)
	}
	if resp.ErrCode != 0 || resp.Raw["errmsg"] != "ok" {
		t.Fatalf("resp=%+v", resp)
	}
}

func TestClientSendMarkdown(t *testing.T) {
	httpClient := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path != "/v1.0/robot/groupMessages/send" {
			t.Fatalf("unexpected request: %s", r.URL.Path)
		}
		var body struct {
			RobotCode          string `json:"robotCode"`
			MsgKey             string `json:"msgKey"`
			MsgParam           string `json:"msgParam"`
			OpenConversationID string `json:"openConversationId"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if body.RobotCode != "robot-1" || body.MsgKey != "sampleMarkdown" || body.OpenConversationID != "cid-group" {
			t.Fatalf("body=%+v", body)
		}
		var param map[string]string
		if err := json.Unmarshal([]byte(body.MsgParam), &param); err != nil {
			t.Fatal(err)
		}
		if param["title"] != "日志查询" || param["text"] != "# 日志查询\n- 错误日志\n\n@staff-1" {
			t.Fatalf("param=%+v", param)
		}
		return jsonResponse(map[string]any{"processQueryKey": "pqk-markdown"})
	})}
	client := NewClient("https://api.dingtalk.test", "client-1", "secret-1", "robot-1")
	client.HTTPClient = httpClient
	client.AccessToken = "token-1"
	client.AccessTokenExpiresAt = time.Now().Add(time.Hour)
	resp, err := client.SendMarkdown(context.Background(), SendMarkdownRequest{
		ChatType: ChatTypeGroup,
		ChatID:   "cid-group",
		Title:    "日志查询",
		Text:     "# 日志查询\n- 错误日志",
		At:       AtOptions{AtUserIDs: []string{"staff-1"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.ProcessQueryKey != "pqk-markdown" {
		t.Fatalf("resp=%+v", resp)
	}
}

func TestClientSendMarkdownDerivesTitleFromContent(t *testing.T) {
	httpClient := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path != "/v1.0/robot/groupMessages/send" {
			t.Fatalf("unexpected request: %s", r.URL.Path)
		}
		var body struct {
			MsgParam string `json:"msgParam"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		var param map[string]string
		if err := json.Unmarshal([]byte(body.MsgParam), &param); err != nil {
			t.Fatal(err)
		}
		if param["title"] != "这个明显就是用 正文内容截断之后作为标题" || param["text"] != "# 这个明显就是用 正文内容截断之后作为标题" {
			t.Fatalf("param=%+v", param)
		}
		return jsonResponse(map[string]any{"processQueryKey": "pqk-markdown-default-title"})
	})}
	client := NewClient("https://api.dingtalk.test", "client-1", "secret-1", "robot-1")
	client.HTTPClient = httpClient
	client.AccessToken = "token-1"
	client.AccessTokenExpiresAt = time.Now().Add(time.Hour)
	resp, err := client.SendMarkdown(context.Background(), SendMarkdownRequest{
		ChatType: ChatTypeGroup,
		ChatID:   "cid-group",
		Text:     "# 这个明显就是用 正文内容截断之后作为标题",
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.ProcessQueryKey != "pqk-markdown-default-title" {
		t.Fatalf("resp=%+v", resp)
	}
}

func TestClientSendWebhookTextRejectsUntrustedURL(t *testing.T) {
	client := NewClient("https://api.dingtalk.test", "client-1", "secret-1", "robot-1")
	if _, err := client.SendWebhookText(context.Background(), "https://example.test/robot/sendBySession?session=s1", "reply"); err == nil || !strings.Contains(err.Error(), "not allowed") {
		t.Fatalf("SendWebhookText() error=%v, want not allowed", err)
	}
}

func TestClientSendWebhookTextRedactsURLQueryInError(t *testing.T) {
	httpClient := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusInternalServerError,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader(`{"errcode":500}`)),
		}, nil
	})}
	client := NewClient("https://api.dingtalk.test", "client-1", "secret-1", "robot-1")
	client.HTTPClient = httpClient
	_, err := client.SendWebhookText(context.Background(), "https://oapi.dingtalk.com/robot/sendBySession?session=secret-token", "reply")
	if err == nil {
		t.Fatal("SendWebhookText() error=nil, want status error")
	}
	if strings.Contains(err.Error(), "secret-token") || strings.Contains(err.Error(), "session=") {
		t.Fatalf("error leaks query: %v", err)
	}
	if !strings.Contains(err.Error(), "https://oapi.dingtalk.com/robot/sendBySession") {
		t.Fatalf("error missing sanitized url: %v", err)
	}
}

func TestClientSendWebhookMarkdownMessage(t *testing.T) {
	httpClient := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.String() != "https://oapi.dingtalk.com/robot/sendBySession?session=s1" {
			t.Fatalf("unexpected request: %s", r.URL.String())
		}
		var body struct {
			MsgType  string `json:"msgtype"`
			Markdown struct {
				Title string `json:"title"`
				Text  string `json:"text"`
			} `json:"markdown"`
			At struct {
				AtUserIDs []string `json:"atUserIds"`
			} `json:"at"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if body.MsgType != "markdown" || body.Markdown.Title != "日志查询" || body.Markdown.Text != "# 日志查询\n- 错误日志\n\n@staff-1" {
			t.Fatalf("body=%+v", body)
		}
		if len(body.At.AtUserIDs) != 1 || body.At.AtUserIDs[0] != "staff-1" {
			t.Fatalf("at=%+v", body.At)
		}
		return jsonResponse(map[string]any{"errcode": 0, "errmsg": "ok"})
	})}
	client := NewClient("https://api.dingtalk.test", "client-1", "secret-1", "robot-1")
	client.HTTPClient = httpClient
	_, err := client.SendWebhookMarkdownMessage(context.Background(), "https://oapi.dingtalk.com/robot/sendBySession?session=s1", SendWebhookMarkdownRequest{
		Title: "日志查询",
		Text:  "# 日志查询\n- 错误日志",
		At:    AtOptions{AtUserIDs: []string{"staff-1"}},
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestClientSendWebhookTextMessageMentions(t *testing.T) {
	httpClient := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
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
				AtMobiles     []string `json:"atMobiles"`
				IsAtAll       bool     `json:"isAtAll"`
			} `json:"at"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if body.MsgType != "text" || body.Text.Content != "reply @ding-1 @staff-1 @13800000000 @all" {
			t.Fatalf("body=%+v", body)
		}
		if len(body.At.AtUserIDs) != 1 || body.At.AtUserIDs[0] != "staff-1" ||
			len(body.At.AtDingtalkIDs) != 1 || body.At.AtDingtalkIDs[0] != "ding-1" ||
			len(body.At.AtMobiles) != 1 || body.At.AtMobiles[0] != "13800000000" ||
			!body.At.IsAtAll {
			t.Fatalf("at=%+v", body.At)
		}
		return jsonResponse(map[string]any{"errcode": 0, "errmsg": "ok"})
	})}
	client := NewClient("https://api.dingtalk.test", "client-1", "secret-1", "robot-1")
	client.HTTPClient = httpClient
	_, err := client.SendWebhookTextMessage(context.Background(), "https://oapi.dingtalk.com/robot/sendBySession?session=s1", SendWebhookTextRequest{
		Text: "reply",
		At: AtOptions{
			AtUserIDs:     []string{"staff-1"},
			AtDingtalkIDs: []string{"ding-1"},
			AtMobiles:     []string{"13800000000"},
			AtAll:         true,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestClientSendWebhookTextMessageMentionBoundary(t *testing.T) {
	httpClient := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		var body struct {
			Text struct {
				Content string `json:"content"`
			} `json:"text"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if body.Text.Content != "reply @staff-12 @staff-1" {
			t.Fatalf("content=%q", body.Text.Content)
		}
		return jsonResponse(map[string]any{"errcode": 0, "errmsg": "ok"})
	})}
	client := NewClient("https://api.dingtalk.test", "client-1", "secret-1", "robot-1")
	client.HTTPClient = httpClient
	_, err := client.SendWebhookTextMessage(context.Background(), "https://oapi.dingtalk.com/robot/sendBySession?session=s1", SendWebhookTextRequest{
		Text: "reply @staff-12",
		At:   AtOptions{AtUserIDs: []string{"staff-1"}},
	})
	if err != nil {
		t.Fatal(err)
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
		"text":{"content":" hello group ","at":{"atUserIds":["staff-2"],"atDingtalkIds":["ding-1"],"atMobiles":["13800000000"],"isAtAll":true}}
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
	if !event.IsAtAll || len(event.AtUserIDs) != 1 || event.AtUserIDs[0] != "staff-2" ||
		len(event.AtDingtalkIDs) != 1 || event.AtDingtalkIDs[0] != "ding-1" ||
		len(event.AtMobiles) != 1 || event.AtMobiles[0] != "13800000000" {
		t.Fatalf("mentions=%v %v %v mention_all=%v", event.AtUserIDs, event.AtDingtalkIDs, event.AtMobiles, event.IsAtAll)
	}
}

func TestParseStreamEventTopLevelAt(t *testing.T) {
	event, err := ParseStreamEvent([]byte(`{
		"conversationType":"2",
		"conversationId":"cid-group",
		"senderStaffId":"staff-1",
		"msgId":"msg-1",
		"msgtype":"text",
		"isAtAll":true,
		"at":{"atUserIds":["staff-2"],"atDingtalkIds":["ding-1"],"atMobiles":["13800000000"]},
		"text":{"content":"hello group"}
	}`))
	if err != nil {
		t.Fatal(err)
	}
	if !event.IsAtAll || len(event.AtUserIDs) != 1 || event.AtUserIDs[0] != "staff-2" ||
		len(event.AtDingtalkIDs) != 1 || event.AtDingtalkIDs[0] != "ding-1" ||
		len(event.AtMobiles) != 1 || event.AtMobiles[0] != "13800000000" {
		t.Fatalf("mentions=%v %v %v mention_all=%v", event.AtUserIDs, event.AtDingtalkIDs, event.AtMobiles, event.IsAtAll)
	}
}

func TestParseStreamEventTopLevelContentText(t *testing.T) {
	event, err := ParseStreamEvent([]byte(`{
		"conversationType":"2",
		"conversationId":"cid-group",
		"senderStaffId":"staff-1",
		"msgId":"msg-1",
		"msgtype":"text",
		"content":" hello top-level "
	}`))
	if err != nil {
		t.Fatal(err)
	}
	if got := event.Text(); got != "hello top-level" {
		t.Fatalf("text=%q", got)
	}
}

func TestStreamAck(t *testing.T) {
	ack := StreamAck("delivery-1", "")
	if ack.Code != 200 || ack.Headers["messageId"] != "delivery-1" || ack.Data != `{"response":null}` {
		t.Fatalf("ack=%+v", ack)
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
