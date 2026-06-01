package dingtalk

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	neturl "net/url"
	"strings"
	"time"
)

const defaultRequestTimeout = 15 * time.Second

type Client struct {
	BaseURL              string
	ClientID             string
	ClientSecret         string
	RobotCode            string
	AccessToken          string
	AccessTokenExpiresAt time.Time
	RequestTimeout       time.Duration
	HTTPClient           *http.Client
}

func NewClient(baseURL, clientID, clientSecret, robotCode string) *Client {
	if strings.TrimSpace(baseURL) == "" {
		baseURL = DefaultBaseURL
	}
	if strings.TrimSpace(robotCode) == "" {
		robotCode = clientID
	}
	return &Client{
		BaseURL:        baseURL,
		ClientID:       clientID,
		ClientSecret:   clientSecret,
		RobotCode:      robotCode,
		RequestTimeout: defaultRequestTimeout,
		HTTPClient:     http.DefaultClient,
	}
}

func (c *Client) Token(ctx context.Context) (string, error) {
	token, _, err := c.TokenWithExpiry(ctx, time.Now().UTC())
	return token, err
}

func (c *Client) TokenWithExpiry(ctx context.Context, now time.Time) (string, time.Time, error) {
	if strings.TrimSpace(c.AccessToken) != "" && c.AccessTokenExpiresAt.After(now.Add(5*time.Minute)) {
		return c.AccessToken, c.AccessTokenExpiresAt, nil
	}
	if strings.TrimSpace(c.ClientID) == "" {
		return "", time.Time{}, fmt.Errorf("client_id is required")
	}
	if strings.TrimSpace(c.ClientSecret) == "" {
		return "", time.Time{}, fmt.Errorf("client_secret is required")
	}
	var resp TokenResponse
	if err := c.doJSON(ctx, http.MethodPost, "/v1.0/oauth2/accessToken", map[string]any{
		"appKey":    c.ClientID,
		"appSecret": c.ClientSecret,
	}, &resp); err != nil {
		return "", time.Time{}, err
	}
	if resp.AccessToken == "" {
		return "", time.Time{}, fmt.Errorf("dingtalk access token failed: code=%s message=%s", resp.Code, resp.Message)
	}
	expiresIn := resp.ExpireIn
	if expiresIn <= 0 {
		expiresIn = 7200
	}
	expiresAt := now.Add(time.Duration(expiresIn) * time.Second)
	c.AccessToken = resp.AccessToken
	c.AccessTokenExpiresAt = expiresAt
	return resp.AccessToken, expiresAt, nil
}

func (c *Client) SendText(ctx context.Context, req SendTextRequest) (*SendTextResponse, error) {
	if strings.TrimSpace(req.ChatID) == "" {
		return nil, fmt.Errorf("chat_id is required")
	}
	if strings.TrimSpace(req.Text) == "" {
		return nil, fmt.Errorf("text is required")
	}
	token := strings.TrimSpace(c.AccessToken)
	if token == "" {
		var err error
		token, err = c.Token(ctx)
		if err != nil {
			return nil, err
		}
	}
	robotCode := strings.TrimSpace(req.RobotCode)
	if robotCode == "" {
		robotCode = strings.TrimSpace(c.RobotCode)
	}
	if robotCode == "" {
		robotCode = strings.TrimSpace(c.ClientID)
	}
	msgParam, err := json.Marshal(textMsgParam(req.Text, req.At))
	if err != nil {
		return nil, err
	}
	body := map[string]any{
		"robotCode": robotCode,
		"msgKey":    "sampleText",
		"msgParam":  string(msgParam),
	}
	path := "/v1.0/robot/oToMessages/batchSend"
	if req.ChatType == ChatTypeGroup {
		path = "/v1.0/robot/groupMessages/send"
		body["openConversationId"] = req.ChatID
	} else {
		body["userIds"] = []string{req.ChatID}
	}
	var resp SendTextResponse
	if err := c.doJSON(ctx, http.MethodPost, path, body, &resp, withAccessToken(token)); err != nil {
		return nil, err
	}
	if resp.Code != "" {
		return nil, fmt.Errorf("send text failed: code=%s message=%s", resp.Code, resp.Message)
	}
	return &resp, nil
}

func (c *Client) SendMarkdown(ctx context.Context, req SendMarkdownRequest) (*SendTextResponse, error) {
	if strings.TrimSpace(req.ChatID) == "" {
		return nil, fmt.Errorf("chat_id is required")
	}
	if strings.TrimSpace(req.Text) == "" {
		return nil, fmt.Errorf("text is required")
	}
	token := strings.TrimSpace(c.AccessToken)
	if token == "" {
		var err error
		token, err = c.Token(ctx)
		if err != nil {
			return nil, err
		}
	}
	robotCode := strings.TrimSpace(req.RobotCode)
	if robotCode == "" {
		robotCode = strings.TrimSpace(c.RobotCode)
	}
	if robotCode == "" {
		robotCode = strings.TrimSpace(c.ClientID)
	}
	msgParam, err := json.Marshal(markdownMsgParam(req.Title, req.Text, req.At))
	if err != nil {
		return nil, err
	}
	body := map[string]any{
		"robotCode": robotCode,
		"msgKey":    "sampleMarkdown",
		"msgParam":  string(msgParam),
	}
	path := "/v1.0/robot/oToMessages/batchSend"
	if req.ChatType == ChatTypeGroup {
		path = "/v1.0/robot/groupMessages/send"
		body["openConversationId"] = req.ChatID
	} else {
		body["userIds"] = []string{req.ChatID}
	}
	var resp SendTextResponse
	if err := c.doJSON(ctx, http.MethodPost, path, body, &resp, withAccessToken(token)); err != nil {
		return nil, err
	}
	if resp.Code != "" {
		return nil, fmt.Errorf("send markdown failed: code=%s message=%s", resp.Code, resp.Message)
	}
	return &resp, nil
}

func (c *Client) SendWebhookText(ctx context.Context, sessionWebhook string, text string) (*WebhookSendResponse, error) {
	return c.SendWebhookTextMessage(ctx, sessionWebhook, SendWebhookTextRequest{Text: text})
}

func (c *Client) SendWebhookTextMessage(ctx context.Context, sessionWebhook string, req SendWebhookTextRequest) (*WebhookSendResponse, error) {
	sessionWebhook = strings.TrimSpace(sessionWebhook)
	if sessionWebhook == "" {
		return nil, fmt.Errorf("session_webhook is required")
	}
	if !IsAllowedSessionWebhookURL(sessionWebhook) {
		return nil, fmt.Errorf("session_webhook url is not allowed")
	}
	if strings.TrimSpace(req.Text) == "" {
		return nil, fmt.Errorf("text is required")
	}
	body := map[string]any{
		"msgtype": "text",
		"text": map[string]string{
			"content": textWithAtSuffix(req.Text, req.At),
		},
	}
	if at := webhookAtParam(req.At); len(at) > 0 {
		body["at"] = at
	}
	var resp WebhookSendResponse
	if err := c.doJSONURL(ctx, http.MethodPost, sessionWebhook, body, &resp); err != nil {
		return nil, err
	}
	if resp.ErrCode != 0 {
		return nil, fmt.Errorf("session webhook send text failed: code=%d message=%s", resp.ErrCode, resp.ErrMsg)
	}
	return &resp, nil
}

func (c *Client) SendWebhookMarkdownMessage(ctx context.Context, sessionWebhook string, req SendWebhookMarkdownRequest) (*WebhookSendResponse, error) {
	sessionWebhook = strings.TrimSpace(sessionWebhook)
	if sessionWebhook == "" {
		return nil, fmt.Errorf("session_webhook is required")
	}
	if !IsAllowedSessionWebhookURL(sessionWebhook) {
		return nil, fmt.Errorf("session_webhook url is not allowed")
	}
	if strings.TrimSpace(req.Text) == "" {
		return nil, fmt.Errorf("text is required")
	}
	body := map[string]any{
		"msgtype": "markdown",
		"markdown": map[string]string{
			"title": markdownTitle(req.Title, req.Text),
			"text":  markdownWithAtSuffix(req.Text, req.At),
		},
	}
	if at := webhookAtParam(req.At); len(at) > 0 {
		body["at"] = at
	}
	var resp WebhookSendResponse
	if err := c.doJSONURL(ctx, http.MethodPost, sessionWebhook, body, &resp); err != nil {
		return nil, err
	}
	if resp.ErrCode != 0 {
		return nil, fmt.Errorf("session webhook send markdown failed: code=%d message=%s", resp.ErrCode, resp.ErrMsg)
	}
	return &resp, nil
}

func textMsgParam(text string, at AtOptions) map[string]any {
	return map[string]any{"content": textWithAtSuffix(text, at)}
}

func markdownMsgParam(title string, text string, at AtOptions) map[string]any {
	return map[string]any{
		"title": markdownTitle(title, text),
		"text":  markdownWithAtSuffix(text, at),
	}
}

func markdownTitle(title string, text string) string {
	if title = strings.TrimSpace(title); title != "" {
		return title
	}
	return titleFromMarkdown(text)
}

func titleFromMarkdown(text string) string {
	for _, line := range strings.Split(strings.TrimSpace(text), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		line = strings.TrimSpace(strings.TrimLeft(line, "#*>- \t"))
		if line == "" {
			continue
		}
		if len([]rune(line)) > 20 {
			return string([]rune(line)[:20])
		}
		return line
	}
	return "Message"
}

func textWithAtSuffix(text string, at AtOptions) string {
	out := strings.TrimSpace(text)
	ids := append(uniqueStrings(at.AtDingtalkIDs), uniqueStrings(at.AtUserIDs)...)
	ids = append(ids, uniqueStrings(at.AtMobiles)...)
	for _, id := range ids {
		if id != "" && !containsAtToken(out, id) {
			out = strings.TrimSpace(out + " @" + id)
		}
	}
	if at.AtAll && !strings.Contains(strings.ToLower(out), "@all") {
		out = strings.TrimSpace(out + " @all")
	}
	return out
}

func markdownWithAtSuffix(text string, at AtOptions) string {
	out := strings.TrimSpace(text)
	var suffixes []string
	ids := append(uniqueStrings(at.AtDingtalkIDs), uniqueStrings(at.AtUserIDs)...)
	ids = append(ids, uniqueStrings(at.AtMobiles)...)
	for _, id := range ids {
		if id != "" && !containsAtToken(out, id) {
			suffixes = append(suffixes, "@"+id)
		}
	}
	if at.AtAll && !strings.Contains(strings.ToLower(out), "@all") {
		suffixes = append(suffixes, "@all")
	}
	if len(suffixes) == 0 {
		return out
	}
	if out == "" {
		return strings.Join(suffixes, " ")
	}
	return out + "\n\n" + strings.Join(suffixes, " ")
}

func containsAtToken(text string, id string) bool {
	token := "@" + strings.TrimSpace(id)
	for _, field := range strings.Fields(text) {
		if strings.Trim(field, " \t\r\n,.;:!?，。！？、()[]{}<>") == token {
			return true
		}
	}
	return false
}

func webhookAtParam(at AtOptions) map[string]any {
	out := make(map[string]any)
	if values := uniqueStrings(at.AtUserIDs); len(values) > 0 {
		out["atUserIds"] = values
	}
	if values := uniqueStrings(at.AtDingtalkIDs); len(values) > 0 {
		out["atDingtalkIds"] = values
	}
	if values := uniqueStrings(at.AtMobiles); len(values) > 0 {
		out["atMobiles"] = values
	}
	if at.AtAll {
		out["isAtAll"] = true
	} else if len(out) > 0 {
		out["isAtAll"] = false
	}
	return out
}

func uniqueStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

type requestOption func(*http.Request)

func withAccessToken(token string) requestOption {
	return func(req *http.Request) {
		req.Header.Set("x-acs-dingtalk-access-token", token)
	}
}

func (c *Client) doJSON(ctx context.Context, method, path string, body any, out any, opts ...requestOption) error {
	return c.doJSONURL(ctx, method, c.url(path), body, out, opts...)
}

func (c *Client) doJSONURL(ctx context.Context, method, targetURL string, body any, out any, opts ...requestOption) error {
	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("encode request: %w", err)
		}
		reader = bytes.NewReader(data)
	}
	timeout := c.RequestTimeout
	if timeout <= 0 {
		timeout = defaultRequestTimeout
	}
	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, method, targetURL, reader)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "BeakAgentDingTalk/0.1.0")
	if body != nil {
		req.Header.Set("Content-Type", "application/json; charset=utf-8")
	}
	for _, opt := range opts {
		opt(req)
	}
	client := c.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		if urlErr, ok := err.(*neturl.Error); ok {
			return fmt.Errorf("%s %s failed: %v", method, sanitizeURLForError(targetURL), urlErr.Err)
		}
		return fmt.Errorf("%s %s failed: %w", method, sanitizeURLForError(targetURL), err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("%s %s failed: status=%d body=%s", method, sanitizeURLForError(targetURL), resp.StatusCode, string(data))
	}
	if out == nil || len(bytes.TrimSpace(data)) == 0 {
		return nil
	}
	if err := json.Unmarshal(data, out); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	switch response := out.(type) {
	case *SendTextResponse:
		_ = json.Unmarshal(data, &response.Raw)
	case *WebhookSendResponse:
		_ = json.Unmarshal(data, &response.Raw)
	}
	return nil
}

func (c *Client) url(path string) string {
	return strings.TrimRight(c.BaseURL, "/") + "/" + strings.TrimLeft(path, "/")
}

func IsAllowedSessionWebhookURL(rawURL string) bool {
	parsed, err := neturl.Parse(strings.TrimSpace(rawURL))
	if err != nil || parsed == nil {
		return false
	}
	if !strings.EqualFold(parsed.Scheme, "https") {
		return false
	}
	host := strings.ToLower(strings.TrimSpace(parsed.Hostname()))
	return host == "dingtalk.com" || strings.HasSuffix(host, ".dingtalk.com")
}

func sanitizeURLForError(rawURL string) string {
	parsed, err := neturl.Parse(strings.TrimSpace(rawURL))
	if err != nil || parsed == nil {
		return "<invalid-url>"
	}
	parsed.User = nil
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return parsed.String()
}
