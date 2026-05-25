package dingtalk

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

const defaultRequestTimeout = 15 * time.Second

type Client struct {
	BaseURL        string
	ClientID       string
	ClientSecret   string
	RobotCode      string
	AccessToken    string
	RequestTimeout time.Duration
	HTTPClient     *http.Client
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
	if strings.TrimSpace(c.ClientID) == "" {
		return "", fmt.Errorf("client_id is required")
	}
	if strings.TrimSpace(c.ClientSecret) == "" {
		return "", fmt.Errorf("client_secret is required")
	}
	var resp TokenResponse
	if err := c.doJSON(ctx, http.MethodPost, "/v1.0/oauth2/accessToken", map[string]any{
		"appKey":    c.ClientID,
		"appSecret": c.ClientSecret,
	}, &resp); err != nil {
		return "", err
	}
	if resp.AccessToken == "" {
		return "", fmt.Errorf("dingtalk access token failed: code=%s message=%s", resp.Code, resp.Message)
	}
	return resp.AccessToken, nil
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
	msgParam, err := json.Marshal(map[string]string{"content": req.Text})
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

type requestOption func(*http.Request)

func withAccessToken(token string) requestOption {
	return func(req *http.Request) {
		req.Header.Set("x-acs-dingtalk-access-token", token)
	}
}

func (c *Client) doJSON(ctx context.Context, method, path string, body any, out any, opts ...requestOption) error {
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
	req, err := http.NewRequestWithContext(reqCtx, method, c.url(path), reader)
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
		return err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("%s %s failed: status=%d body=%s", method, path, resp.StatusCode, string(data))
	}
	if out == nil || len(bytes.TrimSpace(data)) == 0 {
		return nil
	}
	if err := json.Unmarshal(data, out); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	if response, ok := out.(*SendTextResponse); ok {
		_ = json.Unmarshal(data, &response.Raw)
	}
	return nil
}

func (c *Client) url(path string) string {
	return strings.TrimRight(c.BaseURL, "/") + "/" + strings.TrimLeft(path, "/")
}
