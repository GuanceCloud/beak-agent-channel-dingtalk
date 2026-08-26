package dingtalk

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net"
	"net/http"
	neturl "net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const defaultRequestTimeout = 15 * time.Second
const maxMediaBytes int64 = 32 << 20

func blockedMediaIP(ip net.IP) bool {
	if ip == nil {
		return true
	}
	if ipv4 := ip.To4(); ipv4 != nil {
		ip = ipv4
		// Go's IsPrivate does not classify the shared address space as private.
		if ip[0] == 100 && ip[1]&0xc0 == 0x40 {
			return true
		}
	}
	return !ip.IsGlobalUnicast() || ip.IsPrivate()
}

func blockedMediaHost(host string) bool {
	host = strings.Trim(strings.ToLower(strings.TrimSpace(host)), "[]")
	if host == "" || host == "localhost" || strings.HasSuffix(host, ".localhost") {
		return true
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return false
	}
	return blockedMediaIP(ip)
}

type mediaNetworkDialer struct {
	lookupIP    func(context.Context, string) ([]net.IPAddr, error)
	dialContext func(context.Context, string, string) (net.Conn, error)
}

func (d mediaNetworkDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, fmt.Errorf("dingtalk media download network address is invalid")
	}
	addresses := []net.IPAddr(nil)
	if ip := net.ParseIP(strings.Trim(host, "[]")); ip != nil {
		addresses = []net.IPAddr{{IP: ip}}
	} else {
		if d.lookupIP == nil {
			return nil, fmt.Errorf("dingtalk media download URL host resolver is not configured")
		}
		addresses, err = d.lookupIP(ctx, host)
		if err != nil || len(addresses) == 0 {
			return nil, fmt.Errorf("dingtalk media download URL host could not be resolved")
		}
	}
	for _, candidate := range addresses {
		if blockedMediaIP(candidate.IP) {
			return nil, fmt.Errorf("dingtalk media download URL host is not allowed")
		}
	}
	if d.dialContext == nil {
		return nil, fmt.Errorf("dingtalk media download network dialer is not configured")
	}
	var lastErr error
	for _, candidate := range addresses {
		ip := candidate.IP
		if network == "tcp4" && ip.To4() == nil {
			continue
		}
		if network == "tcp6" && ip.To4() != nil {
			continue
		}
		connection, dialErr := d.dialContext(ctx, network, net.JoinHostPort(ip.String(), port))
		if dialErr == nil {
			return connection, nil
		}
		lastErr = dialErr
	}
	if lastErr == nil {
		return nil, fmt.Errorf("dingtalk media download URL host has no compatible address")
	}
	return nil, lastErr
}

func mediaDownloadHTTPClient(base *http.Client, timeout time.Duration) *http.Client {
	if base == nil {
		base = http.DefaultClient
	}
	client := *base
	var transport *http.Transport
	switch baseTransport := base.Transport.(type) {
	case nil:
		defaultTransport, ok := http.DefaultTransport.(*http.Transport)
		if !ok {
			return &client
		}
		transport = defaultTransport.Clone()
	case *http.Transport:
		transport = baseTransport.Clone()
	default:
		// A caller-provided RoundTripper owns its network policy. Structural URL
		// and redirect validation still run before it receives a request.
		return &client
	}
	networkDialer := &net.Dialer{Timeout: timeout, KeepAlive: 30 * time.Second}
	mediaDialer := mediaNetworkDialer{
		lookupIP:    net.DefaultResolver.LookupIPAddr,
		dialContext: networkDialer.DialContext,
	}
	transport.Proxy = nil
	transport.DialContext = mediaDialer.DialContext
	transport.DialTLSContext = nil
	transport.DisableKeepAlives = true
	client.Transport = transport
	return &client
}

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
		return "", time.Time{}, credentialRejected("client_id is required")
	}
	if strings.TrimSpace(c.ClientSecret) == "" {
		return "", time.Time{}, credentialRejected("client_secret is required")
	}
	var resp TokenResponse
	if err := c.doJSON(ctx, http.MethodPost, "/v1.0/oauth2/accessToken", map[string]any{
		"appKey":    c.ClientID,
		"appSecret": c.ClientSecret,
	}, &resp); err != nil {
		return "", time.Time{}, err
	}
	if resp.AccessToken == "" {
		message := fmt.Sprintf("dingtalk access token failed: code=%s message=%s", resp.Code, resp.Message)
		if credentialResponseRejected(resp.Code, resp.Message) {
			return "", time.Time{}, credentialRejected(message)
		}
		return "", time.Time{}, transientFailure(message)
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

func (c *Client) SendMedia(ctx context.Context, req SendMediaRequest) (*SendTextResponse, error) {
	if strings.TrimSpace(req.ChatID) == "" {
		return nil, fmt.Errorf("chat_id is required")
	}
	if strings.TrimSpace(req.MediaID) == "" {
		return nil, fmt.Errorf("media_id is required")
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
	msgKey := "sampleFileMsg"
	msgParam := map[string]string{"mediaId": req.MediaID}
	switch strings.TrimSpace(req.Kind) {
	case "image", "sticker":
		msgKey = "sampleImageMsg"
		msgParam = map[string]string{"photoURL": req.MediaID}
	case "audio":
		msgKey = "sampleAudioMsg"
	case "video":
		msgKey = "sampleVideoMsg"
	}
	encoded, err := json.Marshal(msgParam)
	if err != nil {
		return nil, err
	}
	body := map[string]any{"robotCode": robotCode, "msgKey": msgKey, "msgParam": string(encoded)}
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
		return nil, fmt.Errorf("send media failed: code=%s message=%s", resp.Code, resp.Message)
	}
	return &resp, nil
}

func (c *Client) UploadMedia(ctx context.Context, path, kind, robotCode, fileName string) (string, error) {
	info, err := os.Stat(path)
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() || info.Size() > maxMediaBytes {
		return "", fmt.Errorf("dingtalk media exceeds %d bytes or is not a regular file", maxMediaBytes)
	}
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	token := strings.TrimSpace(c.AccessToken)
	if token == "" {
		token, err = c.Token(ctx)
		if err != nil {
			return "", err
		}
	}
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	if err := writer.WriteField("robotCode", strings.TrimSpace(robotCode)); err != nil {
		return "", err
	}
	if err := writer.WriteField("type", strings.TrimSpace(kind)); err != nil {
		return "", err
	}
	uploadName := strings.TrimSpace(fileName)
	if uploadName == "" {
		uploadName = path
	}
	part, err := writer.CreateFormFile("file", filepath.Base(uploadName))
	if err != nil {
		return "", err
	}
	if _, err := io.Copy(part, io.LimitReader(file, maxMediaBytes+1)); err != nil {
		return "", err
	}
	if err := writer.Close(); err != nil {
		return "", err
	}
	var resp MediaUploadResponse
	if err := c.doMultipart(ctx, "/v1.0/robot/messageFiles/upload", &body, writer.FormDataContentType(), &resp, withAccessToken(token)); err != nil {
		return "", err
	}
	if resp.DownloadCode == "" {
		return "", fmt.Errorf("dingtalk media upload failed: code=%s message=%s", resp.Code, resp.Message)
	}
	return resp.DownloadCode, nil
}

func (c *Client) DownloadMedia(ctx context.Context, downloadCode, robotCode string) (string, func(), error) {
	if strings.TrimSpace(downloadCode) == "" {
		return "", nil, fmt.Errorf("download_code is required")
	}
	token := strings.TrimSpace(c.AccessToken)
	if token == "" {
		var err error
		token, err = c.Token(ctx)
		if err != nil {
			return "", nil, err
		}
	}
	var info MediaDownloadResponse
	if err := c.doJSON(ctx, http.MethodPost, "/v1.0/robot/messageFiles/download", map[string]any{"downloadCode": downloadCode, "robotCode": robotCode}, &info, withAccessToken(token)); err != nil {
		return "", nil, err
	}
	return c.DownloadURL(ctx, info.DownloadURL)
}

// DownloadURL materializes a DingTalk-provided media URL into a temporary file.
// The URL and every redirect are validated before a request is sent so inbound
// pictureUrl values cannot be used to reach local or private HTTP endpoints.
func (c *Client) DownloadURL(ctx context.Context, rawURL string) (string, func(), error) {
	parsed, err := validateMediaDownloadURL(rawURL)
	if err != nil {
		return "", nil, err
	}
	timeout := c.RequestTimeout
	if timeout <= 0 {
		timeout = defaultRequestTimeout
	}
	requestCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	request, err := http.NewRequestWithContext(requestCtx, http.MethodGet, parsed.String(), nil)
	if err != nil {
		return "", nil, err
	}
	client := mediaDownloadHTTPClient(c.HTTPClient, timeout)
	originalCheckRedirect := client.CheckRedirect
	client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		if _, err := validateMediaDownloadURL(req.URL.String()); err != nil {
			return err
		}
		if originalCheckRedirect != nil {
			return originalCheckRedirect(req, via)
		}
		if len(via) >= 10 {
			return fmt.Errorf("dingtalk media download stopped after 10 redirects")
		}
		return nil
	}
	response, err := client.Do(request)
	if err != nil {
		target := sanitizeURLForError(parsed.String())
		if isTimeoutError(err) {
			return "", nil, &transportError{message: fmt.Sprintf("GET %s timed out while downloading DingTalk media", target), cause: context.DeadlineExceeded}
		}
		var urlErr *neturl.Error
		if errors.As(err, &urlErr) {
			return "", nil, &transportError{message: fmt.Sprintf("GET %s failed: %v", target, urlErr.Err), cause: urlErr.Err}
		}
		return "", nil, &transportError{message: fmt.Sprintf("GET %s failed", target), cause: err}
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return "", nil, fmt.Errorf("dingtalk media download failed: status=%d", response.StatusCode)
	}
	tmp, err := os.CreateTemp("", "beak-dingtalk-media-*")
	if err != nil {
		return "", nil, err
	}
	path := tmp.Name()
	cleanup := func() { _ = os.Remove(path) }
	if _, err := io.Copy(tmp, io.LimitReader(response.Body, maxMediaBytes+1)); err != nil {
		_ = tmp.Close()
		cleanup()
		return "", nil, err
	}
	if info, statErr := tmp.Stat(); statErr != nil || info.Size() > maxMediaBytes {
		_ = tmp.Close()
		cleanup()
		if statErr != nil {
			return "", nil, statErr
		}
		return "", nil, fmt.Errorf("dingtalk media exceeds %d bytes", maxMediaBytes)
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return "", nil, err
	}
	return path, cleanup, nil
}

func validateMediaDownloadURL(rawURL string) (*neturl.URL, error) {
	rawURL = strings.TrimSpace(rawURL)
	if strings.HasPrefix(rawURL, "//") {
		rawURL = "https:" + rawURL
	}
	parsed, err := neturl.Parse(rawURL)
	if err != nil || parsed.Host == "" || parsed.Hostname() == "" || parsed.Opaque != "" || parsed.User != nil {
		return nil, fmt.Errorf("dingtalk media download URL is invalid")
	}
	parsed.Scheme = strings.ToLower(strings.TrimSpace(parsed.Scheme))
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return nil, fmt.Errorf("dingtalk media download URL scheme is not allowed")
	}
	if port := parsed.Port(); port != "" && !((parsed.Scheme == "http" && port == "80") || (parsed.Scheme == "https" && port == "443")) {
		return nil, fmt.Errorf("dingtalk media download URL port is not allowed")
	}
	if blockedMediaHost(parsed.Hostname()) {
		return nil, fmt.Errorf("dingtalk media download URL host is not allowed")
	}
	return parsed, nil
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
	if title = markdownTitleFromText(text); title != "" {
		return title
	}
	return "Beak"
}

func markdownTitleFromText(text string) string {
	for _, line := range strings.Split(text, "\n") {
		line = cleanMarkdownTitleLine(line)
		if line == "" {
			continue
		}
		return truncateRunes(line, 20)
	}
	return ""
}

func cleanMarkdownTitleLine(line string) string {
	line = strings.TrimSpace(line)
	for strings.HasPrefix(line, ">") {
		line = strings.TrimSpace(strings.TrimPrefix(line, ">"))
	}
	for strings.HasPrefix(line, "#") {
		line = strings.TrimSpace(strings.TrimPrefix(line, "#"))
	}
	for _, marker := range []string{"- ", "* ", "+ "} {
		if strings.HasPrefix(line, marker) {
			line = strings.TrimSpace(strings.TrimPrefix(line, marker))
			break
		}
	}
	return strings.Join(strings.Fields(line), " ")
}

func truncateRunes(text string, max int) string {
	if max <= 0 {
		return ""
	}
	runes := []rune(text)
	if len(runes) <= max {
		return text
	}
	return string(runes[:max])
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
		if isTimeoutError(err) {
			return &transportError{message: fmt.Sprintf("%s %s timed out while waiting for DingTalk API response", method, sanitizeURLForError(targetURL)), cause: context.DeadlineExceeded}
		}
		var urlErr *neturl.Error
		if errors.As(err, &urlErr) {
			return &transportError{message: fmt.Sprintf("%s %s failed: %v", method, sanitizeURLForError(targetURL), urlErr.Err), cause: urlErr.Err}
		}
		return &transportError{message: fmt.Sprintf("%s %s failed: %v", method, sanitizeURLForError(targetURL), err), cause: err}
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return &HTTPError{StatusCode: resp.StatusCode, Method: method, Target: sanitizeURLForError(targetURL), Body: string(data)}
	}
	if out == nil || len(bytes.TrimSpace(data)) == 0 {
		return nil
	}
	if err := json.Unmarshal(data, out); err != nil {
		return transientFailure(fmt.Sprintf("decode response: %v", err))
	}
	switch response := out.(type) {
	case *SendTextResponse:
		_ = json.Unmarshal(data, &response.Raw)
	case *WebhookSendResponse:
		_ = json.Unmarshal(data, &response.Raw)
	}
	return nil
}

func (c *Client) doMultipart(ctx context.Context, path string, body io.Reader, contentType string, out any, opts ...requestOption) error {
	timeout := c.RequestTimeout
	if timeout <= 0 {
		timeout = defaultRequestTimeout
	}
	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, c.url(path), body)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("User-Agent", "BeakAgentDingTalk/0.1.0")
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
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxMediaBytes))
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return &HTTPError{StatusCode: resp.StatusCode, Method: http.MethodPost, Target: sanitizeURLForError(req.URL.String()), Body: string(data)}
	}
	if out != nil && len(bytes.TrimSpace(data)) > 0 {
		if err := json.Unmarshal(data, out); err != nil {
			return transientFailure(fmt.Sprintf("decode response: %v", err))
		}
	}
	return nil
}

func isTimeoutError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
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
