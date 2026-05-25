package beakdingtalk

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"beak-agent-dingtalk/sdk"
)

func TestDingTalkScenarioMultipleAccountsShareGroupButUseSeparateSessions(t *testing.T) {
	connector := NewConnector()
	eventConnector, ok := any(connector).(EventConnector)
	if !ok {
		t.Fatal("connector should implement EventConnector")
	}
	gateway := newScenarioGateway(Platform)
	store := newScenarioStore()
	accountA := scenarioDingTalkAccount("account-a", "client-a", "secret-a", "robot-a", "bot-a")
	accountB := scenarioDingTalkAccount("account-b", "client-b", "secret-b", "robot-b", "bot-b")
	runtime := sdk.Runtime{
		WorkspaceUUID: "workspace-1",
		Channel:       sdk.Channel{UUID: "channel-1", WorkspaceUUID: "workspace-1", Platform: Platform},
		Accounts:      []sdk.ChannelAccount{accountA, accountB},
		Gateway:       gateway,
		AccountStore:  store,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	err := connector.Start(ctx, runtime)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Start error=%v", err)
	}
	if gateway.linkSession("account-a") == "" || gateway.linkSession("account-b") == "" {
		t.Fatalf("missing channel link sessions: %+v", gateway.linkSessions)
	}

	resultA1, err := eventConnector.HandleEvent(context.Background(), runtime, accountA, []byte(`{
		"conversationType":"2",
		"conversationId":"cid-team",
		"conversationTitle":"Team",
		"senderStaffId":"staff-a",
		"senderNick":"Alice",
		"msgId":"msg-a-1",
		"msgtype":"text",
		"text":{"content":"hello from bot a group"}
	}`))
	if err != nil {
		t.Fatal(err)
	}
	accountA.State = store.state("account-a")
	runtime.Accounts[0] = accountA

	resultA2, err := eventConnector.HandleEvent(context.Background(), runtime, accountA, []byte(`{
		"conversationType":"2",
		"conversationId":"cid-team",
		"conversationTitle":"Team",
		"senderStaffId":"staff-b",
		"senderNick":"Bob",
		"msgId":"msg-a-2",
		"msgtype":"text",
		"text":{"content":"second group message"}
	}`))
	if err != nil {
		t.Fatal(err)
	}
	if resultA1.SessionUUID == "" || resultA1.SessionUUID != resultA2.SessionUUID {
		t.Fatalf("same account and group should reuse session: first=%+v second=%+v", resultA1, resultA2)
	}

	resultB, err := eventConnector.HandleEvent(context.Background(), runtime, accountB, []byte(`{
		"conversationType":"2",
		"conversationId":"cid-team",
		"conversationTitle":"Team",
		"senderStaffId":"staff-c",
		"senderNick":"Carol",
		"msgId":"msg-b-1",
		"msgtype":"text",
		"text":{"content":"hello from bot b group"}
	}`))
	if err != nil {
		t.Fatal(err)
	}
	if resultB.SessionUUID == "" || resultB.SessionUUID == resultA1.SessionUUID {
		t.Fatalf("different accounts in same group need separate sessions: accountA=%s accountB=%s", resultA1.SessionUUID, resultB.SessionUUID)
	}

	accountA.State = store.state("account-a")
	runtime.Accounts[0] = accountA
	duplicate, err := eventConnector.HandleEvent(context.Background(), runtime, accountA, []byte(`{
		"conversationType":"2",
		"conversationId":"cid-team",
		"senderStaffId":"staff-a",
		"msgId":"msg-a-1",
		"msgtype":"text",
		"text":{"content":"hello from bot a group"}
	}`))
	if err != nil {
		t.Fatal(err)
	}
	if !duplicate.Ignored || duplicate.Reason != "duplicate" || duplicate.SessionUUID != resultA1.SessionUUID {
		t.Fatalf("duplicate should be ignored with original session: %+v", duplicate)
	}

	selfEcho, err := eventConnector.HandleEvent(context.Background(), runtime, accountA, []byte(`{
		"conversationType":"2",
		"conversationId":"cid-team",
		"senderStaffId":"bot-a",
		"msgId":"msg-self",
		"msgtype":"text",
		"text":{"content":"self echo"}
	}`))
	if err != nil {
		t.Fatal(err)
	}
	if !selfEcho.Ignored || selfEcho.Reason != "self_echo" {
		t.Fatalf("self echo should be ignored: %+v", selfEcho)
	}

	gateway.mu.Lock()
	defer gateway.mu.Unlock()
	if len(gateway.messages) != 3 {
		t.Fatalf("expected three non-duplicate user messages, got %+v", gateway.messages)
	}
	if gateway.messages[0].SenderID != "im:dingtalk:group:cid-team:user:staff-a" {
		t.Fatalf("sender participant=%q", gateway.messages[0].SenderID)
	}
}

func TestDingTalkScenarioOutboundGroupAndDirectRequests(t *testing.T) {
	var sentPaths []string
	httpClient := &http.Client{Transport: scenarioRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		sentPaths = append(sentPaths, r.URL.Path)
		switch r.URL.Path {
		case "/v1.0/oauth2/accessToken":
			var body map[string]string
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			if body["appKey"] != "client-b" || body["appSecret"] != "secret-b" {
				t.Fatalf("token body=%+v", body)
			}
			return scenarioJSONResponse(map[string]any{"accessToken": "token-b", "expireIn": 7200})
		case "/v1.0/robot/groupMessages/send":
			if got := r.Header.Get("x-acs-dingtalk-access-token"); got != "token-b" {
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
			if body.RobotCode != "robot-b" || body.OpenConversationID != "cid-team" || body.MsgKey != "sampleText" {
				t.Fatalf("group body=%+v", body)
			}
			assertDingTalkTextParam(t, body.MsgParam, "group reply")
			return scenarioJSONResponse(map[string]any{"processQueryKey": "pqk-group"})
		case "/v1.0/robot/oToMessages/batchSend":
			var body struct {
				RobotCode string   `json:"robotCode"`
				UserIDs   []string `json:"userIds"`
				MsgKey    string   `json:"msgKey"`
				MsgParam  string   `json:"msgParam"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			if body.RobotCode != "robot-b" || len(body.UserIDs) != 1 || body.UserIDs[0] != "staff-direct" || body.MsgKey != "sampleText" {
				t.Fatalf("direct body=%+v", body)
			}
			assertDingTalkTextParam(t, body.MsgParam, "direct reply")
			return scenarioJSONResponse(map[string]any{"processQueryKey": "pqk-direct"})
		default:
			t.Fatalf("unexpected request path: %s", r.URL.Path)
		}
		return nil, nil
	})}

	connector := NewConnector()
	runtime := sdk.Runtime{
		HTTPClient: httpClient,
		Accounts: []sdk.ChannelAccount{
			scenarioDingTalkAccount("account-a", "client-a", "secret-a", "robot-a", "bot-a"),
			scenarioDingTalkAccount("account-b", "client-b", "secret-b", "robot-b", "bot-b"),
		},
	}

	groupResult, err := connector.Send(context.Background(), runtime, sdk.OutboundMessage{
		AccountUUID: "account-b",
		ChatType:    sdk.ChatTypeGroup,
		ChatID:      "cid-team",
		Text:        "group reply",
		MessageUUID: "message-group",
	})
	if err != nil {
		t.Fatal(err)
	}
	if groupResult.MessageID != "pqk-group" || groupResult.AccountUUID != "account-b" {
		t.Fatalf("group result=%+v", groupResult)
	}

	directResult, err := connector.Send(context.Background(), runtime, sdk.OutboundMessage{
		AccountUUID: "account-b",
		ChatType:    sdk.ChatTypeDirect,
		ChatID:      "staff-direct",
		Text:        "direct reply",
		MessageUUID: "message-direct",
	})
	if err != nil {
		t.Fatal(err)
	}
	if directResult.MessageID != "pqk-direct" || directResult.AccountUUID != "account-b" {
		t.Fatalf("direct result=%+v", directResult)
	}
	if strings.Join(sentPaths, ",") != "/v1.0/oauth2/accessToken,/v1.0/robot/groupMessages/send,/v1.0/oauth2/accessToken,/v1.0/robot/oToMessages/batchSend" {
		t.Fatalf("request paths=%+v", sentPaths)
	}
}

func TestDingTalkScenarioCredentialInboundAndFixedReply(t *testing.T) {
	const fixedReply = "Beak Agent 已收到你的钉钉消息"
	connector := NewConnector()
	eventConnector, ok := any(connector).(EventConnector)
	if !ok {
		t.Fatal("connector should implement EventConnector")
	}
	gateway := newScenarioGateway(Platform)
	store := newScenarioStore()
	account := scenarioDingTalkAccount("account-fixed", "client-fixed", "secret-fixed", "robot-fixed", "bot-fixed")

	var sent scenarioDingTalkSentMessage
	httpClient := &http.Client{Transport: scenarioRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/v1.0/oauth2/accessToken":
			var body map[string]string
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			if body["appKey"] != "client-fixed" || body["appSecret"] != "secret-fixed" {
				t.Fatalf("token body=%+v", body)
			}
			return scenarioJSONResponse(map[string]any{"accessToken": "token-fixed", "expireIn": 7200})
		case "/v1.0/robot/groupMessages/send":
			if got := r.Header.Get("x-acs-dingtalk-access-token"); got != "token-fixed" {
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
			var param map[string]string
			if err := json.Unmarshal([]byte(body.MsgParam), &param); err != nil {
				t.Fatal(err)
			}
			sent = scenarioDingTalkSentMessage{
				robotCode:      body.RobotCode,
				conversationID: body.OpenConversationID,
				msgKey:         body.MsgKey,
				text:           param["content"],
			}
			return scenarioJSONResponse(map[string]any{"processQueryKey": "pqk-fixed"})
		default:
			t.Fatalf("unexpected request path: %s", r.URL.Path)
		}
		return nil, nil
	})}

	runtime := sdk.Runtime{
		WorkspaceUUID: "workspace-1",
		Channel:       sdk.Channel{UUID: "channel-1", WorkspaceUUID: "workspace-1", Platform: Platform},
		Account:       account,
		Gateway:       gateway,
		AccountStore:  store,
		HTTPClient:    httpClient,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	err := connector.Start(ctx, runtime)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Start error=%v", err)
	}

	result, err := eventConnector.HandleEvent(context.Background(), runtime, account, []byte(`{
		"conversationType":"2",
		"conversationId":"cid-fixed",
		"conversationTitle":"Fixed Reply Group",
		"senderStaffId":"staff-fixed",
		"senderNick":"Alice",
		"msgId":"msg-fixed-1",
		"msgtype":"text",
		"text":{"content":"你好 Beak"},
		"chatbotUserId":"bot-fixed",
		"chatbotCorpId":"corp-fixed"
	}`))
	if err != nil {
		t.Fatal(err)
	}
	if result.Ignored || result.SessionUUID == "" || result.MessageUUID == "" {
		t.Fatalf("result=%+v", result)
	}
	if result.Inbound == nil || result.Inbound.ChatType != sdk.ChatTypeGroup || result.Inbound.ChatID != "cid-fixed" || result.Inbound.Text != "你好 Beak" {
		t.Fatalf("inbound=%+v", result.Inbound)
	}

	account.State = store.state("account-fixed")
	sendResult, err := connector.Send(context.Background(), runtime, sdk.OutboundMessage{
		AccountUUID: "account-fixed",
		ChatType:    result.Inbound.ChatType,
		ChatID:      result.Inbound.ChatID,
		Text:        fixedReply,
		MessageUUID: "agent-message-fixed",
	})
	if err != nil {
		t.Fatal(err)
	}
	if sendResult.MessageID != "pqk-fixed" || sendResult.AccountUUID != "account-fixed" {
		t.Fatalf("send result=%+v", sendResult)
	}
	if sent.robotCode != "robot-fixed" || sent.conversationID != "cid-fixed" || sent.msgKey != "sampleText" || sent.text != fixedReply {
		t.Fatalf("sent=%+v", sent)
	}

	gateway.mu.Lock()
	createdMessages := append([]sdk.CreateMessageRequest(nil), gateway.messages...)
	chatRequests := append([]sdk.EnsureChatSessionRequest(nil), gateway.chatRequests...)
	gateway.mu.Unlock()
	if len(createdMessages) != 1 || createdMessages[0].Content != "你好 Beak" || createdMessages[0].SenderID != "im:dingtalk:group:cid-fixed:user:staff-fixed" {
		t.Fatalf("created messages=%+v", createdMessages)
	}
	if len(chatRequests) != 1 || chatRequests[0].AccountUUID != "account-fixed" || chatRequests[0].ChatType != sdk.ChatTypeGroup || chatRequests[0].ChatID != "cid-fixed" {
		t.Fatalf("chat requests=%+v", chatRequests)
	}
	state := store.state("account-fixed")
	peerSessions, ok := state["peer_sessions"].(map[string]string)
	if !ok || peerSessions["group:cid-fixed"] != result.SessionUUID {
		t.Fatalf("peer sessions=%+v", state["peer_sessions"])
	}
	inboundSeen, ok := state["inbound_seen"].(map[string]string)
	if !ok || inboundSeen["account-fixed:message:msg-fixed-1"] == "" {
		t.Fatalf("inbound seen=%+v", state["inbound_seen"])
	}
}

func scenarioDingTalkAccount(uuid, clientID, secret, robotCode, botUserID string) sdk.ChannelAccount {
	return sdk.ChannelAccount{
		UUID:          uuid,
		WorkspaceUUID: "workspace-1",
		ChannelUUID:   "channel-1",
		Platform:      Platform,
		Credential: map[string]any{
			"account_id":      uuid,
			"client_id":       clientID,
			"client_secret":   secret,
			"robot_code":      robotCode,
			"base_url":        "https://api.dingtalk.test",
			"chatbot_user_id": botUserID,
		},
		State: map[string]any{},
	}
}

type scenarioDingTalkSentMessage struct {
	robotCode      string
	conversationID string
	msgKey         string
	text           string
}

func assertDingTalkTextParam(t *testing.T, raw string, want string) {
	t.Helper()
	var param map[string]string
	if err := json.Unmarshal([]byte(raw), &param); err != nil {
		t.Fatal(err)
	}
	if param["content"] != want {
		t.Fatalf("msgParam=%+v want content=%q", param, want)
	}
}

type scenarioGateway struct {
	mu              sync.Mutex
	platform        string
	linkSessions    map[string]string
	chatSessions    map[string]string
	chatRequests    []sdk.EnsureChatSessionRequest
	messages        []sdk.CreateMessageRequest
	nextChatSession int
	nextMessage     int
}

func newScenarioGateway(platform string) *scenarioGateway {
	return &scenarioGateway{
		platform:     platform,
		linkSessions: make(map[string]string),
		chatSessions: make(map[string]string),
	}
}

func (g *scenarioGateway) EnsureChannel(ctx context.Context, req sdk.EnsureChannelRequest) (string, error) {
	if req.Platform != g.platform {
		return "", errors.New("unexpected platform")
	}
	return "channel-1", nil
}

func (g *scenarioGateway) EnsureChannelLinkSession(ctx context.Context, req sdk.EnsureChannelLinkSessionRequest) (string, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.linkSessions[req.AccountUUID] == "" {
		g.linkSessions[req.AccountUUID] = "link-" + req.AccountUUID
	}
	return g.linkSessions[req.AccountUUID], nil
}

func (g *scenarioGateway) EnsureChatSession(ctx context.Context, req sdk.EnsureChatSessionRequest) (string, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.chatRequests = append(g.chatRequests, req)
	key := req.AccountUUID + ":" + req.ChatType + ":" + req.ChatID
	if g.chatSessions[key] == "" {
		g.nextChatSession++
		g.chatSessions[key] = "session-" + req.AccountUUID + "-" + req.ChatType + "-" + req.ChatID
	}
	return g.chatSessions[key], nil
}

func (g *scenarioGateway) CreateMessage(ctx context.Context, req sdk.CreateMessageRequest) (string, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.nextMessage++
	g.messages = append(g.messages, req)
	return "message-scenario-" + req.SessionUUID, nil
}

func (g *scenarioGateway) StreamSession(ctx context.Context, req sdk.StreamSessionRequest, handle func(sdk.StreamEvent) error) error {
	return nil
}

func (g *scenarioGateway) AgentParticipantID() string {
	return "agent:agent-1"
}

func (g *scenarioGateway) BridgeParticipantID(platform string) string {
	return sdk.BridgeParticipantID(platform)
}

func (g *scenarioGateway) linkSession(accountUUID string) string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.linkSessions[accountUUID]
}

type scenarioStore struct {
	mu     sync.Mutex
	states map[string]map[string]any
}

func newScenarioStore() *scenarioStore {
	return &scenarioStore{states: make(map[string]map[string]any)}
}

func (s *scenarioStore) SaveChannelAccountState(ctx context.Context, accountUUID string, state map[string]any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	copied := make(map[string]any, len(state))
	for key, value := range state {
		copied[key] = value
	}
	s.states[accountUUID] = copied
	return nil
}

func (s *scenarioStore) state(accountUUID string) map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	copied := make(map[string]any, len(s.states[accountUUID]))
	for key, value := range s.states[accountUUID] {
		copied[key] = value
	}
	return copied
}

type scenarioRoundTripFunc func(*http.Request) (*http.Response, error)

func (f scenarioRoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func scenarioJSONResponse(body any) (*http.Response, error) {
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
