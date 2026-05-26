package beakdingtalk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"beak-agent-dingtalk/internal/dingtalk"
	"beak-agent-dingtalk/sdk"
	beakstate "beak-agent-dingtalk/state"
)

var ErrCredentialLogin = errors.New("dingtalk connector uses credential login; create channel account from CredentialSchema")

type Connector struct {
	channel Channel
}

type EventResult struct {
	Type        string              `json:"type"`
	Ignored     bool                `json:"ignored,omitempty"`
	Reason      string              `json:"reason,omitempty"`
	SessionUUID string              `json:"session_uuid,omitempty"`
	MessageUUID string              `json:"message_uuid,omitempty"`
	Inbound     *sdk.InboundMessage `json:"inbound,omitempty"`
}

type EventConnector interface {
	sdk.Connector
	HandleEvent(ctx context.Context, runtime sdk.Runtime, account sdk.ChannelAccount, body []byte) (*EventResult, error)
}

func NewConnector() Connector {
	return Connector{channel: Channel{}}
}

func (c Connector) Metadata() sdk.ConnectorMetadata {
	meta := c.channel.Metadata()
	caps := c.channel.Capabilities()
	return sdk.ConnectorMetadata{
		ID:          meta.ID,
		Platform:    meta.Platform,
		Label:       meta.Label,
		Description: meta.Description,
		Capabilities: sdk.Capabilities{
			LoginModes:     []string{sdk.LoginModeCredential},
			Text:           caps.Text,
			Media:          caps.Media,
			GroupChat:      caps.GroupChat,
			DirectChat:     caps.DirectChat,
			BlockStreaming: caps.BlockStreaming,
		},
	}
}

func (c Connector) CredentialSchema(context.Context) sdk.CredentialSchema {
	schema := c.channel.SettingsSchema()
	properties := make(map[string]sdk.CredentialField, len(schema.Properties))
	for key, raw := range schema.Properties {
		item, _ := raw.(map[string]any)
		properties[key] = sdk.CredentialField{
			Type:        stringValue(item["type"]),
			Title:       stringValue(item["title"]),
			Description: stringValue(item["description"]),
			Secret:      boolValue(item["secret"]),
		}
	}
	return sdk.CredentialSchema{
		Type:                 schema.Type,
		LoginModes:           []string{sdk.LoginModeCredential},
		Properties:           properties,
		Required:             schema.Required,
		AdditionalProperties: false,
	}
}

func (Connector) StartLogin(context.Context, sdk.LoginStartRequest) (*sdk.LoginChallenge, error) {
	return nil, ErrCredentialLogin
}

func (Connector) PollLogin(context.Context, sdk.LoginPollRequest) (*sdk.LoginStatus, error) {
	return nil, ErrCredentialLogin
}

func (c Connector) Start(ctx context.Context, runtime sdk.Runtime) error {
	if runtime.Gateway == nil {
		return fmt.Errorf("dingtalk connector requires sdk.Runtime.Gateway")
	}
	if _, err := runtime.Gateway.EnsureChannel(ctx, sdk.EnsureChannelRequest{
		WorkspaceUUID: runtime.WorkspaceUUID,
		Platform:      Platform,
		Name:          "DingTalk",
		Config:        map[string]any{"bridge": ID},
	}); err != nil {
		return err
	}
	store := newConnectorStateStore(runtime.AccountStore)
	for _, account := range runtimeAccountCandidates(runtime) {
		store.seed(account)
		accountUUID := accountKey(account)
		if accountUUID == "" {
			return fmt.Errorf("dingtalk account_uuid or client_id is required")
		}
		sessionUUID, err := runtime.Gateway.EnsureChannelLinkSession(ctx, sdk.EnsureChannelLinkSessionRequest{
			WorkspaceUUID:       runtime.WorkspaceUUID,
			Platform:            Platform,
			AccountUUID:         accountUUID,
			AgentParticipantID:  runtime.Gateway.AgentParticipantID(),
			BridgeParticipantID: runtime.Gateway.BridgeParticipantID(Platform),
		})
		if err != nil {
			return err
		}
		state, err := store.LoadAccount(accountUUID)
		if err != nil {
			return err
		}
		if state.ChannelLinkSession != sessionUUID {
			state.ChannelLinkSession = sessionUUID
			if err := store.SaveAccount(state); err != nil {
				return err
			}
		}
	}
	<-ctx.Done()
	return ctx.Err()
}

func (c Connector) Send(ctx context.Context, runtime sdk.Runtime, req sdk.OutboundMessage) (*sdk.SendResult, error) {
	account, err := selectRuntimeAccount(runtime, req.AccountUUID)
	if err != nil {
		return nil, err
	}
	accountUUID := accountKey(account)
	store := newConnectorStateStore(runtime.AccountStore)
	store.seed(account)
	accountState, err := store.LoadAccount(accountUUID)
	if err != nil {
		return nil, err
	}
	client := clientFromAccount(runtime, account)
	now := time.Now().UTC()
	if accountState.AccessToken != "" && accountState.AccessTokenExpires.After(now.Add(5*time.Minute)) {
		client.AccessToken = accountState.AccessToken
		client.AccessTokenExpiresAt = accountState.AccessTokenExpires
	}
	if !boolValue(req.Raw["force_openapi"]) {
		if webhook, ok := accountState.SessionWebhooks[outboundStateKey(req)]; ok && webhook.URL != "" && webhook.ExpiresAt.After(now.Add(10*time.Second)) {
			resp, err := client.SendWebhookText(ctx, webhook.URL, req.Text)
			if err != nil {
				return nil, err
			}
			return &sdk.SendResult{
				Platform:    Platform,
				AccountUUID: accountUUID,
				Raw: map[string]any{
					"delivery_method": "session_webhook",
					"chat_type":       req.ChatType,
					"chat_id":         req.ChatID,
					"response":        resp.Raw,
				},
			}, nil
		}
	}
	if client.AccessToken == "" || !client.AccessTokenExpiresAt.After(now.Add(5*time.Minute)) {
		token, expiresAt, err := client.TokenWithExpiry(ctx, now)
		if err != nil {
			return nil, err
		}
		accountState.AccessToken = token
		accountState.AccessTokenExpires = expiresAt
		if len(account.State) > 0 {
			if err := store.SaveAccount(accountState); err != nil {
				return nil, err
			}
		}
	}
	resp, err := client.SendText(ctx, dingtalk.SendTextRequest{
		ChatType:    req.ChatType,
		ChatID:      strings.TrimSpace(req.ChatID),
		Text:        req.Text,
		RobotCode:   firstString(req.Raw["robot_code"], account.Credential["robot_code"], account.Credential["client_id"]),
		MessageUUID: req.MessageUUID,
	})
	if err != nil {
		return nil, err
	}
	return &sdk.SendResult{
		Platform:    Platform,
		AccountUUID: accountUUID,
		MessageID:   resp.ProcessQueryKey,
		Raw: map[string]any{
			"delivery_method":   "openapi",
			"process_query_key": resp.ProcessQueryKey,
			"chat_type":         req.ChatType,
			"chat_id":           req.ChatID,
			"response":          resp.Raw,
		},
	}, nil
}

func (Connector) Stop(context.Context, sdk.ChannelAccount) error {
	return nil
}

func (c Connector) HandleEvent(ctx context.Context, runtime sdk.Runtime, account sdk.ChannelAccount, body []byte) (*EventResult, error) {
	event, err := dingtalk.ParseStreamEvent(body)
	if err != nil {
		return nil, err
	}
	return c.processStreamEvent(ctx, runtime, account, event)
}

func (c Connector) processStreamEvent(ctx context.Context, runtime sdk.Runtime, account sdk.ChannelAccount, event *dingtalk.StreamEvent) (*EventResult, error) {
	if runtime.Gateway == nil {
		return nil, fmt.Errorf("dingtalk event handling requires sdk.Runtime.Gateway")
	}
	accountUUID := accountKey(account)
	if accountUUID == "" {
		return nil, fmt.Errorf("dingtalk account_uuid or client_id is required")
	}
	senderID := event.Sender()
	botUserID := firstString(account.Credential["chatbot_user_id"], account.State["chatbot_user_id"], event.ChatbotUserID)
	if botUserID != "" && senderID == botUserID {
		return &EventResult{Type: "message", Ignored: true, Reason: "self_echo"}, nil
	}
	text := event.Text()
	chat := event.ChatIdentity()
	if chat.ChatType == "" || chat.ChatID == "" || chat.SenderID == "" || text == "" {
		return &EventResult{Type: "message", Ignored: true, Reason: "incomplete_or_non_text_message"}, nil
	}

	store := newConnectorStateStore(runtime.AccountStore)
	store.seed(account)
	state, err := store.LoadAccount(accountUUID)
	if err != nil {
		return nil, err
	}
	key := event.DedupeKey(accountUUID)
	if _, ok := state.InboundSeen[key]; ok {
		return &EventResult{Type: "message", Ignored: true, Reason: "duplicate", SessionUUID: state.PeerSessions[chat.StateKey()]}, nil
	}

	sessionUUID, err := runtime.Gateway.EnsureChatSession(ctx, sdk.EnsureChatSessionRequest{
		WorkspaceUUID:       runtime.WorkspaceUUID,
		Platform:            Platform,
		AccountUUID:         accountUUID,
		ChatType:            chat.ChatType,
		ChatID:              chat.ChatID,
		SenderID:            chat.SenderID,
		AgentParticipantID:  runtime.Gateway.AgentParticipantID(),
		BridgeParticipantID: runtime.Gateway.BridgeParticipantID(Platform),
		Metadata: map[string]any{
			"source":       Platform,
			"account_uuid": accountUUID,
		},
	})
	if err != nil {
		return nil, err
	}
	inbound := BuildInboundMessage(runtime.WorkspaceUUID, runtime.Channel.UUID, accountUUID, event, text)
	messageUUID, err := runtime.Gateway.CreateMessage(ctx, sdk.CreateMessageRequest{
		WorkspaceUUID: runtime.WorkspaceUUID,
		SessionUUID:   sessionUUID,
		SenderID:      sdk.IMPersonParticipantID(Platform, chat.ChatType, chat.ChatID, chat.SenderID),
		Content:       text,
		Metadata: map[string]any{
			"source":                       Platform,
			"platform":                     Platform,
			"account_uuid":                 accountUUID,
			"dingtalk_account_id":          accountUUID,
			"dingtalk_client_id":           stringValue(account.Credential["client_id"]),
			"dingtalk_conversation_type":   event.ConversationType,
			"dingtalk_conversation_id":     event.ConversationID,
			"dingtalk_conversation_title":  event.ConversationTitle,
			"dingtalk_sender_id":           chat.SenderID,
			"dingtalk_sender_name":         event.SenderNick,
			"dingtalk_message_id":          event.MsgID,
			"dingtalk_delivery_message_id": event.DeliveryMessageID,
			"dingtalk_message_type":        event.MsgType,
			"dingtalk_session_webhook":     event.SessionWebhook,
			"dingtalk_chatbot_user_id":     event.ChatbotUserID,
			"dingtalk_chatbot_corp_id":     event.ChatbotCorpID,
			"inbound_message":              inbound,
		},
	})
	if err != nil {
		return nil, err
	}
	state.PeerSessions[chat.StateKey()] = sessionUUID
	state.InboundSeen[key] = time.Now().UTC().Format(time.RFC3339Nano)
	if event.ChatbotUserID != "" {
		state.ChatbotUserID = event.ChatbotUserID
	}
	if event.ChatbotCorpID != "" {
		state.ChatbotCorpID = event.ChatbotCorpID
	}
	if event.SessionWebhook != "" {
		state.SessionWebhooks[chat.StateKey()] = beakstate.Webhook{
			URL:       event.SessionWebhook,
			ExpiresAt: timeFromUnixMilli(event.SessionWebhookExpiredTime),
		}
	}
	if err := store.SaveAccount(state); err != nil {
		return nil, err
	}
	return &EventResult{Type: "message", SessionUUID: sessionUUID, MessageUUID: messageUUID, Inbound: &inbound}, nil
}

func BuildInboundMessage(workspaceUUID, channelUUID, accountUUID string, event *dingtalk.StreamEvent, text string) sdk.InboundMessage {
	chat := event.ChatIdentity()
	return sdk.InboundMessage{
		WorkspaceUUID: workspaceUUID,
		Platform:      Platform,
		AccountUUID:   accountUUID,
		ChannelUUID:   channelUUID,
		ChatType:      chat.ChatType,
		ChatID:        chat.ChatID,
		SenderID:      chat.SenderID,
		MessageID:     event.MsgID,
		Text:          text,
		DedupeKey:     event.DedupeKey(accountUUID),
		Raw: map[string]any{
			"conversation_type":          event.ConversationType,
			"conversation_id":            event.ConversationID,
			"conversation_title":         event.ConversationTitle,
			"sender_id":                  chat.SenderID,
			"sender_name":                event.SenderNick,
			"message_id":                 event.MsgID,
			"delivery_message_id":        event.DeliveryMessageID,
			"message_type":               event.MsgType,
			"robot_code":                 event.RobotCode,
			"session_webhook":            event.SessionWebhook,
			"session_webhook_expires_at": timeFromUnixMilli(event.SessionWebhookExpiredTime),
			"is_in_at_list":              event.IsInAtList,
			"chatbot_user_id":            event.ChatbotUserID,
			"chatbot_corp_id":            event.ChatbotCorpID,
		},
	}
}

func outboundStateKey(req sdk.OutboundMessage) string {
	if req.ChatType == sdk.ChatTypeGroup {
		return sdk.ChatTypeGroup + ":" + strings.TrimSpace(req.ChatID)
	}
	return strings.TrimSpace(req.ChatID)
}

func runtimeAccountCandidates(runtime sdk.Runtime) []sdk.ChannelAccount {
	seen := make(map[string]bool)
	var out []sdk.ChannelAccount
	add := func(account sdk.ChannelAccount) {
		key := accountKey(account)
		if key == "" || seen[key] {
			return
		}
		seen[key] = true
		out = append(out, account)
	}
	add(runtime.Account)
	for _, account := range runtime.Accounts {
		add(account)
	}
	return out
}

func selectRuntimeAccount(runtime sdk.Runtime, accountUUID string) (sdk.ChannelAccount, error) {
	accountUUID = strings.TrimSpace(accountUUID)
	candidates := runtimeAccountCandidates(runtime)
	if accountUUID != "" {
		for _, account := range candidates {
			if accountMatches(account, accountUUID) {
				return account, nil
			}
		}
		return sdk.ChannelAccount{}, fmt.Errorf("dingtalk account %s not found in runtime", accountUUID)
	}
	if len(candidates) == 1 {
		return candidates[0], nil
	}
	if len(candidates) == 0 {
		return sdk.ChannelAccount{}, fmt.Errorf("dingtalk outbound account is required")
	}
	return sdk.ChannelAccount{}, fmt.Errorf("dingtalk outbound account is ambiguous; account_uuid is required")
}

func accountMatches(account sdk.ChannelAccount, accountID string) bool {
	return strings.TrimSpace(account.UUID) == accountID ||
		strings.TrimSpace(stringValue(account.Credential["account_id"])) == accountID ||
		strings.TrimSpace(stringValue(account.Credential["client_id"])) == accountID ||
		strings.TrimSpace(stringValue(account.Credential["robot_code"])) == accountID
}

func accountKey(account sdk.ChannelAccount) string {
	return firstString(account.UUID, account.Credential["account_id"], account.Credential["client_id"])
}

func clientFromAccount(runtime sdk.Runtime, account sdk.ChannelAccount) *dingtalk.Client {
	client := dingtalk.NewClient(
		baseURLFromCredential(account.Credential),
		stringValue(account.Credential["client_id"]),
		stringValue(account.Credential["client_secret"]),
		firstString(account.Credential["robot_code"], account.Credential["client_id"]),
	)
	client.HTTPClient = runtime.HTTPClient
	return client
}

func baseURLFromCredential(credential map[string]any) string {
	if baseURL := strings.TrimSpace(stringValue(credential["base_url"])); baseURL != "" {
		return baseURL
	}
	return dingtalk.DefaultBaseURL
}

type connectorStateStore struct {
	mu           sync.Mutex
	accounts     map[string]*beakstate.AccountState
	sdkAccounts  map[string]sdk.ChannelAccount
	accountStore sdk.AccountStore
}

func newConnectorStateStore(accountStore sdk.AccountStore) *connectorStateStore {
	return &connectorStateStore{
		accounts:     make(map[string]*beakstate.AccountState),
		sdkAccounts:  make(map[string]sdk.ChannelAccount),
		accountStore: accountStore,
	}
}

func (s *connectorStateStore) seed(account sdk.ChannelAccount) {
	accountID := accountKey(account)
	if accountID == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	state := sdkAccountToState(account)
	s.accounts[accountID] = &state
	s.sdkAccounts[accountID] = account
}

func (s *connectorStateStore) LoadAccount(accountID string) (*beakstate.AccountState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if account, ok := s.accounts[accountID]; ok {
		return account, nil
	}
	account := &beakstate.AccountState{AccountID: accountID}
	account.EnsureMaps()
	s.accounts[accountID] = account
	return account, nil
}

func (s *connectorStateStore) SaveAccount(account *beakstate.AccountState) error {
	if err := beakstate.TouchAccount(account); err != nil {
		return err
	}
	s.mu.Lock()
	s.accounts[account.AccountID] = account
	existing := s.sdkAccounts[account.AccountID]
	sdkAccount := accountStateToSDK(*account, existing)
	s.sdkAccounts[account.AccountID] = sdkAccount
	accountStore := s.accountStore
	s.mu.Unlock()
	if accountStore != nil && sdkAccount.UUID != "" {
		return accountStore.SaveChannelAccountState(context.Background(), sdkAccount.UUID, sdkAccount.State)
	}
	return nil
}

func sdkAccountToState(account sdk.ChannelAccount) beakstate.AccountState {
	out := beakstate.AccountState{
		AccountID:          accountKey(account),
		ClientID:           stringValue(account.Credential["client_id"]),
		RobotCode:          firstString(account.Credential["robot_code"], account.Credential["client_id"]),
		BaseURL:            baseURLFromCredential(account.Credential),
		AccessToken:        stringValue(account.State["access_token"]),
		AccessTokenExpires: timeValue(account.State["access_token_expires_at"]),
		ChatbotUserID:      firstString(account.Credential["chatbot_user_id"], account.State["chatbot_user_id"]),
		ChatbotCorpID:      firstString(account.Credential["chatbot_corp_id"], account.State["chatbot_corp_id"]),
		ChannelLinkSession: stringValue(account.State["channel_link_session"]),
		PeerSessions:       stringMap(account.State["peer_sessions"]),
		SessionWebhooks:    webhookMap(account.State["session_webhooks"]),
		InboundSeen:        stringMap(account.State["inbound_seen"]),
		SentBeakMessages:   stringMap(account.State["sent_beak_messages"]),
		StreamCursors:      stringMap(account.State["stream_cursors"]),
	}
	out.EnsureMaps()
	return out
}

func accountStateToSDK(account beakstate.AccountState, existing sdk.ChannelAccount) sdk.ChannelAccount {
	if existing.UUID == "" {
		existing.UUID = account.AccountID
	}
	existing.Platform = Platform
	if existing.Credential == nil {
		existing.Credential = map[string]any{}
	}
	existing.State = map[string]any{
		"channel_link_session":    account.ChannelLinkSession,
		"peer_sessions":           account.PeerSessions,
		"session_webhooks":        account.SessionWebhooks,
		"inbound_seen":            account.InboundSeen,
		"sent_beak_messages":      account.SentBeakMessages,
		"stream_cursors":          account.StreamCursors,
		"access_token":            account.AccessToken,
		"access_token_expires_at": account.AccessTokenExpires,
		"chatbot_user_id":         account.ChatbotUserID,
		"chatbot_corp_id":         account.ChatbotCorpID,
		"updated_at":              account.UpdatedAt,
	}
	return existing
}

func webhookMap(value any) map[string]beakstate.Webhook {
	out := make(map[string]beakstate.Webhook)
	switch typed := value.(type) {
	case map[string]beakstate.Webhook:
		for key, item := range typed {
			out[key] = item
		}
	case map[string]any:
		for key, item := range typed {
			switch webhook := item.(type) {
			case beakstate.Webhook:
				out[key] = webhook
			case map[string]any:
				out[key] = beakstate.Webhook{
					URL:       stringValue(webhook["url"]),
					ExpiresAt: timeValue(webhook["expires_at"]),
				}
			case json.RawMessage:
				var parsed beakstate.Webhook
				if err := json.Unmarshal(webhook, &parsed); err == nil {
					out[key] = parsed
				}
			}
		}
	case json.RawMessage:
		_ = json.Unmarshal(typed, &out)
	}
	return out
}

func timeValue(value any) time.Time {
	switch typed := value.(type) {
	case time.Time:
		return typed
	case string:
		if typed == "" {
			return time.Time{}
		}
		parsed, _ := time.Parse(time.RFC3339Nano, typed)
		return parsed
	case json.RawMessage:
		var text string
		if err := json.Unmarshal(typed, &text); err == nil {
			return timeValue(text)
		}
	}
	return time.Time{}
}

func timeFromUnixMilli(ms int64) time.Time {
	if ms <= 0 {
		return time.Time{}
	}
	return time.UnixMilli(ms).UTC()
}

func stringMap(value any) map[string]string {
	out := make(map[string]string)
	switch typed := value.(type) {
	case map[string]string:
		for key, item := range typed {
			out[key] = item
		}
	case map[string]any:
		for key, item := range typed {
			if stringItem, ok := item.(string); ok {
				out[key] = stringItem
			}
		}
	case json.RawMessage:
		_ = json.Unmarshal(typed, &out)
	}
	return out
}

func stringValue(value any) string {
	if value == nil {
		return ""
	}
	switch typed := value.(type) {
	case string:
		return typed
	default:
		return fmt.Sprint(typed)
	}
}

func boolValue(value any) bool {
	switch typed := value.(type) {
	case bool:
		return typed
	case string:
		return strings.EqualFold(strings.TrimSpace(typed), "true")
	default:
		return false
	}
}

func firstString(values ...any) string {
	for _, value := range values {
		if stringValue := strings.TrimSpace(stringValue(value)); stringValue != "" {
			return stringValue
		}
	}
	return ""
}

var _ sdk.Connector = Connector{}
var _ EventConnector = Connector{}
