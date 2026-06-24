package beakdingtalk

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/GuanceCloud/beak-agent-channel-dingtalk/internal/dingtalk"
	"github.com/GuanceCloud/beak-agent-channel-dingtalk/sdk"
	beakstate "github.com/GuanceCloud/beak-agent-channel-dingtalk/state"
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

type WebhookRequestConnector interface {
	sdk.Connector
	HandleWebhookRequest(ctx context.Context, runtime sdk.Runtime, account sdk.ChannelAccount, req *http.Request) (*sdk.WebhookResponse, error)
}

func NewConnector() sdk.Connector {
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
			LoginModes:       []string{sdk.LoginModeCredential},
			Text:             caps.Text,
			Media:            caps.Media,
			GroupChat:        caps.GroupChat,
			DirectChat:       caps.DirectChat,
			Stream:           true,
			Webhook:          false,
			BlockStreaming:   caps.BlockStreaming,
			AckModes:         nil,
			RuntimeOwnership: sdk.RuntimeOwnershipHostStream,
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

func (Connector) ValidateCredential(ctx context.Context, req sdk.CredentialValidationRequest) (*sdk.CredentialValidationResult, error) {
	credential := cloneMap(req.Credential)
	state := cloneMap(req.State)
	client := dingtalk.NewClient(
		baseURLFromCredential(credential),
		stringValue(credential["client_id"]),
		stringValue(credential["client_secret"]),
		firstString(credential["robot_code"], credential["client_id"]),
	)
	client.HTTPClient = req.Runtime.HTTPClient

	now := time.Now().UTC()
	token, expiresAt, err := client.TokenWithExpiry(ctx, now)
	if err != nil {
		return credentialValidationFailure(credential, state, err), nil
	}

	robotCode := firstString(credential["robot_code"], credential["client_id"])
	if strings.TrimSpace(robotCode) != "" {
		credential["robot_code"] = robotCode
		state["robot_code"] = robotCode
		if identities := dingtalkBotIdentityState(beakstate.AccountState{RobotCode: robotCode}); len(identities) > 0 {
			state["bot_identities"] = identities
			state["bot_identity"] = identities[0]
		}
	}
	accountKey := firstString(credential["account_id"], robotCode, credential["client_id"])
	if accountKey != "" {
		credential["account_id"] = accountKey
	}
	state["access_token"] = token
	state["access_token_expires_at"] = expiresAt

	return &sdk.CredentialValidationResult{
		Valid:       true,
		AccountKey:  accountKey,
		DisplayName: firstString(credential["display_name"], robotCode, credential["client_id"]),
		Credential:  credential,
		State:       state,
		Metadata: map[string]any{
			"platform":   Platform,
			"robot_code": robotCode,
		},
	}, nil
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
		state, err := store.LoadAccount(ctx, accountUUID)
		if err != nil {
			return err
		}
		if state.ChannelLinkSession != sessionUUID {
			state.ChannelLinkSession = sessionUUID
			if err := store.SaveAccount(ctx, state); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c Connector) Send(ctx context.Context, runtime sdk.Runtime, req sdk.OutboundMessage) (*sdk.SendResult, error) {
	account, err := selectRuntimeAccount(runtime, req.AccountUUID)
	if err != nil {
		return nil, err
	}
	accountUUID := accountKey(account)
	store := newConnectorStateStore(runtime.AccountStore)
	store.seed(account)
	accountState, err := store.LoadAccount(ctx, accountUUID)
	if err != nil {
		return nil, err
	}
	client := clientFromAccount(runtime, account)
	now := time.Now().UTC()
	if accountState.AccessToken != "" && accountState.AccessTokenExpires.After(now.Add(5*time.Minute)) {
		client.AccessToken = accountState.AccessToken
		client.AccessTokenExpiresAt = accountState.AccessTokenExpires
	}
	at := dingtalkOutboundAtOptions(req)
	format := dingtalkOutboundFormat(req)
	if !boolValue(req.Raw["force_openapi"]) {
		if webhook, ok := accountState.SessionWebhooks[outboundStateKey(req)]; ok && dingtalk.IsAllowedSessionWebhookURL(webhook.URL) && webhook.ExpiresAt.After(now.Add(10*time.Second)) {
			var resp *dingtalk.WebhookSendResponse
			var err error
			if format == "markdown" {
				resp, err = client.SendWebhookMarkdownMessage(ctx, webhook.URL, dingtalk.SendWebhookMarkdownRequest{
					Title: dingtalkOutboundTitle(req),
					Text:  req.Text,
					At:    at,
				})
			} else {
				resp, err = client.SendWebhookTextMessage(ctx, webhook.URL, dingtalk.SendWebhookTextRequest{
					Text: req.Text,
					At:   at,
				})
			}
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
					"msg_type":        format,
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
		if err := store.SaveAccount(ctx, accountState); err != nil {
			return nil, err
		}
	}
	robotCode := firstString(req.Raw["robot_code"], account.Credential["robot_code"], account.Credential["client_id"])
	var resp *dingtalk.SendTextResponse
	var sendErr error
	if format == "markdown" {
		resp, sendErr = client.SendMarkdown(ctx, dingtalk.SendMarkdownRequest{
			ChatType:    req.ChatType,
			ChatID:      strings.TrimSpace(req.ChatID),
			Title:       dingtalkOutboundTitle(req),
			Text:        req.Text,
			RobotCode:   robotCode,
			MessageUUID: req.MessageUUID,
			At:          at,
		})
	} else {
		resp, sendErr = client.SendText(ctx, dingtalk.SendTextRequest{
			ChatType:    req.ChatType,
			ChatID:      strings.TrimSpace(req.ChatID),
			Text:        req.Text,
			RobotCode:   robotCode,
			MessageUUID: req.MessageUUID,
			At:          at,
		})
	}
	if sendErr != nil {
		return nil, sendErr
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
			"msg_type":          format,
			"response":          resp.Raw,
		},
	}, nil
}

func (c Connector) Acknowledge(ctx context.Context, runtime sdk.Runtime, req sdk.OutboundAck) (*sdk.AckResult, error) {
	account, err := selectRuntimeAccount(runtime, req.AccountUUID)
	if err != nil {
		return nil, err
	}
	return &sdk.AckResult{
		Platform:    Platform,
		AccountUUID: accountKey(account),
		Mode:        "unsupported",
		Status:      "unsupported",
		Raw: map[string]any{
			"reason": "dingtalk_visible_ack_unsupported",
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

func (c Connector) HandleWebhookRequest(ctx context.Context, runtime sdk.Runtime, account sdk.ChannelAccount, req *http.Request) (*sdk.WebhookResponse, error) {
	if req == nil || req.Body == nil {
		return nil, fmt.Errorf("dingtalk webhook request body is required")
	}
	if err := verifyDingTalkWebhookRequest(account, req, time.Now().UTC()); err != nil {
		return nil, err
	}
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, err
	}
	if _, err := c.HandleEvent(ctx, runtime, account, body); err != nil {
		return nil, err
	}
	return &sdk.WebhookResponse{StatusCode: http.StatusOK}, nil
}

func (c Connector) processStreamEvent(ctx context.Context, runtime sdk.Runtime, account sdk.ChannelAccount, event *dingtalk.StreamEvent) (*EventResult, error) {
	if runtime.Gateway == nil {
		return nil, fmt.Errorf("dingtalk event handling requires sdk.Runtime.Gateway")
	}
	if !dingtalkEventOwnershipValid(account, event) {
		return &EventResult{Type: "message", Ignored: true, Reason: "robot_code_mismatch"}, nil
	}
	accountUUID := accountKey(account)
	if accountUUID == "" {
		return nil, fmt.Errorf("dingtalk account_uuid or client_id is required")
	}
	store := newConnectorStateStore(runtime.AccountStore)
	store.seed(account)
	state, err := store.LoadAccount(ctx, accountUUID)
	if err != nil {
		return nil, err
	}
	senderID := event.Sender()
	botUserID := firstString(state.ChatbotUserID, account.State["chatbot_user_id"], standardBotIdentityValue(account.State, "chatbot_user_id"), account.Credential["chatbot_user_id"], event.ChatbotUserID)
	if botUserID != "" && (senderID == botUserID || event.SenderID == botUserID || event.SenderStaffID == botUserID) {
		return &EventResult{Type: "message", Ignored: true, Reason: "self_echo"}, nil
	}
	text := event.Text()
	chat := event.ChatIdentity()
	inbound := BuildInboundMessage(runtime.WorkspaceUUID, runtime.Channel.UUID, accountUUID, event, text)
	if chat.ChatType == "" || chat.ChatID == "" || chat.SenderID == "" {
		return &EventResult{Type: "message", Ignored: true, Reason: "incomplete_or_non_text_message"}, nil
	}
	if strings.TrimSpace(text) == "" && !inbound.MentionedMe {
		return &EventResult{Type: "message", Ignored: true, Reason: "incomplete_or_non_text_message"}, nil
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
		ChatDisplayName:     chat.DisplayName,
		ChatIdentity:        dingtalkSDKChatIdentity(chat),
		SenderID:            chat.SenderID,
		AgentParticipantID:  runtime.Gateway.AgentParticipantID(),
		BridgeParticipantID: runtime.Gateway.BridgeParticipantID(Platform),
		Metadata: map[string]any{
			"source":            Platform,
			"account_uuid":      accountUUID,
			"chat_display_name": chat.DisplayName,
			"chat_identity":     dingtalkSDKChatIdentity(chat),
		},
	})
	if err != nil {
		return nil, err
	}
	messageUUID, err := runtime.Gateway.CreateMessage(ctx, sdk.CreateMessageRequest{
		WorkspaceUUID: runtime.WorkspaceUUID,
		SessionUUID:   sessionUUID,
		SenderID:      sdk.IMPersonParticipantID(Platform, chat.ChatType, chat.ChatID, chat.SenderID),
		Content:       text,
		DedupeKey:     key,
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
			"chat_display_name":            strings.TrimSpace(chat.DisplayName),
			"chat_identity":                dingtalkSDKChatIdentity(chat),
			"sender_display_name":          strings.TrimSpace(event.SenderNick),
			"dingtalk_message_id":          event.MsgID,
			"dingtalk_delivery_message_id": event.DeliveryMessageID,
			"dingtalk_message_type":        event.MsgType,
			"dingtalk_has_session_webhook": dingtalk.IsAllowedSessionWebhookURL(event.SessionWebhook),
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
	if event.RobotCode != "" {
		state.RobotCode = event.RobotCode
	}
	if dingtalk.IsAllowedSessionWebhookURL(event.SessionWebhook) {
		state.SessionWebhooks[chat.StateKey()] = beakstate.Webhook{
			URL:       event.SessionWebhook,
			ExpiresAt: timeFromUnixMilli(event.SessionWebhookExpiredTime),
		}
	}
	now := time.Now().UTC()
	state.StreamLastEventAt = now
	state.StreamLastActivityAt = now
	if err := store.SaveAccount(ctx, state); err != nil {
		return nil, err
	}
	return &EventResult{Type: "message", SessionUUID: sessionUUID, MessageUUID: messageUUID, Inbound: &inbound}, nil
}

func verifyDingTalkWebhookRequest(account sdk.ChannelAccount, req *http.Request, now time.Time) error {
	timestamp := strings.TrimSpace(req.Header.Get("timestamp"))
	signature := strings.TrimSpace(req.Header.Get("sign"))
	if timestamp == "" || signature == "" {
		return fmt.Errorf("dingtalk webhook signature headers are required")
	}
	secret := strings.TrimSpace(stringValue(account.Credential["client_secret"]))
	if secret == "" {
		return fmt.Errorf("dingtalk webhook signature verification requires client_secret")
	}
	millis, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil {
		return fmt.Errorf("dingtalk webhook timestamp is invalid")
	}
	sentAt := time.UnixMilli(millis)
	age := now.Sub(sentAt)
	if age < 0 {
		age = -age
	}
	if age > time.Hour {
		return fmt.Errorf("dingtalk webhook timestamp is expired")
	}
	expected := dingtalkWebhookSignature(timestamp, secret)
	if unescaped, err := url.PathUnescape(signature); err == nil {
		signature = unescaped
	}
	if !hmac.Equal([]byte(signature), []byte(expected)) {
		return fmt.Errorf("dingtalk webhook signature mismatch")
	}
	return nil
}

func dingtalkWebhookSignature(timestamp, secret string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(timestamp + "\n" + secret))
	return base64.StdEncoding.EncodeToString(mac.Sum(nil))
}

func BuildInboundMessage(workspaceUUID, channelUUID, accountUUID string, event *dingtalk.StreamEvent, text string) sdk.InboundMessage {
	chat := event.ChatIdentity()
	mentions := dingtalkMentionIdentities(event)
	return sdk.InboundMessage{
		WorkspaceUUID:     workspaceUUID,
		Platform:          Platform,
		AccountUUID:       accountUUID,
		ChannelUUID:       channelUUID,
		ChatType:          chat.ChatType,
		ChatID:            chat.ChatID,
		ChatDisplayName:   chat.DisplayName,
		ChatIdentity:      dingtalkSDKChatIdentity(chat),
		SenderID:          chat.SenderID,
		SenderDisplayName: event.SenderNick,
		MessageID:         event.MsgID,
		Text:              text,
		DedupeKey:         event.DedupeKey(accountUUID),
		Mentions:          mentions,
		MentionedMe:       event.IsInAtList,
		MentionAll:        event.IsAtAll,
		Raw: map[string]any{
			"conversation_type":          event.ConversationType,
			"conversation_id":            event.ConversationID,
			"conversation_title":         event.ConversationTitle,
			"chat_display_name":          chat.DisplayName,
			"chat_identity":              dingtalkSDKChatIdentity(chat),
			"sender_id":                  chat.SenderID,
			"sender_name":                event.SenderNick,
			"sender_display_name":        strings.TrimSpace(event.SenderNick),
			"message_id":                 event.MsgID,
			"delivery_message_id":        event.DeliveryMessageID,
			"message_type":               event.MsgType,
			"robot_code":                 event.RobotCode,
			"has_session_webhook":        dingtalk.IsAllowedSessionWebhookURL(event.SessionWebhook),
			"session_webhook_expires_at": timeFromUnixMilli(event.SessionWebhookExpiredTime),
			"is_in_at_list":              event.IsInAtList,
			"is_at_all":                  event.IsAtAll,
			"mention_all":                event.IsAtAll,
			"at_user_ids":                event.AtUserIDs,
			"at_dingtalk_ids":            event.AtDingtalkIDs,
			"at_mobiles":                 event.AtMobiles,
			"chatbot_user_id":            event.ChatbotUserID,
			"chatbot_corp_id":            event.ChatbotCorpID,
		},
	}
}

func dingtalkSDKChatIdentity(chat dingtalk.ChatIdentity) sdk.ChatIdentity {
	return sdk.ChatIdentity{
		ID:          strings.TrimSpace(chat.ChatID),
		IDType:      "conversation_id",
		Type:        strings.TrimSpace(chat.ChatType),
		DisplayName: strings.TrimSpace(chat.DisplayName),
	}
}

func dingtalkMentionIdentities(event *dingtalk.StreamEvent) []sdk.MentionIdentity {
	if event == nil {
		return nil
	}
	out := make([]sdk.MentionIdentity, 0, 2+len(event.AtUserIDs)+len(event.AtDingtalkIDs)+len(event.AtMobiles))
	for _, id := range event.AtUserIDs {
		out = append(out, sdk.MentionIdentity{ID: id, IDType: "staff_id"})
	}
	for _, id := range event.AtDingtalkIDs {
		out = append(out, sdk.MentionIdentity{ID: id, IDType: "dingtalk_id"})
	}
	for _, id := range event.AtMobiles {
		out = append(out, sdk.MentionIdentity{ID: id, IDType: "mobile"})
	}
	if event.IsAtAll {
		out = append(out, sdk.MentionIdentity{ID: "all", IDType: "mention_all", DisplayName: "all"})
	}
	if event.IsInAtList {
		if id := strings.TrimSpace(event.ChatbotUserID); id != "" {
			out = append(out, sdk.MentionIdentity{ID: id, IDType: "chatbot_user_id"})
		}
		if id := strings.TrimSpace(event.RobotCode); id != "" {
			out = append(out, sdk.MentionIdentity{ID: id, IDType: "robot_code"})
		}
	}
	return uniqueMentionIdentities(out)
}

func dingtalkOutboundAtOptions(req sdk.OutboundMessage) dingtalk.AtOptions {
	out := dingtalk.AtOptions{
		AtAll: req.MentionAll || boolValue(req.Raw["mention_all"]) || boolValue(req.Raw["mentionAll"]) ||
			boolValue(req.Raw["at_all"]) || boolValue(req.Raw["atAll"]) || boolValue(req.Raw["isAtAll"]),
	}
	for _, id := range stringSlice(firstValue(req.Raw["at_user_ids"], req.Raw["atUserIds"], req.Raw["user_ids"], req.Raw["userIds"])) {
		out.AtUserIDs = append(out.AtUserIDs, id)
	}
	for _, id := range stringSlice(firstValue(req.Raw["at_dingtalk_ids"], req.Raw["atDingtalkIds"], req.Raw["dingtalk_ids"], req.Raw["dingtalkIds"])) {
		out.AtDingtalkIDs = append(out.AtDingtalkIDs, id)
	}
	for _, id := range stringSlice(firstValue(req.Raw["at_mobiles"], req.Raw["atMobiles"], req.Raw["mobiles"])) {
		out.AtMobiles = append(out.AtMobiles, id)
	}
	mentions := append([]sdk.MentionIdentity{}, req.Mentions...)
	mentions = append(mentions, rawMentionIdentities(req.Raw["mentions"])...)
	for _, mention := range mentions {
		id := strings.TrimSpace(mention.ID)
		if id == "" {
			continue
		}
		switch strings.ToLower(strings.TrimSpace(mention.IDType)) {
		case "all", "mention_all", "at_all":
			out.AtAll = true
		case "dingtalk_id", "dingtalk_user_id", "chatbot_user_id", "robot_user_id", "robot_code":
			out.AtDingtalkIDs = append(out.AtDingtalkIDs, id)
		case "mobile", "phone":
			out.AtMobiles = append(out.AtMobiles, id)
		default:
			out.AtUserIDs = append(out.AtUserIDs, id)
		}
	}
	out.AtUserIDs = uniqueStringList(out.AtUserIDs)
	out.AtDingtalkIDs = uniqueStringList(out.AtDingtalkIDs)
	out.AtMobiles = uniqueStringList(out.AtMobiles)
	return out
}

func dingtalkOutboundFormat(req sdk.OutboundMessage) string {
	format := strings.ToLower(strings.TrimSpace(firstString(
		req.Format,
		req.Raw["format"],
		req.Raw["content_type"],
		req.Raw["content_format"],
		req.Raw["contentType"],
		req.Raw["contentFormat"],
		req.Raw["message_format"],
		req.Raw["messageFormat"],
		req.Raw["msg_format"],
		req.Raw["msgFormat"],
		req.Raw["msg_type"],
		req.Raw["msgType"],
	)))
	if format == "markdown" || format == "md" || format == "text/markdown" || format == "application/markdown" || format == "samplemarkdown" {
		return "markdown"
	}
	return "text"
}

func dingtalkOutboundTitle(req sdk.OutboundMessage) string {
	if title := strings.TrimSpace(firstString(req.Title, req.Raw["title"])); title != "" {
		return title
	}
	return ""
}

func dingtalkEventOwnershipValid(account sdk.ChannelAccount, event *dingtalk.StreamEvent) bool {
	expected := firstString(account.Credential["robot_code"], account.Credential["client_id"])
	if expected == "" || event == nil {
		return true
	}
	received := strings.TrimSpace(event.RobotCode)
	if received == "" {
		return true
	}
	return received == expected
}

func rawMentionIdentities(value any) []sdk.MentionIdentity {
	switch typed := value.(type) {
	case []sdk.MentionIdentity:
		return typed
	case []any:
		out := make([]sdk.MentionIdentity, 0, len(typed))
		for _, item := range typed {
			out = append(out, mentionIdentityFromAny(item))
		}
		return out
	case []map[string]any:
		out := make([]sdk.MentionIdentity, 0, len(typed))
		for _, item := range typed {
			out = append(out, mentionIdentityFromAny(item))
		}
		return out
	case string:
		if strings.TrimSpace(typed) == "" {
			return nil
		}
		var parsed []map[string]any
		if err := json.Unmarshal([]byte(typed), &parsed); err == nil {
			out := make([]sdk.MentionIdentity, 0, len(parsed))
			for _, item := range parsed {
				out = append(out, mentionIdentityFromAny(item))
			}
			return out
		}
	case json.RawMessage:
		var parsed []map[string]any
		if err := json.Unmarshal(typed, &parsed); err == nil {
			out := make([]sdk.MentionIdentity, 0, len(parsed))
			for _, item := range parsed {
				out = append(out, mentionIdentityFromAny(item))
			}
			return out
		}
	}
	return nil
}

func mentionIdentityFromAny(value any) sdk.MentionIdentity {
	mention, ok := value.(sdk.MentionIdentity)
	if ok {
		return mention
	}
	item, ok := value.(map[string]any)
	if !ok {
		return sdk.MentionIdentity{}
	}
	return sdk.MentionIdentity{
		ID:          firstString(item["id"], item["ID"], item["user_id"], item["userId"], item["dingtalk_id"], item["dingtalkId"], item["mobile"]),
		IDType:      firstString(item["id_type"], item["idType"], item["IDType"], item["type"]),
		DisplayName: firstString(item["display_name"], item["displayName"], item["name"]),
	}
}

func uniqueMentionIdentities(mentions []sdk.MentionIdentity) []sdk.MentionIdentity {
	seen := make(map[string]struct{}, len(mentions))
	out := make([]sdk.MentionIdentity, 0, len(mentions))
	for _, mention := range mentions {
		mention.ID = strings.TrimSpace(mention.ID)
		mention.IDType = strings.TrimSpace(mention.IDType)
		mention.DisplayName = strings.TrimSpace(mention.DisplayName)
		if mention.ID == "" {
			continue
		}
		key := mention.IDType + "\x00" + mention.ID
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, mention)
	}
	return out
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

func (s *connectorStateStore) LoadAccount(ctx context.Context, accountID string) (*beakstate.AccountState, error) {
	s.mu.Lock()
	if account, ok := s.accounts[accountID]; ok {
		sdkAccount := s.sdkAccounts[accountID]
		accountStore := s.accountStore
		s.mu.Unlock()
		if refreshed, ok, err := loadAccountState(ctx, accountStore, sdkAccount); err != nil {
			return nil, err
		} else if ok {
			s.mu.Lock()
			s.accounts[accountID] = refreshed
			sdkAccount.State = accountStateToSDK(*refreshed, sdkAccount).State
			s.sdkAccounts[accountID] = sdkAccount
			s.mu.Unlock()
			return refreshed, nil
		}
		return account, nil
	}
	accountStore := s.accountStore
	s.mu.Unlock()
	if refreshed, ok, err := loadAccountState(ctx, accountStore, sdk.ChannelAccount{UUID: accountID}); err != nil {
		return nil, err
	} else if ok {
		s.mu.Lock()
		s.accounts[accountID] = refreshed
		s.sdkAccounts[accountID] = accountStateToSDK(*refreshed, sdk.ChannelAccount{UUID: accountID})
		s.mu.Unlock()
		return refreshed, nil
	}
	account := &beakstate.AccountState{AccountID: accountID}
	account.EnsureMaps()
	s.mu.Lock()
	s.accounts[accountID] = account
	s.mu.Unlock()
	return account, nil
}

func loadAccountState(ctx context.Context, accountStore sdk.AccountStore, account sdk.ChannelAccount) (*beakstate.AccountState, bool, error) {
	if accountStore == nil || strings.TrimSpace(account.UUID) == "" {
		return nil, false, nil
	}
	state, err := accountStore.LoadChannelAccountState(ctx, account.UUID)
	if err != nil {
		return nil, false, err
	}
	if len(state) == 0 {
		return nil, false, nil
	}
	account.State = state
	refreshed := sdkAccountToState(account)
	return &refreshed, true, nil
}

func (s *connectorStateStore) SaveAccount(ctx context.Context, account *beakstate.AccountState) error {
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
		return accountStore.SaveChannelAccountState(ctx, sdkAccount.UUID, sdkAccount.State)
	}
	return nil
}

func sdkAccountToState(account sdk.ChannelAccount) beakstate.AccountState {
	out := beakstate.AccountState{
		AccountID:                  accountKey(account),
		ClientID:                   stringValue(account.Credential["client_id"]),
		RobotCode:                  firstString(account.Credential["robot_code"], account.State["robot_code"], standardBotIdentityValue(account.State, "robot_code"), account.Credential["client_id"]),
		BaseURL:                    baseURLFromCredential(account.Credential),
		AccessToken:                stringValue(account.State["access_token"]),
		AccessTokenExpires:         timeValue(account.State["access_token_expires_at"]),
		ChatbotUserID:              firstString(account.State["chatbot_user_id"], standardBotIdentityValue(account.State, "chatbot_user_id"), account.Credential["chatbot_user_id"]),
		ChatbotCorpID:              firstString(account.Credential["chatbot_corp_id"], account.State["chatbot_corp_id"]),
		ChannelLinkSession:         stringValue(account.State["channel_link_session"]),
		PeerSessions:               stringMap(account.State["peer_sessions"]),
		SessionWebhooks:            webhookMap(account.State["session_webhooks"]),
		InboundSeen:                stringMap(account.State["inbound_seen"]),
		SentBeakMessages:           stringMap(account.State["sent_beak_messages"]),
		StreamCursors:              stringMap(account.State["stream_cursors"]),
		StreamConnectionState:      stringValue(account.State[sdk.RuntimeHealthKeyStreamConnectionState]),
		StreamConnectedAt:          timeValue(account.State[sdk.RuntimeHealthKeyStreamConnectedAt]),
		StreamDisconnectedAt:       timeValue(account.State[sdk.RuntimeHealthKeyStreamDisconnectedAt]),
		StreamLastActivityAt:       timeValue(account.State[sdk.RuntimeHealthKeyStreamLastActivityAt]),
		StreamLastPingAt:           timeValue(account.State[sdk.RuntimeHealthKeyStreamLastPingAt]),
		StreamLastPongAt:           timeValue(account.State[sdk.RuntimeHealthKeyStreamLastPongAt]),
		StreamLastEventAt:          timeValue(account.State[sdk.RuntimeHealthKeyStreamLastEventAt]),
		StreamLastError:            stringValue(account.State[sdk.RuntimeHealthKeyStreamLastError]),
		StreamLastErrorAt:          timeValue(account.State[sdk.RuntimeHealthKeyStreamLastErrorAt]),
		StreamReconnectRequestedAt: timeValue(account.State[sdk.RuntimeHealthKeyStreamReconnectRequestedAt]),
		StreamReconnectError:       stringValue(account.State[sdk.RuntimeHealthKeyStreamReconnectError]),
		StreamReconnectErrorAt:     timeValue(account.State[sdk.RuntimeHealthKeyStreamReconnectErrorAt]),
		StreamSessionExpired:       boolValue(account.State[sdk.RuntimeHealthKeyStreamSessionExpired]),
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
		"channel_link_session":                         account.ChannelLinkSession,
		"peer_sessions":                                account.PeerSessions,
		"session_webhooks":                             account.SessionWebhooks,
		"inbound_seen":                                 account.InboundSeen,
		"sent_beak_messages":                           account.SentBeakMessages,
		"stream_cursors":                               account.StreamCursors,
		"access_token":                                 account.AccessToken,
		"access_token_expires_at":                      account.AccessTokenExpires,
		"chatbot_user_id":                              account.ChatbotUserID,
		"chatbot_corp_id":                              account.ChatbotCorpID,
		"robot_code":                                   account.RobotCode,
		sdk.RuntimeHealthKeyStreamConnectionState:      account.StreamConnectionState,
		sdk.RuntimeHealthKeyStreamConnectedAt:          account.StreamConnectedAt,
		sdk.RuntimeHealthKeyStreamDisconnectedAt:       account.StreamDisconnectedAt,
		sdk.RuntimeHealthKeyStreamLastActivityAt:       account.StreamLastActivityAt,
		sdk.RuntimeHealthKeyStreamLastPingAt:           account.StreamLastPingAt,
		sdk.RuntimeHealthKeyStreamLastPongAt:           account.StreamLastPongAt,
		sdk.RuntimeHealthKeyStreamLastEventAt:          account.StreamLastEventAt,
		sdk.RuntimeHealthKeyStreamLastError:            account.StreamLastError,
		sdk.RuntimeHealthKeyStreamLastErrorAt:          account.StreamLastErrorAt,
		sdk.RuntimeHealthKeyStreamReconnectRequestedAt: account.StreamReconnectRequestedAt,
		sdk.RuntimeHealthKeyStreamReconnectError:       account.StreamReconnectError,
		sdk.RuntimeHealthKeyStreamReconnectErrorAt:     account.StreamReconnectErrorAt,
		sdk.RuntimeHealthKeyStreamSessionExpired:       account.StreamSessionExpired,
		"updated_at":                                   account.UpdatedAt,
	}
	if identities := dingtalkBotIdentityState(account); len(identities) > 0 {
		existing.State["bot_identities"] = identities
		existing.State["bot_identity"] = identities[0]
	}
	return existing
}

func dingtalkBotIdentityState(account beakstate.AccountState) []map[string]any {
	var identities []map[string]any
	add := func(id, idType string) {
		id = strings.TrimSpace(id)
		if id == "" {
			return
		}
		identities = append(identities, map[string]any{
			"id":      id,
			"id_type": idType,
		})
	}
	add(account.ChatbotUserID, "chatbot_user_id")
	add(account.RobotCode, "robot_code")
	return identities
}

func standardBotIdentityValue(state map[string]any, idTypes ...string) string {
	wanted := make(map[string]struct{}, len(idTypes))
	for _, idType := range idTypes {
		idType = strings.TrimSpace(idType)
		if idType != "" {
			wanted[idType] = struct{}{}
		}
	}
	for _, identity := range standardBotIdentityMaps(state) {
		idType := strings.TrimSpace(stringValue(identity["id_type"]))
		if len(wanted) > 0 {
			if _, ok := wanted[idType]; !ok {
				continue
			}
		}
		if id := strings.TrimSpace(stringValue(identity["id"])); id != "" {
			return id
		}
	}
	return ""
}

func standardBotIdentityMaps(state map[string]any) []map[string]any {
	if len(state) == 0 {
		return nil
	}
	var out []map[string]any
	out = append(out, botIdentityMapsFromAny(state["bot_identities"])...)
	out = append(out, botIdentityMapsFromAny(state["bot_identity"])...)
	return out
}

func botIdentityMapsFromAny(value any) []map[string]any {
	switch typed := value.(type) {
	case nil:
		return nil
	case map[string]any:
		return []map[string]any{typed}
	case []map[string]any:
		return typed
	case []any:
		out := make([]map[string]any, 0, len(typed))
		for _, item := range typed {
			out = append(out, botIdentityMapsFromAny(item)...)
		}
		return out
	case json.RawMessage:
		var list []map[string]any
		if err := json.Unmarshal(typed, &list); err == nil {
			return list
		}
		var item map[string]any
		if err := json.Unmarshal(typed, &item); err == nil {
			return []map[string]any{item}
		}
	case string:
		if strings.TrimSpace(typed) == "" {
			return nil
		}
		return botIdentityMapsFromAny(json.RawMessage(typed))
	}
	return nil
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

func stringSlice(value any) []string {
	var values []any
	switch typed := value.(type) {
	case []string:
		out := make([]string, 0, len(typed))
		for _, item := range typed {
			if item = strings.TrimSpace(item); item != "" {
				out = append(out, item)
			}
		}
		return out
	case []any:
		values = typed
	case string:
		if strings.TrimSpace(typed) == "" {
			return nil
		}
		var parsed []any
		if err := json.Unmarshal([]byte(typed), &parsed); err == nil {
			values = parsed
			break
		}
		return []string{strings.TrimSpace(typed)}
	case json.RawMessage:
		var parsed []any
		if err := json.Unmarshal(typed, &parsed); err == nil {
			values = parsed
		}
	}
	out := make([]string, 0, len(values))
	for _, value := range values {
		if item := strings.TrimSpace(stringValue(value)); item != "" {
			out = append(out, item)
		}
	}
	return uniqueStringList(out)
}

func uniqueStringList(values []string) []string {
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

func firstValue(values ...any) any {
	for _, value := range values {
		if value != nil {
			return value
		}
	}
	return nil
}

func cloneMap(value map[string]any) map[string]any {
	out := make(map[string]any, len(value))
	for key, item := range value {
		out[key] = item
	}
	return out
}

func credentialValidationFailure(credential, state map[string]any, err error) *sdk.CredentialValidationResult {
	message := ""
	if err != nil {
		message = err.Error()
	}
	return &sdk.CredentialValidationResult{
		Valid:       false,
		AccountKey:  firstString(credential["account_id"], credential["robot_code"], credential["client_id"]),
		DisplayName: firstString(credential["display_name"], credential["robot_code"], credential["client_id"]),
		Credential:  credential,
		State:       state,
		Metadata:    map[string]any{"platform": Platform},
		Error:       message,
	}
}

var _ sdk.Connector = Connector{}
var _ EventConnector = Connector{}
