package dingtalk

import (
	"encoding/json"
	"fmt"
	"strings"
)

const (
	DefaultBaseURL = "https://api.dingtalk.com"

	ChatTypeDirect = "direct"
	ChatTypeGroup  = "group"

	ConversationTypeDirect = "1"
	ConversationTypeGroup  = "2"
)

type TokenResponse struct {
	AccessToken string `json:"accessToken"`
	ExpireIn    int    `json:"expireIn"`
	Code        string `json:"code,omitempty"`
	Message     string `json:"message,omitempty"`
}

type SendTextRequest struct {
	ChatType    string
	ChatID      string
	Text        string
	RobotCode   string
	MessageUUID string
}

type SendTextResponse struct {
	ProcessQueryKey string         `json:"processQueryKey"`
	Code            string         `json:"code,omitempty"`
	Message         string         `json:"message,omitempty"`
	Raw             map[string]any `json:"-"`
}

type StreamEvent struct {
	ConversationType  string `json:"conversationType"`
	ConversationID    string `json:"conversationId"`
	ConversationTitle string `json:"conversationTitle"`
	SenderStaffID     string `json:"senderStaffId"`
	SenderID          string `json:"senderId"`
	SenderNick        string `json:"senderNick"`
	ChatbotUserID     string `json:"chatbotUserId"`
	ChatbotCorpID     string `json:"chatbotCorpId"`
	SessionWebhook    string `json:"sessionWebhook"`
	RobotCode         string `json:"robotCode"`
	MsgID             string `json:"msgId"`
	MsgType           string `json:"msgtype"`
	DeliveryMessageID string
	Raw               map[string]any
}

type ChatIdentity struct {
	ChatType string
	ChatID   string
	SenderID string
}

func ParseStreamEvent(data []byte) (*StreamEvent, error) {
	payload := data
	deliveryMessageID := ""

	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(data, &envelope); err == nil {
		if rawHeaders, ok := envelope["headers"]; ok {
			var headers map[string]any
			if err := json.Unmarshal(rawHeaders, &headers); err == nil {
				deliveryMessageID = firstString(headers["messageId"], headers["message_id"])
			}
		}
		if rawData, ok := envelope["data"]; ok {
			var dataString string
			if err := json.Unmarshal(rawData, &dataString); err == nil {
				payload = []byte(dataString)
			} else {
				payload = rawData
			}
		}
	}

	var raw map[string]any
	if err := json.Unmarshal(payload, &raw); err != nil {
		return nil, fmt.Errorf("decode dingtalk stream event: %w", err)
	}
	event := &StreamEvent{
		ConversationType:  stringValue(raw["conversationType"]),
		ConversationID:    stringValue(raw["conversationId"]),
		ConversationTitle: stringValue(raw["conversationTitle"]),
		SenderStaffID:     stringValue(raw["senderStaffId"]),
		SenderID:          stringValue(raw["senderId"]),
		SenderNick:        stringValue(raw["senderNick"]),
		ChatbotUserID:     stringValue(raw["chatbotUserId"]),
		ChatbotCorpID:     stringValue(raw["chatbotCorpId"]),
		SessionWebhook:    stringValue(raw["sessionWebhook"]),
		RobotCode:         stringValue(raw["robotCode"]),
		MsgID:             stringValue(raw["msgId"]),
		MsgType:           stringValue(raw["msgtype"]),
		DeliveryMessageID: deliveryMessageID,
		Raw:               raw,
	}
	if event.MsgType == "" {
		event.MsgType = "text"
	}
	return event, nil
}

func (e StreamEvent) Sender() string {
	return firstString(e.SenderStaffID, e.SenderID)
}

func (e StreamEvent) ChatIdentity() ChatIdentity {
	senderID := e.Sender()
	switch strings.TrimSpace(e.ConversationType) {
	case ConversationTypeDirect:
		chatID := senderID
		if chatID == "" {
			chatID = strings.TrimSpace(e.ConversationID)
		}
		return ChatIdentity{ChatType: ChatTypeDirect, ChatID: chatID, SenderID: senderID}
	case ConversationTypeGroup:
		chatID := strings.TrimSpace(e.ConversationID)
		if chatID == "" {
			chatID = senderID
		}
		return ChatIdentity{ChatType: ChatTypeGroup, ChatID: chatID, SenderID: senderID}
	default:
		return ChatIdentity{ChatID: strings.TrimSpace(e.ConversationID), SenderID: senderID}
	}
}

func (c ChatIdentity) StateKey() string {
	if c.ChatType == ChatTypeGroup {
		return ChatTypeGroup + ":" + c.ChatID
	}
	return c.ChatID
}

func (e StreamEvent) Text() string {
	switch strings.TrimSpace(e.MsgType) {
	case "", "text":
		text := pathString(e.Raw, "text", "content")
		if text == "" {
			text = pathString(e.Raw, "content", "text")
		}
		return normalizeText(text)
	case "markdown":
		text := pathString(e.Raw, "text", "content")
		if text == "" {
			text = pathString(e.Raw, "content", "text")
		}
		if text == "" {
			text = pathString(e.Raw, "markdown", "text")
		}
		return normalizeText(text)
	case "richText":
		return normalizeText(richText(e.Raw))
	default:
		return ""
	}
}

func (e StreamEvent) DedupeKey(accountUUID string) string {
	accountUUID = strings.TrimSpace(accountUUID)
	if id := strings.TrimSpace(e.MsgID); id != "" {
		return accountUUID + ":message:" + id
	}
	if id := strings.TrimSpace(e.DeliveryMessageID); id != "" {
		return accountUUID + ":delivery:" + id
	}
	chat := e.ChatIdentity()
	return accountUUID + ":chat:" + chat.ChatType + ":" + chat.ChatID + ":sender:" + chat.SenderID
}

func pathString(raw map[string]any, path ...string) string {
	var cur any = raw
	for _, key := range path {
		switch typed := cur.(type) {
		case map[string]any:
			cur = typed[key]
		case string:
			var next map[string]any
			if err := json.Unmarshal([]byte(typed), &next); err != nil {
				return ""
			}
			cur = next[key]
		default:
			return ""
		}
	}
	return stringValue(cur)
}

func richText(raw map[string]any) string {
	lists := []any{
		pathValue(raw, "content", "richText"),
		pathValue(raw, "richText", "richTextList"),
	}
	var parts []string
	for _, list := range lists {
		items, ok := list.([]any)
		if !ok {
			continue
		}
		for _, item := range items {
			obj, ok := item.(map[string]any)
			if !ok {
				continue
			}
			if stringValue(obj["type"]) == "skill" || obj["skillData"] != nil {
				continue
			}
			if text := stringValue(obj["text"]); text != "" {
				parts = append(parts, text)
			}
		}
	}
	return strings.Join(parts, "")
}

func pathValue(raw map[string]any, path ...string) any {
	var cur any = raw
	for _, key := range path {
		switch typed := cur.(type) {
		case map[string]any:
			cur = typed[key]
		case string:
			var next map[string]any
			if err := json.Unmarshal([]byte(typed), &next); err != nil {
				return nil
			}
			cur = next[key]
		default:
			return nil
		}
	}
	return cur
}

func normalizeText(text string) string {
	return strings.TrimSpace(text)
}

func firstString(values ...any) string {
	for _, value := range values {
		if text := strings.TrimSpace(stringValue(value)); text != "" {
			return text
		}
	}
	return ""
}

func stringValue(value any) string {
	if value == nil {
		return ""
	}
	switch typed := value.(type) {
	case string:
		return typed
	case json.Number:
		return typed.String()
	default:
		return fmt.Sprint(typed)
	}
}
