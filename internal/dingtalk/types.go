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
	At          AtOptions
}

type AtOptions struct {
	AtUserIDs     []string
	AtDingtalkIDs []string
	AtMobiles     []string
	AtAll         bool
}

type SendTextResponse struct {
	ProcessQueryKey string         `json:"processQueryKey"`
	Code            string         `json:"code,omitempty"`
	Message         string         `json:"message,omitempty"`
	Raw             map[string]any `json:"-"`
}

type WebhookSendResponse struct {
	ErrCode int            `json:"errcode"`
	ErrMsg  string         `json:"errmsg"`
	Raw     map[string]any `json:"-"`
}

type SendWebhookTextRequest struct {
	Text string
	At   AtOptions
}

type StreamEvent struct {
	ConversationType          string `json:"conversationType"`
	ConversationID            string `json:"conversationId"`
	ConversationTitle         string `json:"conversationTitle"`
	SenderStaffID             string `json:"senderStaffId"`
	SenderID                  string `json:"senderId"`
	SenderNick                string `json:"senderNick"`
	ChatbotUserID             string `json:"chatbotUserId"`
	ChatbotCorpID             string `json:"chatbotCorpId"`
	SessionWebhook            string `json:"sessionWebhook"`
	SessionWebhookExpiredTime int64  `json:"sessionWebhookExpiredTime"`
	RobotCode                 string `json:"robotCode"`
	MsgID                     string `json:"msgId"`
	MsgType                   string `json:"msgtype"`
	IsInAtList                bool   `json:"isInAtList"`
	IsAtAll                   bool   `json:"isAtAll"`
	AtUserIDs                 []string
	AtDingtalkIDs             []string
	AtMobiles                 []string
	DeliveryMessageID         string
	Raw                       map[string]any
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
		ConversationType:          stringValue(raw["conversationType"]),
		ConversationID:            stringValue(raw["conversationId"]),
		ConversationTitle:         stringValue(raw["conversationTitle"]),
		SenderStaffID:             stringValue(raw["senderStaffId"]),
		SenderID:                  stringValue(raw["senderId"]),
		SenderNick:                stringValue(raw["senderNick"]),
		ChatbotUserID:             stringValue(raw["chatbotUserId"]),
		ChatbotCorpID:             stringValue(raw["chatbotCorpId"]),
		SessionWebhook:            stringValue(raw["sessionWebhook"]),
		SessionWebhookExpiredTime: int64Value(raw["sessionWebhookExpiredTime"]),
		RobotCode:                 stringValue(raw["robotCode"]),
		MsgID:                     stringValue(raw["msgId"]),
		MsgType:                   stringValue(raw["msgtype"]),
		IsInAtList:                boolValue(raw["isInAtList"]),
		IsAtAll: boolValue(firstValue(
			raw["isAtAll"],
			pathValue(raw, "text", "at", "isAtAll"),
			pathValue(raw, "at", "isAtAll"),
			raw["atAll"],
		)),
		AtUserIDs: stringSlice(firstValue(
			pathValue(raw, "text", "at", "atUserIds"),
			pathValue(raw, "at", "atUserIds"),
			raw["atUserIds"],
			raw["at_user_ids"],
		)),
		AtDingtalkIDs: stringSlice(firstValue(
			pathValue(raw, "text", "at", "atDingtalkIds"),
			pathValue(raw, "at", "atDingtalkIds"),
			raw["atDingtalkIds"],
			raw["at_dingtalk_ids"],
		)),
		AtMobiles: stringSlice(firstValue(
			pathValue(raw, "text", "at", "atMobiles"),
			pathValue(raw, "at", "atMobiles"),
			raw["atMobiles"],
			raw["at_mobiles"],
		)),
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

type StreamFrameResponse struct {
	Code    int               `json:"code"`
	Headers map[string]string `json:"headers"`
	Message string            `json:"message"`
	Data    string            `json:"data"`
}

func StreamAck(messageID string, data string) StreamFrameResponse {
	if strings.TrimSpace(data) == "" {
		data = `{"response":null}`
	}
	return StreamFrameResponse{
		Code:    200,
		Message: "OK",
		Headers: map[string]string{
			"contentType": "application/json",
			"messageId":   messageID,
		},
		Data: data,
	}
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

func firstValue(values ...any) any {
	for _, value := range values {
		if value == nil {
			continue
		}
		return value
	}
	return nil
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
		if typed == "" {
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
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		item := strings.TrimSpace(stringValue(value))
		if item == "" {
			continue
		}
		if _, ok := seen[item]; ok {
			continue
		}
		seen[item] = struct{}{}
		out = append(out, item)
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
	case json.Number:
		return typed.String()
	default:
		return fmt.Sprint(typed)
	}
}

func int64Value(value any) int64 {
	switch typed := value.(type) {
	case int64:
		return typed
	case int:
		return int64(typed)
	case float64:
		return int64(typed)
	case json.Number:
		item, _ := typed.Int64()
		return item
	case string:
		var item int64
		_, _ = fmt.Sscan(strings.TrimSpace(typed), &item)
		return item
	default:
		return 0
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
