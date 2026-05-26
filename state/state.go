package state

import (
	"fmt"
	"time"
)

type AccountState struct {
	AccountID          string             `json:"account_id"`
	ClientID           string             `json:"client_id,omitempty"`
	RobotCode          string             `json:"robot_code,omitempty"`
	BaseURL            string             `json:"base_url,omitempty"`
	AccessToken        string             `json:"access_token,omitempty"`
	AccessTokenExpires time.Time          `json:"access_token_expires_at,omitempty"`
	ChatbotUserID      string             `json:"chatbot_user_id,omitempty"`
	ChatbotCorpID      string             `json:"chatbot_corp_id,omitempty"`
	ChannelLinkSession string             `json:"channel_link_session,omitempty"`
	PeerSessions       map[string]string  `json:"peer_sessions,omitempty"`
	SessionWebhooks    map[string]Webhook `json:"session_webhooks,omitempty"`
	InboundSeen        map[string]string  `json:"inbound_seen,omitempty"`
	SentBeakMessages   map[string]string  `json:"sent_beak_messages,omitempty"`
	StreamCursors      map[string]string  `json:"stream_cursors,omitempty"`
	UpdatedAt          time.Time          `json:"updated_at"`
}

type Webhook struct {
	URL       string    `json:"url"`
	ExpiresAt time.Time `json:"expires_at,omitempty"`
}

func (a *AccountState) EnsureMaps() {
	if a == nil {
		return
	}
	if a.PeerSessions == nil {
		a.PeerSessions = make(map[string]string)
	}
	if a.SessionWebhooks == nil {
		a.SessionWebhooks = make(map[string]Webhook)
	}
	if a.InboundSeen == nil {
		a.InboundSeen = make(map[string]string)
	}
	if a.SentBeakMessages == nil {
		a.SentBeakMessages = make(map[string]string)
	}
	if a.StreamCursors == nil {
		a.StreamCursors = make(map[string]string)
	}
}

func TouchAccount(account *AccountState) error {
	if account == nil {
		return fmt.Errorf("account state is nil")
	}
	if account.AccountID == "" {
		return fmt.Errorf("account_id is required")
	}
	account.EnsureMaps()
	account.UpdatedAt = time.Now().UTC()
	return nil
}
