package sdk

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"time"
)

const (
	ChatTypeGroup  = "group"
	ChatTypeDirect = "direct"

	LoginModeQRCode     = "qr_code"
	LoginModeCredential = "credential"
)

type Connector interface {
	Metadata() ConnectorMetadata
	CredentialSchema(ctx context.Context) CredentialSchema
	ValidateCredential(ctx context.Context, req CredentialValidationRequest) (*CredentialValidationResult, error)
	StartLogin(ctx context.Context, req LoginStartRequest) (*LoginChallenge, error)
	PollLogin(ctx context.Context, req LoginPollRequest) (*LoginStatus, error)
	Start(ctx context.Context, runtime Runtime) error
	Send(ctx context.Context, runtime Runtime, req OutboundMessage) (*SendResult, error)
	Acknowledge(ctx context.Context, runtime Runtime, req OutboundAck) (*AckResult, error)
	Stop(ctx context.Context, account ChannelAccount) error
}

type WebhookResponse struct {
	StatusCode int
	Headers    map[string]string
	Body       []byte
}

type ConnectorMetadata struct {
	ID           string       `json:"id"`
	Platform     string       `json:"platform"`
	Label        string       `json:"label"`
	Description  string       `json:"description,omitempty"`
	Capabilities Capabilities `json:"capabilities"`
}

type Capabilities struct {
	LoginModes     []string `json:"login_modes"`
	Text           bool     `json:"text"`
	Media          bool     `json:"media"`
	GroupChat      bool     `json:"group_chat"`
	DirectChat     bool     `json:"direct_chat"`
	Stream         bool     `json:"stream"`
	Webhook        bool     `json:"webhook"`
	BlockStreaming bool     `json:"block_streaming"`
	AckModes       []string `json:"ack_modes,omitempty"`
}

type CredentialSchema struct {
	Type                 string                     `json:"type"`
	LoginModes           []string                   `json:"login_modes"`
	Properties           map[string]CredentialField `json:"properties"`
	Required             []string                   `json:"required,omitempty"`
	AdditionalProperties bool                       `json:"additionalProperties"`
}

type CredentialField struct {
	Type        string `json:"type"`
	Title       string `json:"title,omitempty"`
	Description string `json:"description,omitempty"`
	Secret      bool   `json:"secret,omitempty"`
}

type CredentialValidationRequest struct {
	WorkspaceUUID string         `json:"workspace_uuid,omitempty"`
	ChannelUUID   string         `json:"channel_uuid,omitempty"`
	Platform      string         `json:"platform,omitempty"`
	Credential    map[string]any `json:"credential,omitempty"`
	State         map[string]any `json:"state,omitempty"`
	Runtime       Runtime        `json:"-"`
}

type CredentialValidationResult struct {
	Valid       bool           `json:"valid"`
	AccountKey  string         `json:"account_key,omitempty"`
	DisplayName string         `json:"display_name,omitempty"`
	Credential  map[string]any `json:"credential,omitempty"`
	State       map[string]any `json:"state,omitempty"`
	Metadata    map[string]any `json:"metadata,omitempty"`
	Error       string         `json:"error,omitempty"`
}

type Runtime struct {
	WorkspaceUUID   string
	Channel         Channel
	Account         ChannelAccount
	Accounts        []ChannelAccount
	Gateway         Gateway
	AccountStore    AccountStore
	HTTPClient      *http.Client
	Logger          *log.Logger
	PollInterval    time.Duration
	StreamReconnect time.Duration
	Native          any
}

type Gateway interface {
	EnsureChannel(ctx context.Context, req EnsureChannelRequest) (string, error)
	EnsureChannelLinkSession(ctx context.Context, req EnsureChannelLinkSessionRequest) (string, error)
	EnsureChatSession(ctx context.Context, req EnsureChatSessionRequest) (string, error)
	CreateMessage(ctx context.Context, req CreateMessageRequest) (string, error)
	StreamSession(ctx context.Context, req StreamSessionRequest, handle func(StreamEvent) error) error
	AgentParticipantID() string
	BridgeParticipantID(platform string) string
}

type AccountStore interface {
	LoadChannelAccountState(ctx context.Context, accountUUID string) (map[string]any, error)
	SaveChannelAccountState(ctx context.Context, accountUUID string, state map[string]any) error
}

type Channel struct {
	UUID          string         `json:"channel_uuid"`
	WorkspaceUUID string         `json:"workspace_uuid"`
	Platform      string         `json:"platform"`
	Name          string         `json:"name,omitempty"`
	Config        map[string]any `json:"config,omitempty"`
}

type ChannelAccount struct {
	UUID          string         `json:"account_uuid"`
	WorkspaceUUID string         `json:"workspace_uuid"`
	ChannelUUID   string         `json:"channel_uuid"`
	Platform      string         `json:"platform"`
	DisplayName   string         `json:"display_name,omitempty"`
	Credential    map[string]any `json:"credential,omitempty"`
	State         map[string]any `json:"state,omitempty"`
	Status        string         `json:"status,omitempty"`
}

type LoginStartRequest struct {
	WorkspaceUUID string         `json:"workspace_uuid"`
	ChannelUUID   string         `json:"channel_uuid"`
	Payload       map[string]any `json:"payload,omitempty"`
	Runtime       Runtime        `json:"-"`
}

type LoginPollRequest struct {
	WorkspaceUUID  string         `json:"workspace_uuid"`
	ChannelUUID    string         `json:"channel_uuid"`
	ChallengeUUID  string         `json:"challenge_uuid,omitempty"`
	ChallengeCode  string         `json:"challenge_code,omitempty"`
	ChallengeState map[string]any `json:"challenge_state,omitempty"`
	Runtime        Runtime        `json:"-"`
}

type LoginChallenge struct {
	Type      string         `json:"type"`
	Code      string         `json:"code,omitempty"`
	URL       string         `json:"url,omitempty"`
	ExpiresAt *time.Time     `json:"expires_at,omitempty"`
	State     map[string]any `json:"state,omitempty"`
}

type LoginStatus struct {
	Status     string         `json:"status"`
	Confirmed  bool           `json:"confirmed"`
	Expired    bool           `json:"expired"`
	Account    ChannelAccount `json:"account,omitempty"`
	Credential map[string]any `json:"credential,omitempty"`
	State      map[string]any `json:"state,omitempty"`
}

type InboundMessage struct {
	WorkspaceUUID     string            `json:"workspace_uuid"`
	Platform          string            `json:"platform"`
	AccountUUID       string            `json:"account_uuid"`
	ChannelUUID       string            `json:"channel_uuid"`
	ChatType          string            `json:"chat_type"`
	ChatID            string            `json:"chat_id"`
	ThreadID          string            `json:"thread_id,omitempty"`
	ChatDisplayName   string            `json:"chat_display_name,omitempty"`
	ChatAvatarURL     string            `json:"chat_avatar_url,omitempty"`
	ChatIdentity      ChatIdentity      `json:"chat_identity,omitempty"`
	SenderID          string            `json:"sender_id"`
	SenderDisplayName string            `json:"sender_display_name,omitempty"`
	MessageID         string            `json:"message_id,omitempty"`
	Text              string            `json:"text"`
	DedupeKey         string            `json:"dedupe_key,omitempty"`
	Mentions          []MentionIdentity `json:"mentions,omitempty"`
	MentionedMe       bool              `json:"mentioned_me,omitempty"`
	MentionAll        bool              `json:"mention_all,omitempty"`
	Raw               map[string]any    `json:"raw,omitempty"`
}

type MentionIdentity struct {
	ID          string `json:"id"`
	IDType      string `json:"id_type,omitempty"`
	DisplayName string `json:"display_name,omitempty"`
}

type ChatIdentity struct {
	ID          string `json:"id,omitempty"`
	IDType      string `json:"id_type,omitempty"`
	Type        string `json:"type,omitempty"`
	DisplayName string `json:"display_name,omitempty"`
	AvatarURL   string `json:"avatar_url,omitempty"`
}

type OutboundMessage struct {
	WorkspaceUUID string `json:"workspace_uuid"`
	Platform      string `json:"platform"`
	AccountUUID   string `json:"account_uuid"`
	ChannelUUID   string `json:"channel_uuid"`
	SessionUUID   string `json:"session_uuid"`
	MessageUUID   string `json:"message_uuid"`
	ChatType      string `json:"chat_type"`
	ChatID        string `json:"chat_id"`
	Text          string `json:"text"`
	// Format is a common host-facing field. Set "markdown" for rich rendering;
	// each SDK maps it to platform-native delivery or falls back internally.
	Format string `json:"format,omitempty"`
	// Title is a common markdown title hint. Hosts should pass it uniformly and
	// let the SDK decide whether the platform can use it.
	Title      string            `json:"title,omitempty"`
	Mentions   []MentionIdentity `json:"mentions,omitempty"`
	MentionAll bool              `json:"mention_all,omitempty"`
	// Raw is a platform-native escape hatch, not required for common text or markdown.
	Raw map[string]any `json:"raw,omitempty"`
}

type SendResult struct {
	Platform    string         `json:"platform"`
	AccountUUID string         `json:"account_uuid"`
	MessageID   string         `json:"message_id,omitempty"`
	Raw         map[string]any `json:"raw,omitempty"`
}

type OutboundAck struct {
	WorkspaceUUID     string         `json:"workspace_uuid"`
	Platform          string         `json:"platform"`
	AccountUUID       string         `json:"account_uuid"`
	ChannelUUID       string         `json:"channel_uuid"`
	SessionUUID       string         `json:"session_uuid"`
	SourceMessageUUID string         `json:"source_message_uuid,omitempty"`
	ChatType          string         `json:"chat_type"`
	ChatID            string         `json:"chat_id"`
	TargetMessageID   string         `json:"target_message_id,omitempty"`
	Intent            string         `json:"intent,omitempty"`
	Action            string         `json:"action,omitempty"`
	Mode              string         `json:"mode,omitempty"`
	Emoji             string         `json:"emoji,omitempty"`
	Raw               map[string]any `json:"raw,omitempty"`
}

type AckResult struct {
	Platform    string         `json:"platform"`
	AccountUUID string         `json:"account_uuid"`
	Mode        string         `json:"mode,omitempty"`
	Status      string         `json:"status"`
	ReactionID  string         `json:"reaction_id,omitempty"`
	Raw         map[string]any `json:"raw,omitempty"`
}

type EnsureChannelRequest struct {
	WorkspaceUUID string
	Platform      string
	Name          string
	Config        map[string]any
}

type EnsureChannelLinkSessionRequest struct {
	WorkspaceUUID       string
	Platform            string
	AccountUUID         string
	AgentParticipantID  string
	BridgeParticipantID string
}

type EnsureChatSessionRequest struct {
	WorkspaceUUID       string
	Platform            string
	AccountUUID         string
	ChatType            string
	ChatID              string
	ThreadID            string
	ChatDisplayName     string
	ChatAvatarURL       string
	ChatIdentity        ChatIdentity
	SenderID            string
	AgentParticipantID  string
	BridgeParticipantID string
	Metadata            map[string]any
}

type CreateMessageRequest struct {
	WorkspaceUUID string
	SessionUUID   string
	SenderID      string
	Content       string
	DedupeKey     string
	Metadata      map[string]any
}

type StreamSessionRequest struct {
	WorkspaceUUID string
	SessionUUID   string
	SubscriberID  string
	LastEventUUID string
	Filters       map[string]any
}

type StreamEvent struct {
	EventUUID     string          `json:"event_uuid,omitempty"`
	WorkspaceUUID string          `json:"workspace_uuid,omitempty"`
	SessionUUID   string          `json:"session_uuid,omitempty"`
	EventType     string          `json:"event_type,omitempty"`
	MessageUUID   string          `json:"message_uuid,omitempty"`
	SenderID      string          `json:"sender_id,omitempty"`
	Content       string          `json:"content,omitempty"`
	Payload       json.RawMessage `json:"payload,omitempty"`
}
