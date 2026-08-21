package beakdingtalk

import (
	"context"

	"github.com/GuanceCloud/beak-agent-channel-dingtalk/sdk"
)

const (
	ID       = "beak-agent-dingtalk"
	Platform = "dingtalk"
)

type API interface {
	RegisterChannel(Channel) error
}

type Plugin struct{}

type Channel struct{}

type Metadata struct {
	ID          string
	Platform    string
	Label       string
	Description string
}

type Capabilities struct {
	DirectChat     bool
	GroupChat      bool
	Text           bool
	Media          bool
	MediaKinds     []string
	BlockStreaming bool
}

type SettingsSchema struct {
	Type                 string         `json:"type"`
	AdditionalProperties bool           `json:"additionalProperties"`
	Properties           map[string]any `json:"properties"`
	Required             []string       `json:"required,omitempty"`
}

func New() Plugin {
	return Plugin{}
}

func Register(api API) error {
	return New().Register(api)
}

func (Plugin) Register(api API) error {
	return api.RegisterChannel(Channel{})
}

func (Plugin) Channel() Channel {
	return Channel{}
}

func (Channel) Metadata() Metadata {
	return Metadata{
		ID:          ID,
		Platform:    Platform,
		Label:       "DingTalk",
		Description: "DingTalk connector for Beak channel gateway sessions",
	}
}

func (Channel) Capabilities() Capabilities {
	return Capabilities{
		DirectChat:     true,
		GroupChat:      true,
		Text:           true,
		Media:          true,
		MediaKinds:     []string{sdk.MediaKindImage, sdk.MediaKindFile, sdk.MediaKindAudio, sdk.MediaKindVideo, sdk.MediaKindSticker},
		BlockStreaming: true,
	}
}

func (Channel) SettingsSchema() SettingsSchema {
	return SettingsSchema{
		Type:                 "object",
		AdditionalProperties: false,
		Required:             []string{"client_id", "client_secret"},
		Properties: map[string]any{
			"client_id": map[string]any{
				"type":        "string",
				"title":       "Client ID",
				"description": "DingTalk application AppKey/clientId used by the robot.",
			},
			"client_secret": map[string]any{
				"type":        "string",
				"title":       "Client Secret",
				"description": "DingTalk application AppSecret/clientSecret.",
				"secret":      true,
			},
			"robot_code": map[string]any{
				"type":        "string",
				"title":       "Robot Code",
				"description": "Optional robotCode override. Defaults to client_id.",
			},
			"chatbot_user_id": map[string]any{
				"type":        "string",
				"title":       "Chatbot User ID",
				"description": "Optional encrypted chatbot user id used to drop self echo messages.",
			},
			"chatbot_corp_id": map[string]any{
				"type":        "string",
				"title":       "Chatbot Corp ID",
				"description": "Optional DingTalk corp id metadata for this bot.",
			},
		},
	}
}

func (Channel) CheckHealth(context.Context) error {
	return nil
}
