package beakdingtalk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/GuanceCloud/beak-agent-channel-dingtalk/internal/dingtalk"
	"github.com/GuanceCloud/beak-agent-channel-dingtalk/sdk"
	dtpayload "github.com/open-dingtalk/dingtalk-stream-sdk-go/payload"
	"strconv"
	"strings"
	"time"
)

const cardCallbackTopic = "/v1.0/card/instances/callback"

func sendDingtalkInteraction(ctx context.Context, runtime sdk.Runtime, account sdk.ChannelAccount, req sdk.OutboundMessage) (*sdk.SendResult, error) {
	client := clientFromAccount(runtime, account)
	id, err := client.SendInteraction(ctx, req.ChatType, req.ChatID, firstString(req.Interaction.TemplateID, runtime.Channel.Config["interaction_template_id"]), *req.Interaction)
	if err != nil {
		return nil, err
	}
	return &sdk.SendResult{Platform: Platform, AccountUUID: account.UUID, MessageID: id}, nil
}
func handleDingtalkInteraction(ctx context.Context, runtime sdk.Runtime, account sdk.ChannelAccount, frame *dtpayload.DataFrame, out *sdk.StreamFrameResult) (*dtpayload.DataFrameResponse, error) {
	var event struct {
		CorpID     string `json:"corpId"`
		SpaceType  string `json:"spaceType"`
		SpaceID    string `json:"spaceId"`
		UserID     string `json:"userId"`
		UserIDType int    `json:"userIdType"`
		OutTrackID string `json:"outTrackId"`
		Content    string `json:"content"`
	}
	if err := json.Unmarshal([]byte(frame.Data), &event); err != nil {
		return nil, err
	}
	if corp := stringValue(account.Credential["chatbot_corp_id"]); corp != "" && corp != event.CorpID {
		return nil, fmt.Errorf("dingtalk callback corporation mismatch")
	}
	if event.UserIDType != 1 {
		return nil, fmt.Errorf("dingtalk card requires staff user identity")
	}
	var content struct {
		CardPrivateData struct {
			Params map[string]string `json:"params"`
		} `json:"cardPrivateData"`
	}
	if err := json.Unmarshal([]byte(event.Content), &content); err != nil {
		return nil, err
	}
	action, err := sdk.ParseInteractionToken(content.CardPrivateData.Params["beak_action"])
	if err != nil {
		return nil, err
	}
	if event.OutTrackID != action.RequestID+"_"+strconv.FormatInt(action.Revision, 36) {
		return nil, fmt.Errorf("dingtalk callback card identity mismatch")
	}
	switch event.SpaceType {
	case "im":
		// The callback uses conversationId even for direct messages; Beak uses
		// staff user IDs for direct chat routing. Only resolve aliases learned
		// from authenticated incoming messages for this channel account.
		action.ChatID = event.SpaceID
		state := sdkAccountToState(account)
		if latest, ok, err := loadAccountState(ctx, runtime.AccountStore, account); err != nil {
			return nil, err
		} else if ok {
			state = *latest
		}
		if conversation, ok := state.Conversations[event.SpaceID]; ok && time.Since(conversation.UpdatedAt) <= 7*24*time.Hour {
			if conversation.ChatType == sdk.ChatTypeDirect {
				if conversation.ChatID != event.UserID {
					return nil, fmt.Errorf("dingtalk direct card actor mismatch")
				}
				action.ChatID = conversation.ChatID
			}
		}
	case "IM_GROUP", "IM_ROBOT":
		action.ChatID = strings.TrimPrefix(event.SpaceID, "dtv1.card//"+event.SpaceType+".")
	default:
		return nil, fmt.Errorf("dingtalk unsupported callback space")
	}
	action.ActorID = event.UserID
	action.EventID = frame.GetMessageId()
	action.MessageID = event.OutTrackID
	result, err := sdk.DispatchInteraction(ctx, runtime, action)
	if err != nil {
		var access interface{ InteractionErrorCode() string }
		if errors.As(err, &access) && access.InteractionErrorCode() == "actor_not_requester" {
			// Existing published templates do not have a private feedback field.
			// Send the notice only to the verified clicking member, preserving the
			// shared pending card and the requester's ability to decide.
			_, sendErr := (Connector{}).Send(ctx, runtime, sdk.OutboundMessage{
				AccountUUID: account.UUID, ChatType: sdk.ChatTypeDirect, ChatID: action.ActorID,
				Text: "只有本次任务的发起人可以审批，请联系发起人处理。",
			})
			if sendErr != nil {
				return nil, fmt.Errorf("send private interaction feedback: %w", sendErr)
			}
			out.EventResult = &sdk.StreamEventResult{Type: "interaction"}
			return dtpayload.NewSuccessDataFrameResponse(), nil
		}
		return nil, err
	}
	response := dtpayload.NewSuccessDataFrameResponse()
	if result != nil && result.Card != nil {
		data, err := dingtalk.InteractionData(*result.Card)
		if err != nil {
			return nil, err
		}
		body, err := json.Marshal(map[string]any{"cardData": map[string]any{"cardParamMap": data}})
		if err != nil {
			return nil, err
		}
		response.Data = string(body)
	}
	out.EventResult = &sdk.StreamEventResult{Type: "interaction"}
	return response, nil
}
