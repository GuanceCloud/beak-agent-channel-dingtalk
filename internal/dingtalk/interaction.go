package dingtalk

import (
	"context"
	"fmt"
	"github.com/GuanceCloud/beak-agent-channel-dingtalk/sdk"
	"net/http"
	"strconv"
)

func InteractionData(card sdk.InteractionCard) (map[string]string, error) {
	if err := card.Validate(); err != nil {
		return nil, err
	}
	if len(card.Inputs) > 0 {
		return nil, fmt.Errorf("dingtalk native input requires a supported input template")
	}
	data := map[string]string{"title": card.Title, "summary": card.Summary, "state": card.State, "pending": "false"}
	if card.State == "pending" {
		data["pending"] = "true"
	}
	for _, id := range []string{"approve", "reject", "revise", "submit"} {
		data[id+"_visible"] = "false"
		data[id+"_token"] = ""
		data[id+"_label"] = ""
	}
	for _, button := range card.Actions {
		token, err := card.ButtonToken(button)
		if err != nil {
			return nil, err
		}
		data[button.ID+"_visible"] = "true"
		data[button.ID+"_token"] = token
		data[button.ID+"_label"] = button.Label
	}
	return data, nil
}
func (c *Client) SendInteraction(ctx context.Context, chatType, chatID, templateID string, card sdk.InteractionCard) (string, error) {
	if templateID == "" {
		return "", fmt.Errorf("dingtalk interaction_template_id is required; import and publish the Beak approval template")
	}
	if chatID == "" {
		return "", fmt.Errorf("dingtalk interaction chat identity missing")
	}
	data, err := InteractionData(card)
	if err != nil {
		return "", err
	}
	token, err := c.Token(ctx)
	if err != nil {
		return "", err
	}
	trackID := card.RequestID + "_" + strconv.FormatInt(card.Revision, 36)
	body := map[string]any{"outTrackId": trackID, "cardData": map[string]any{"cardParamMap": data}, "userIdType": 1}
	method, path := http.MethodPost, "/v1.0/card/instances/createAndDeliver"
	if card.MessageID != "" {
		if card.MessageID != trackID {
			return "", fmt.Errorf("dingtalk card identity mismatch")
		}
		method, path = http.MethodPut, "/v1.0/card/instances"
	} else {
		body["cardTemplateId"] = templateID
		body["callbackType"] = "STREAM"
		switch chatType {
		case sdk.ChatTypeGroup:
			body["openSpaceId"] = "dtv1.card//IM_GROUP." + chatID
			body["imGroupOpenSpaceModel"] = map[string]bool{"supportForward": false}
			body["imGroupOpenDeliverModel"] = map[string]string{"robotCode": c.RobotCode}
		case sdk.ChatTypeDirect:
			body["openSpaceId"] = "dtv1.card//IM_ROBOT." + chatID
			body["imRobotOpenSpaceModel"] = map[string]bool{"supportForward": false}
			body["imRobotOpenDeliverModel"] = map[string]string{"spaceType": "IM_ROBOT"}
		default:
			return "", fmt.Errorf("dingtalk unsupported card conversation")
		}
	}
	if method == http.MethodPut {
		// UpdateCard returns a boolean result; CreateAndDeliver returns an object.
		var response struct {
			Success bool  `json:"success"`
			Result  *bool `json:"result"`
		}
		if err := c.doJSON(ctx, method, path, body, &response, withAccessToken(token)); err != nil {
			return "", err
		}
		if !response.Success || response.Result == nil || !*response.Result {
			return "", fmt.Errorf("dingtalk card update was not successful")
		}
	} else {
		var response struct {
			Success bool `json:"success"`
			Result  struct {
				OutTrackID string `json:"outTrackId"`
			} `json:"result"`
		}
		if err := c.doJSON(ctx, method, path, body, &response, withAccessToken(token)); err != nil {
			return "", err
		}
		if !response.Success {
			return "", fmt.Errorf("dingtalk card request was not successful")
		}
	}
	return trackID, nil
}
