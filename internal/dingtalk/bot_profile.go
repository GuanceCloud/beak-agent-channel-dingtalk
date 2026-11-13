package dingtalk

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
)

func (c *Client) GroupRobotDisplayName(ctx context.Context, token, groupID, robotCode string) (string, error) {
	request := struct {
		OpenConversationID string `json:"openConversationId"`
	}{OpenConversationID: groupID}
	var response struct {
		Robots []struct {
			Code string `json:"robotCode"`
			Name string `json:"name"`
		} `json:"chatbotInstanceVOList"`
	}
	if err := c.doJSON(ctx, http.MethodPost, "/v1.0/robot/groups/robots/query", request, &response, withAccessToken(token)); err != nil {
		return "", err
	}
	for _, robot := range response.Robots {
		if robot.Code == robotCode {
			return strings.TrimSpace(robot.Name), nil
		}
	}
	return "", nil
}

func IsUnknownGroup(err error) bool {
	var response *HTTPError
	if !errors.As(err, &response) || response.StatusCode != http.StatusBadRequest {
		return false
	}
	var body struct {
		Code string `json:"code"`
	}
	return json.Unmarshal([]byte(response.Body), &body) == nil && body.Code == "illegal.openconevrsation.id"
}
