package dingtalk

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/GuanceCloud/beak-agent-channel-dingtalk/sdk"
)

func TestSendInteractionDecodesEndpointResponses(t *testing.T) {
	for _, tc := range []struct {
		name, method, path, body, messageID string
		wantError                           bool
	}{
		{"create", "POST", "/v1.0/card/instances/createAndDeliver", `{"success":true,"result":{"outTrackId":"approval_123_2"}}`, "", false},
		{"update boolean success", "PUT", "/v1.0/card/instances", `{"success":true,"result":true}`, "approval_123_2", false},
		{"update false result", "PUT", "/v1.0/card/instances", `{"success":true,"result":false}`, "approval_123_2", true},
		{"update false success", "PUT", "/v1.0/card/instances", `{"success":false,"result":true}`, "approval_123_2", true},
		{"update missing result", "PUT", "/v1.0/card/instances", `{"success":true}`, "approval_123_2", true},
		{"update unexpected result", "PUT", "/v1.0/card/instances", `{"success":true,"result":{}}`, "approval_123_2", true},
		{"create failed", "POST", "/v1.0/card/instances/createAndDeliver", `{"success":false,"result":{"outTrackId":"approval_123_2"}}`, "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := NewClient("https://api.dingtalk.test", "client", "secret", "robot")
			client.AccessToken = "token"
			client.AccessTokenExpiresAt = time.Now().Add(time.Hour)
			client.HTTPClient = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				if r.Method != tc.method || r.URL.Path != tc.path {
					t.Fatalf("request = %s %s, want %s %s", r.Method, r.URL.Path, tc.method, tc.path)
				}
				return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(tc.body))}, nil
			})}
			card := sdk.InteractionCard{RequestID: "approval_123", Revision: 2, Kind: "tool_approval", State: "approved", MessageID: tc.messageID}
			got, err := client.SendInteraction(context.Background(), sdk.ChatTypeDirect, "staff-1", "template-1", card)
			if (err != nil) != tc.wantError {
				t.Fatalf("SendInteraction error = %v, wantError %v", err, tc.wantError)
			}
			if err == nil && got != "approval_123_2" {
				t.Fatalf("SendInteraction id = %q", got)
			}
		})
	}
}
