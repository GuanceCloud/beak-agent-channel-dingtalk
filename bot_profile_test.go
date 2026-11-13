package beakdingtalk

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/GuanceCloud/beak-agent-channel-dingtalk/sdk"
)

func TestLookupBotDisplayName(t *testing.T) {
	for _, test := range []struct {
		name      string
		chatType  string
		status    int
		response  string
		want      string
		wantCalls int
	}{
		{"matching robot", "group", 200, `{"chatbotInstanceVOList":[{"robotCode":"other","name":"Other"},{"robotCode":"robot","name":"BeakQA会话测试"}]}`, "BeakQA会话测试", 1},
		{"unmatched robot", "group", 200, `{"chatbotInstanceVOList":[{"robotCode":"other","name":"Other"}]}`, "", 1},
		{"missing permission", "group", 400, `{"code":"unauthorized"}`, "", 1},
		{"private only", "private", 200, `{}`, "", 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				var body struct {
					OpenConversationID string `json:"openConversationId"`
				}
				if r.Method != http.MethodPost || r.URL.Path != "/v1.0/robot/groups/robots/query" ||
					r.Header.Get("x-acs-dingtalk-access-token") != "cached-token" ||
					json.NewDecoder(r.Body).Decode(&body) != nil || body.OpenConversationID != "cid+abc==" {
					t.Error("unexpected DingTalk group robot lookup request")
				}
				w.WriteHeader(test.status)
				_, _ = w.Write([]byte(test.response))
			}))
			defer server.Close()
			account := sdk.ChannelAccount{
				UUID: "account", Credential: map[string]any{"client_id": "app", "robot_code": "robot", "base_url": server.URL},
				State: map[string]any{
					"access_token": "cached-token", "access_token_expires_at": time.Now().Add(time.Hour),
					"conversations": map[string]any{"cid+abc==": map[string]any{"chat_type": test.chatType}},
				},
			}
			name, err := (Connector{}).LookupBotDisplayName(context.Background(), sdk.Runtime{HTTPClient: server.Client()}, account)
			if name != test.want || (err != nil) != (test.status >= 400) || calls != test.wantCalls {
				t.Fatalf("name=%q, err=%v, calls=%d; want name=%q, calls=%d", name, err, calls, test.want, test.wantCalls)
			}
		})
	}
}
