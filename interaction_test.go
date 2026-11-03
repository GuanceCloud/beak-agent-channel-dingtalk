package beakdingtalk

import (
	"context"
	"encoding/json"
	"github.com/GuanceCloud/beak-agent-channel-dingtalk/internal/locale"
	"github.com/GuanceCloud/beak-agent-channel-dingtalk/sdk"
	"net/http"
	"net/http/httptest"
	"testing"
)

type cardGateway struct {
	fakeSDKGateway
	actions []sdk.InteractionAction
}

type deniedCardGateway struct {
	fakeSDKGateway
	language string
}
type requesterDenied struct{ language string }

func (e requesterDenied) InteractionLanguage() string { return e.language }
func (requesterDenied) Error() string                 { return "denied" }
func (requesterDenied) InteractionErrorCode() string  { return "actor_not_requester" }
func (g *deniedCardGateway) HandleInteraction(context.Context, sdk.InteractionAction) (*sdk.InteractionResult, error) {
	return nil, requesterDenied{language: g.language}
}

func TestDingtalkNonRequesterGetsPrivateCardFeedbackWithoutSendingMessages(t *testing.T) {
	for _, language := range []string{"", "zh", "en"} {
		for _, decision := range []string{"approve", "reject"} {
			t.Run(language+"/"+decision, func(t *testing.T) {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					t.Errorf("denied callback must not send HTTP requests: %s", r.URL.Path)
					w.WriteHeader(http.StatusInternalServerError)
				}))
				defer server.Close()
				account := sdkAccount("account-1", "client-1", "secret", server.URL)
				runtime := sdk.Runtime{Account: account, AccountStore: newFakeSDKAccountStore(), Gateway: &deniedCardGateway{language: language}, HTTPClient: server.Client()}
				content, _ := json.Marshal(map[string]any{"cardPrivateData": map[string]any{"params": map[string]string{"beak_action": "approval_123:2:" + decision}}})
				callback, _ := json.Marshal(map[string]any{"corpId": "corp-1", "spaceType": "IM_GROUP", "spaceId": "dtv1.card//IM_GROUP.cid-group", "userIdType": 1, "userId": "other-member", "outTrackId": "approval_123_2", "content": string(content)})
				frame, _ := json.Marshal(map[string]any{"type": "CALLBACK", "headers": map[string]string{"topic": cardCallbackTopic, "messageId": "event-1"}, "data": string(callback)})
				for attempt := 0; attempt < 2; attempt++ {
					out, err := (Connector{}).HandleStreamFrame(context.Background(), runtime, account, sdk.StreamFrameRequest{MessageType: sdk.StreamMessageTypeText, Data: frame})
					if err != nil || out == nil || len(out.ResponseFrames) != 1 {
						t.Fatalf("callback=%#v err=%v", out, err)
					}
					var response struct {
						Code    int               `json:"code"`
						Data    string            `json:"data"`
						Headers map[string]string `json:"headers"`
					}
					if err := json.Unmarshal(out.ResponseFrames[0].Data, &response); err != nil {
						t.Fatal(err)
					}
					if response.Code != 200 || response.Headers["messageId"] != "event-1" {
						t.Fatalf("wrong callback ACK: %+v", response)
					}
					var body map[string]json.RawMessage
					if err := json.Unmarshal([]byte(response.Data), &body); err != nil {
						t.Fatal(err)
					}
					if len(body) != 1 || body["response"] == nil {
						t.Fatalf("missing official card callback response envelope: %s", response.Data)
					}
					var cardResponse map[string]json.RawMessage
					if err := json.Unmarshal(body["response"], &cardResponse); err != nil {
						t.Fatal(err)
					}
					if len(cardResponse) != 1 || cardResponse["userPrivateData"] == nil {
						t.Fatalf("must update only the clicking user's view: %s", response.Data)
					}
					var private struct {
						Params map[string]string `json:"cardParamMap"`
					}
					if err := json.Unmarshal(cardResponse["userPrivateData"], &private); err != nil {
						t.Fatal(err)
					}
					want := locale.Translator(sdk.ResolveInteractionLanguage("dingtalk", language))("Only the requester can decide this approval. Please contact the requester.")
					if len(private.Params) != 2 || private.Params["approval_feedback"] != want || private.Params["approval_feedback_visible"] != "true" {
						t.Fatalf("wrong private feedback: %+v", private.Params)
					}
				}
			})
		}
	}
}

func (g *cardGateway) HandleInteraction(_ context.Context, a sdk.InteractionAction) (*sdk.InteractionResult, error) {
	g.actions = append(g.actions, a)
	return &sdk.InteractionResult{State: "approved", Card: &sdk.InteractionCard{RequestID: a.RequestID, Revision: a.Revision, Kind: "tool_approval", State: "approved", Title: "已批准"}}, nil
}
func TestDingtalkApprovalTemplateAndStreamIdentity(t *testing.T) {
	var delivered map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/v1.0/oauth2/accessToken" {
			_, _ = w.Write([]byte(`{"accessToken":"token","expireIn":7200}`))
			return
		}
		if r.URL.Path != "/v1.0/card/instances/createAndDeliver" {
			t.Errorf("unexpected endpoint %s", r.URL.Path)
		}
		_ = json.NewDecoder(r.Body).Decode(&delivered)
		_, _ = w.Write([]byte(`{"success":true,"result":{"outTrackId":"approval_123_2"}}`))
	}))
	defer server.Close()
	account := sdkAccount("account-1", "client-1", "secret", server.URL)
	g := &cardGateway{}
	runtime := sdk.Runtime{Account: account, Gateway: g, HTTPClient: server.Client(), Channel: sdk.Channel{Config: map[string]any{"interaction_template_id": "template-1"}}}
	card := &sdk.InteractionCard{RequestID: "approval_123", Revision: 2, Kind: "tool_approval", State: "pending", Title: "审批", Actions: []sdk.InteractionButton{{ID: "approve", Label: "批准"}}}
	result, err := (Connector{}).Send(context.Background(), runtime, sdk.OutboundMessage{AccountUUID: account.UUID, ChatID: "cid-group", ChatType: sdk.ChatTypeGroup, Interaction: card})
	if err != nil {
		t.Fatal(err)
	}
	if result.MessageID != "approval_123_2" || delivered["cardTemplateId"] != "template-1" || delivered["openSpaceId"] != "dtv1.card//IM_GROUP.cid-group" {
		t.Fatalf("delivery=%+v result=%+v", delivered, result)
	}
	token, _ := card.ButtonToken(card.Actions[0])
	content, _ := json.Marshal(map[string]any{"cardPrivateData": map[string]any{"params": map[string]string{"beak_action": token}}})
	callback, _ := json.Marshal(map[string]any{"corpId": "corp-1", "spaceType": "im", "spaceId": "cid-group", "userIdType": 1, "userId": "staff-1", "outTrackId": "approval_123_2", "content": string(content)})
	frame, _ := json.Marshal(map[string]any{"type": "CALLBACK", "headers": map[string]string{"topic": "/v1.0/card/instances/callback", "messageId": "event-1"}, "data": string(callback)})
	out, err := (Connector{}).HandleStreamFrame(context.Background(), runtime, account, sdk.StreamFrameRequest{MessageType: sdk.StreamMessageTypeText, Data: frame})
	if err != nil {
		t.Fatal(err)
	}
	if len(g.actions) != 1 || g.actions[0].ActorID != "staff-1" || g.actions[0].ChatID != "cid-group" || len(out.ResponseFrames) != 1 {
		t.Fatalf("callback=%+v actions=%+v", out, g.actions)
	}
}

func TestDingtalkDirectCallbackUsesAuthenticatedConversation(t *testing.T) {
	ctx := context.Background()
	account := sdkAccount("account-1", "client-1", "secret", "")
	g := &cardGateway{}
	store := newFakeSDKAccountStore()
	runtime := sdk.Runtime{Account: account, Gateway: g, AccountStore: store}
	connector := newTestEventConnector(t)
	_, err := connector.HandleEvent(ctx, runtime, account, []byte(`{"conversationType":"1","conversationId":"cid-direct","senderStaffId":"staff-1","msgId":"msg-direct","msgtype":"text","text":{"content":"test"},"robotCode":"robot-1"}`))
	if err != nil {
		t.Fatal(err)
	}
	// Simulate persistence and a stale account snapshot on another replica.
	raw, _ := json.Marshal(store.state(account.UUID))
	var persisted map[string]any
	if err := json.Unmarshal(raw, &persisted); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveChannelAccountState(ctx, account.UUID, persisted); err != nil {
		t.Fatal(err)
	}
	state := sdkAccountToState(sdk.ChannelAccount{UUID: account.UUID, State: persisted})
	if state.Conversations["cid-direct"].ChatID != "staff-1" {
		t.Fatal("direct conversation alias was not persisted")
	}
	for _, tc := range []struct {
		name, actor, space, accountID, wantChat string
		wantError                               bool
	}{
		{"approve in direct", "staff-1", "cid-direct", "account-1", "staff-1", false},
		{"other member blocked", "staff-2", "cid-direct", "account-1", "", true},
		{"group stays group", "staff-1", "cid-group", "account-1", "cid-group", false},
		{"another direct does not alias", "staff-1", "cid-other", "account-1", "cid-other", false},
		{"another account does not alias", "staff-1", "cid-direct", "account-2", "cid-direct", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g.actions = nil
			callbackAccount := account
			callbackAccount.UUID = tc.accountID
			content, _ := json.Marshal(map[string]any{"cardPrivateData": map[string]any{"params": map[string]string{"beak_action": "approval_123:2:approve"}}})
			callback, _ := json.Marshal(map[string]any{"corpId": "corp-1", "spaceType": "im", "spaceId": tc.space, "userIdType": 1, "userId": tc.actor, "outTrackId": "approval_123_2", "content": string(content)})
			frame, _ := json.Marshal(map[string]any{"type": "CALLBACK", "headers": map[string]string{"topic": cardCallbackTopic, "messageId": "event-direct"}, "data": string(callback)})
			_, err := (Connector{}).HandleStreamFrame(ctx, runtime, callbackAccount, sdk.StreamFrameRequest{MessageType: sdk.StreamMessageTypeText, Data: frame})
			if tc.wantError {
				if err == nil || len(g.actions) != 0 {
					t.Fatalf("unexpected dispatch: err=%v actions=%+v", err, g.actions)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(g.actions) != 1 || g.actions[0].ChatID != tc.wantChat || g.actions[0].ActorID != tc.actor {
				t.Fatalf("actions=%+v", g.actions)
			}
		})
	}
}
