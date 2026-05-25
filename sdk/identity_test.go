package sdk

import "testing"

func TestIdentityHelpers(t *testing.T) {
	if got := ChatSourceID("dingtalk", "account-1", "group", "chat-1"); got != "dingtalk:account-1:group:chat-1" {
		t.Fatalf("source id=%q", got)
	}
	if got := IMPersonParticipantID("dingtalk", "direct", "chat-1", "user-1"); got != "im:dingtalk:direct:chat-1:user:user-1" {
		t.Fatalf("participant=%q", got)
	}
	if got := BridgeParticipantID("dingtalk"); got != "bridge:dingtalk" {
		t.Fatalf("bridge=%q", got)
	}
}
