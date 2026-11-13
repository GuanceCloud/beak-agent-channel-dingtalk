package beakdingtalk

import (
	"context"
	"slices"
	"strings"

	"github.com/GuanceCloud/beak-agent-channel-dingtalk/internal/dingtalk"
	"github.com/GuanceCloud/beak-agent-channel-dingtalk/sdk"
)

// BotProfileConnector provides an optional lookup for the channel account's display name.
type BotProfileConnector interface {
	LookupBotDisplayName(context.Context, sdk.Runtime, sdk.ChannelAccount) (string, error)
}

// LookupBotDisplayName uses recent groups already observed by this account.
// An empty name means that none of the available groups exposed this robot.
func (Connector) LookupBotDisplayName(ctx context.Context, runtime sdk.Runtime, account sdk.ChannelAccount) (string, error) {
	if runtime.AccountStore != nil && account.UUID != "" {
		state, err := runtime.AccountStore.LoadChannelAccountState(ctx, account.UUID)
		if err != nil {
			return "", err
		}
		account.State = state
	}
	state := sdkAccountToState(account)
	groupIDs := make([]string, 0, len(state.Conversations))
	for id, conversation := range state.Conversations {
		if conversation.ChatType == sdk.ChatTypeGroup && strings.TrimSpace(id) != "" {
			groupIDs = append(groupIDs, id)
		}
	}
	if state.RobotCode == "" || len(groupIDs) == 0 {
		return "", nil
	}
	slices.SortFunc(groupIDs, func(a, b string) int {
		if order := state.Conversations[b].UpdatedAt.Compare(state.Conversations[a].UpdatedAt); order != 0 {
			return order
		}
		return strings.Compare(a, b)
	})
	client := clientFromAccount(runtime, account)
	client.AccessToken = state.AccessToken
	client.AccessTokenExpiresAt = state.AccessTokenExpires
	token, err := client.Token(ctx)
	if err != nil {
		return "", err
	}
	for _, groupID := range groupIDs[:min(3, len(groupIDs))] {
		name, err := client.GroupRobotDisplayName(ctx, token, groupID, state.RobotCode)
		if err != nil {
			if dingtalk.IsUnknownGroup(err) {
				continue
			}
			return "", err
		}
		if name != "" {
			return name, nil
		}
	}
	return "", nil
}
