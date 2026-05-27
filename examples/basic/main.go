package basic

import (
	beakdingtalk "github.com/GuanceCloud/beak-agent-channel-dingtalk"
	"github.com/GuanceCloud/beak-agent-channel-dingtalk/sdk"
)

func DingTalkConnector() sdk.Connector {
	return beakdingtalk.NewConnector()
}
