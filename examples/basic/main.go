package basic

import (
	beakdingtalk "beak-agent-dingtalk"
	"beak-agent-dingtalk/sdk"
)

func DingTalkConnector() sdk.Connector {
	return beakdingtalk.NewConnector()
}
