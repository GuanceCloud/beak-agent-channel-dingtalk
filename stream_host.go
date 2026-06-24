package beakdingtalk

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/GuanceCloud/beak-agent-channel-dingtalk/sdk"
	dtclient "github.com/open-dingtalk/dingtalk-stream-sdk-go/client"
	dthandler "github.com/open-dingtalk/dingtalk-stream-sdk-go/handler"
	dtpayload "github.com/open-dingtalk/dingtalk-stream-sdk-go/payload"
	dtutils "github.com/open-dingtalk/dingtalk-stream-sdk-go/utils"
)

const dingtalkDefaultPingInterval = 120 * time.Second
const dingtalkDefaultPongTimeout = 5 * time.Second

func (c Connector) ConnectStream(ctx context.Context, _ sdk.Runtime, account sdk.ChannelAccount) (*sdk.StreamConnectResult, error) {
	clientID := strings.TrimSpace(stringValue(account.Credential["client_id"]))
	clientSecret := strings.TrimSpace(stringValue(account.Credential["client_secret"]))
	if clientID == "" {
		return nil, fmt.Errorf("dingtalk stream client_id is required")
	}
	if clientSecret == "" {
		return nil, fmt.Errorf("dingtalk stream client_secret is required")
	}
	endpoint, err := dingtalkConnectionEndpoint(ctx, clientID, clientSecret)
	if err != nil {
		return nil, err
	}
	if endpoint == nil || strings.TrimSpace(endpoint.Endpoint) == "" || strings.TrimSpace(endpoint.Ticket) == "" {
		return nil, fmt.Errorf("dingtalk stream endpoint response is empty")
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	return &sdk.StreamConnectResult{
		URL:             fmt.Sprintf("%s?ticket=%s", endpoint.Endpoint, endpoint.Ticket),
		ReadMessageType: sdk.StreamMessageTypeText,
		PingInterval:    dingtalkDefaultPingInterval,
		PongTimeout:     dingtalkDefaultPongTimeout,
		HealthUpdates: map[string]any{
			sdk.RuntimeHealthKeyStreamConnectionState:      sdk.RuntimeHealthStateConnected,
			sdk.RuntimeHealthKeyStreamConnectedAt:          now,
			sdk.RuntimeHealthKeyStreamLastActivityAt:       now,
			sdk.RuntimeHealthKeyStreamReconnectError:       "",
			sdk.RuntimeHealthKeyStreamReconnectErrorAt:     "",
			sdk.RuntimeHealthKeyStreamLastError:            "",
			sdk.RuntimeHealthKeyStreamLastErrorAt:          "",
			sdk.RuntimeHealthKeyStreamReconnectRequestedAt: "",
		},
	}, nil
}

func (c Connector) BuildStreamPing(context.Context, sdk.StreamPingRequest) (*sdk.StreamFrame, error) {
	return &sdk.StreamFrame{MessageType: sdk.StreamMessageTypePing}, nil
}

func (c Connector) HandleStreamFrame(ctx context.Context, runtime sdk.Runtime, account sdk.ChannelAccount, req sdk.StreamFrameRequest) (*sdk.StreamFrameResult, error) {
	out := &sdk.StreamFrameResult{State: req.State}
	if req.MessageType != 0 && req.MessageType != sdk.StreamMessageTypeText {
		return out, nil
	}
	frame, err := dtpayload.DecodeDataFrame(req.Data)
	if err != nil || frame == nil || frame.Headers == nil {
		return out, err
	}
	out.HealthUpdates = map[string]any{
		sdk.RuntimeHealthKeyStreamLastActivityAt: time.Now().UTC().Format(time.RFC3339Nano),
	}

	response, handleErr := c.handleDingTalkStreamDataFrame(ctx, runtime, account, frame, out)
	if handleErr != nil && response == nil {
		response = dtpayload.NewErrorDataFrameResponse(handleErr)
	}
	if response == nil {
		response = dtpayload.NewSuccessDataFrameResponse()
	}
	if response.GetHeader(dtpayload.DataFrameHeaderKMessageId) == "" {
		response.SetHeader(dtpayload.DataFrameHeaderKMessageId, frame.GetMessageId())
	}
	if response.GetHeader(dtpayload.DataFrameHeaderKContentType) == "" {
		response.SetHeader(dtpayload.DataFrameHeaderKContentType, dtpayload.DataFrameContentTypeKJson)
	}
	body, err := json.Marshal(response)
	if err != nil {
		return out, err
	}
	out.ResponseFrames = append(out.ResponseFrames, sdk.StreamFrame{MessageType: sdk.StreamMessageTypeText, Data: body})
	return out, handleErr
}

func (c Connector) handleDingTalkStreamDataFrame(ctx context.Context, runtime sdk.Runtime, account sdk.ChannelAccount, frame *dtpayload.DataFrame, out *sdk.StreamFrameResult) (*dtpayload.DataFrameResponse, error) {
	switch frame.Type {
	case dtutils.SubscriptionTypeKSystem:
		return c.handleDingTalkStreamSystemFrame(frame, out), nil
	case dtutils.SubscriptionTypeKCallback:
		if frame.GetTopic() != dtpayload.BotMessageCallbackTopic {
			return dtpayload.NewDataFrameResponse(dtpayload.DataFrameResponseStatusCodeKHandlerNotFound), nil
		}
		body, err := dingtalkStreamEventBody(frame)
		if err != nil {
			return nil, err
		}
		result, err := c.HandleEvent(ctx, runtime, account, body)
		if err != nil {
			return nil, err
		}
		out.EventResult = dingtalkStreamEventResult(result)
		return dtpayload.NewSuccessDataFrameResponse(), nil
	default:
		return dtpayload.NewDataFrameResponse(dtpayload.DataFrameResponseStatusCodeKHandlerNotFound), nil
	}
}

func (c Connector) handleDingTalkStreamSystemFrame(frame *dtpayload.DataFrame, out *sdk.StreamFrameResult) *dtpayload.DataFrameResponse {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if out.HealthUpdates == nil {
		out.HealthUpdates = map[string]any{}
	}
	switch frame.GetTopic() {
	case "ping":
		out.HealthUpdates["stream_last_system_event_at"] = now
		out.HealthUpdates["stream_last_system_event_type"] = "ping"
		out.HealthUpdates[sdk.RuntimeHealthKeyStreamLastPingAt] = now
		out.HealthUpdates[sdk.RuntimeHealthKeyStreamLastActivityAt] = now
		response := dtpayload.NewDataFrameAckPong(dingtalkFrameMessageID(frame))
		response.Data = frame.Data
		return response
	case "disconnect":
		out.HealthUpdates["stream_last_system_event_at"] = now
		out.HealthUpdates["stream_last_system_event_type"] = "disconnect"
		out.HealthUpdates[sdk.RuntimeHealthKeyStreamDisconnectedAt] = now
		out.HealthUpdates["stream_disconnect_message_id"] = dingtalkFrameMessageID(frame)
		out.CloseReason = "disconnect"
		return nil
	default:
		return dtpayload.NewDataFrameResponse(dtpayload.DataFrameResponseStatusCodeKHandlerNotFound)
	}
}

func dingtalkConnectionEndpoint(ctx context.Context, clientID, clientSecret string) (*dtpayload.ConnectionEndpointResponse, error) {
	client := dtclient.NewStreamClient(
		dtclient.WithAppCredential(dtclient.NewAppCredentialConfig(clientID, clientSecret)),
		dtclient.WithAutoReconnect(false),
	)
	noop := dthandler.IFrameHandler(func(context.Context, *dtpayload.DataFrame) (*dtpayload.DataFrameResponse, error) {
		return dtpayload.NewSuccessDataFrameResponse(), nil
	})
	client.RegisterRouter(dtutils.SubscriptionTypeKSystem, "ping", noop)
	client.RegisterRouter(dtutils.SubscriptionTypeKSystem, "disconnect", noop)
	client.RegisterRouter(dtutils.SubscriptionTypeKCallback, dtpayload.BotMessageCallbackTopic, noop)
	return client.GetConnectionEndpoint(ctx)
}

func dingtalkFrameMessageID(frame *dtpayload.DataFrame) string {
	if frame == nil {
		return ""
	}
	return strings.TrimSpace(frame.GetMessageId())
}

func dingtalkStreamEventBody(frame *dtpayload.DataFrame) ([]byte, error) {
	if frame == nil {
		return nil, fmt.Errorf("dingtalk stream data frame is required")
	}
	if strings.TrimSpace(frame.Data) == "" {
		return nil, fmt.Errorf("dingtalk stream data frame payload is required")
	}
	messageID := strings.TrimSpace(frame.GetMessageId())
	if messageID == "" {
		return []byte(frame.Data), nil
	}
	return json.Marshal(map[string]any{
		"headers": map[string]string{"messageId": messageID},
		"data":    frame.Data,
	})
}

func dingtalkStreamEventResult(result *EventResult) *sdk.StreamEventResult {
	if result == nil {
		return nil
	}
	return &sdk.StreamEventResult{
		Type:        result.Type,
		Ignored:     result.Ignored,
		Reason:      result.Reason,
		SessionUUID: result.SessionUUID,
		MessageUUID: result.MessageUUID,
		Inbound:     result.Inbound,
	}
}
