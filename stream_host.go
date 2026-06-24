package beakdingtalk

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/GuanceCloud/beak-agent-channel-dingtalk/sdk"
	dtpayload "github.com/open-dingtalk/dingtalk-stream-sdk-go/payload"
	dtutils "github.com/open-dingtalk/dingtalk-stream-sdk-go/utils"
)

const dingtalkDefaultPingInterval = 120 * time.Second
const dingtalkDefaultPongTimeout = 5 * time.Second

func (c Connector) ConnectStream(ctx context.Context, runtime sdk.Runtime, account sdk.ChannelAccount) (*sdk.StreamConnectResult, error) {
	clientID := strings.TrimSpace(stringValue(account.Credential["client_id"]))
	clientSecret := strings.TrimSpace(stringValue(account.Credential["client_secret"]))
	if clientID == "" {
		return nil, fmt.Errorf("dingtalk stream client_id is required")
	}
	if clientSecret == "" {
		return nil, fmt.Errorf("dingtalk stream client_secret is required")
	}
	endpoint, err := dingtalkConnectionEndpoint(ctx, runtime.HTTPClient, baseURLFromCredential(account.Credential), clientID, clientSecret)
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
		if result != nil && !result.Ignored && strings.TrimSpace(result.MessageUUID) != "" {
			now := time.Now().UTC().Format(time.RFC3339Nano)
			out.HealthUpdates[sdk.RuntimeHealthKeyStreamLastEventAt] = now
			out.HealthUpdates[sdk.RuntimeHealthKeyStreamLastActivityAt] = now
		}
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

func dingtalkConnectionEndpoint(ctx context.Context, httpClient *http.Client, baseURL, clientID, clientSecret string) (*dtpayload.ConnectionEndpointResponse, error) {
	requestModel := dtpayload.ConnectionEndpointRequest{
		ClientId:     clientID,
		ClientSecret: clientSecret,
		UserAgent:    "dingtalk-sdk-go/v0.9.1",
		Subscriptions: []*dtpayload.SubscriptionModel{
			{Type: dtutils.SubscriptionTypeKSystem, Topic: "ping"},
			{Type: dtutils.SubscriptionTypeKSystem, Topic: "disconnect"},
			{Type: dtutils.SubscriptionTypeKCallback, Topic: dtpayload.BotMessageCallbackTopic},
		},
	}
	if localIP, err := dtutils.GetFirstLanIP(); err == nil {
		requestModel.LocalIP = localIP
	}
	requestBody, err := json.Marshal(requestModel)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(baseURL) == "" {
		baseURL = dtutils.DefaultOpenApiHost
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(baseURL, "/")+dtutils.GetConnectionEndpointAPIUrl, bytes.NewReader(requestBody))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("dingtalk stream endpoint failed: status=%s body=%s", resp.Status, string(responseBody))
	}
	var endpoint dtpayload.ConnectionEndpointResponse
	if err := json.Unmarshal(responseBody, &endpoint); err != nil {
		return nil, err
	}
	if err := endpoint.Valid(); err != nil {
		return nil, err
	}
	return &endpoint, nil
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
