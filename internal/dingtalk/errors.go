package dingtalk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
)

var (
	ErrCredentialRejected = errors.New("dingtalk credential rejected")
	ErrTransientFailure   = errors.New("dingtalk transient platform failure")
)

type HTTPError struct {
	StatusCode int
	Method     string
	Target     string
	Body       string
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("%s %s failed: status=%d body=%s", e.Method, e.Target, e.StatusCode, e.Body)
}

type transportError struct {
	message string
	cause   error
}

func (e *transportError) Error() string { return e.message }
func (e *transportError) Unwrap() error { return e.cause }

func credentialRejected(message string) error {
	return fmt.Errorf("%w: %s", ErrCredentialRejected, message)
}

func transientFailure(message string) error {
	return fmt.Errorf("%w: %s", ErrTransientFailure, message)
}

func credentialResponseRejected(code, message string) bool {
	normalized := strings.NewReplacer("_", "", "-", "", " ", "").Replace(strings.ToLower(strings.TrimSpace(code)))
	switch normalized {
	case "invalidappkey", "invalidappsecret", "appkeynotfound", "invalidclientid", "invalidclientsecret", "unauthorized", "forbidden":
		return true
	}
	message = strings.ToLower(strings.TrimSpace(message))
	return (strings.Contains(message, "app secret") || strings.Contains(message, "appkey") || strings.Contains(message, "app key")) &&
		(strings.Contains(message, "invalid") || strings.Contains(message, "incorrect"))
}

func IsCredentialRejected(err error) bool {
	if errors.Is(err, ErrCredentialRejected) {
		return true
	}
	var httpErr *HTTPError
	if !errors.As(err, &httpErr) {
		return false
	}
	if httpErr.StatusCode == http.StatusUnauthorized || httpErr.StatusCode == http.StatusForbidden {
		return true
	}
	if httpErr.StatusCode != http.StatusBadRequest {
		return false
	}
	var payload struct {
		Code         string `json:"code"`
		Message      string `json:"message"`
		ErrorCode    string `json:"errorCode"`
		ErrorMessage string `json:"errorMessage"`
	}
	if json.Unmarshal([]byte(httpErr.Body), &payload) != nil {
		return false
	}
	return credentialResponseRejected(firstNonEmptyError(payload.Code, payload.ErrorCode), firstNonEmptyError(payload.Message, payload.ErrorMessage))
}

func firstNonEmptyError(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func IsRetryableError(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	if errors.Is(err, ErrTransientFailure) {
		return true
	}
	var transportErr *transportError
	if errors.As(err, &transportErr) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	var httpErr *HTTPError
	return errors.As(err, &httpErr) && (httpErr.StatusCode == http.StatusRequestTimeout || httpErr.StatusCode == http.StatusTooManyRequests || httpErr.StatusCode >= http.StatusInternalServerError)
}
