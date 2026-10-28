package beakdingtalk

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/GuanceCloud/beak-agent-channel-dingtalk/internal/dingtalk"
	"github.com/GuanceCloud/beak-agent-channel-dingtalk/sdk"
)

type templateText struct {
	Content string `json:"content"`
}

type templateComponent struct {
	ID            string `json:"id"`
	ComponentName string `json:"componentName"`
	Props         struct {
		Text   templateText `json:"text"`
		Status struct {
			Type      string `json:"type"`
			ValueType string `json:"valueType"`
			Value     string `json:"value"`
		} `json:"status"`
		ActionType       string       `json:"actionType"`
		ActionID         templateText `json:"actionId"`
		EnableClickEvent bool         `json:"enableClickEvent"`
		Params           []struct {
			Name         string `json:"name"`
			Type         string `json:"type"`
			Variable     string `json:"variable"`
			VariableType string `json:"variableType"`
		} `json:"params"`
		Visible struct {
			ValueType string `json:"valueType"`
			Condition struct {
				Op         string `json:"op"`
				Conditions []struct {
					Op           string `json:"op"`
					Type         string `json:"type"`
					Variable     string `json:"variable"`
					VariableType string `json:"variableType"`
					ValueType    string `json:"valueType"`
					Value        string `json:"value"`
				} `json:"conditions"`
			} `json:"condition"`
		} `json:"visible"`
	} `json:"props"`
	Children []templateComponent `json:"children"`
}

func TestDingtalkApprovalTemplateContract(t *testing.T) {
	t.Parallel()
	raw, err := os.ReadFile("templates/beak-approval-card.json")
	if err != nil {
		t.Fatal(err)
	}
	var source struct {
		EditorData string `json:"editorData"`
	}
	if err := json.Unmarshal(raw, &source); err != nil {
		t.Fatal(err)
	}
	var editor struct {
		Schema struct {
			ComponentsTree []templateComponent `json:"componentsTree"`
		} `json:"schema"`
		VariableList []struct {
			Name    string `json:"name"`
			Type    string `json:"type"`
			Private bool   `json:"private"`
		} `json:"variableList"`
	}
	if err := json.Unmarshal([]byte(source.EditorData), &editor); err != nil {
		t.Fatal(err)
	}
	buttons := map[string]templateComponent{}
	var collect func([]templateComponent)
	collect = func(components []templateComponent) {
		for _, component := range components {
			if component.ComponentName == "SingleButton" {
				if _, exists := buttons[component.ID]; exists {
					t.Fatalf("duplicate button %q", component.ID)
				}
				buttons[component.ID] = component
			}
			collect(component.Children)
		}
	}
	collect(editor.Schema.ComponentsTree)
	if len(buttons) != 4 {
		t.Fatalf("got %d buttons, want 4", len(buttons))
	}
	declared := map[string]bool{}
	for _, variable := range editor.VariableList {
		if variable.Type != "string" || variable.Private || declared[variable.Name] {
			t.Fatalf("invalid public variable: %+v", variable)
		}
		declared[variable.Name] = true
	}
	for _, actionID := range []string{"approve", "reject", "revise", "submit"} {
		t.Run(actionID, func(t *testing.T) {
			card := sdk.InteractionCard{RequestID: "approval_template", Revision: 2, Kind: "tool_approval", State: "pending", Title: "操作审批", Summary: "执行 printf approval_template", Actions: []sdk.InteractionButton{{ID: actionID, Label: actionID}}}
			data, err := dingtalk.InteractionData(card)
			if err != nil {
				t.Fatal(err)
			}
			if len(data) != len(declared) {
				t.Fatalf("template declares %d variables, SDK supplies %d", len(declared), len(data))
			}
			for name := range data {
				if !declared[name] {
					t.Fatalf("SDK variable %q is not declared in template", name)
				}
			}
			button, ok := buttons["beak_"+actionID]
			if !ok {
				t.Fatalf("missing button for %q", actionID)
			}
			props := button.Props
			if props.Status.Type != "dynamicSelect" || props.Status.ValueType != "fixed" || props.Status.Value != "normal" || !props.EnableClickEvent {
				t.Fatalf("pending button must be enabled: status=%+v click=%v", props.Status, props.EnableClickEvent)
			}
			if props.Text.Content != "${"+actionID+"_label}" || props.ActionType != "request" || props.ActionID.Content != "beak_"+actionID {
				t.Fatalf("incorrect button label or callback action: %+v", props)
			}
			if len(props.Params) != 1 || props.Params[0].Name != "beak_action" || props.Params[0].Type != "variable" || props.Params[0].Variable != actionID+"_token" || props.Params[0].VariableType != "global" {
				t.Fatalf("incorrect callback token binding: %+v", props.Params)
			}
			action, err := sdk.ParseInteractionToken(data[props.Params[0].Variable])
			if err != nil || action.RequestID != card.RequestID || action.Revision != card.Revision || action.ActionID != actionID {
				t.Fatalf("template callback token cannot round trip: action=%+v err=%v", action, err)
			}
			if !templateButtonVisible(t, button, data) {
				t.Fatal("pending action is hidden")
			}
			for otherID, other := range buttons {
				if otherID != button.ID && templateButtonVisible(t, other, data) {
					t.Fatalf("unavailable action %q is visible", otherID)
				}
			}
			for _, state := range []string{"approved", "rejected", "canceled", "expired"} {
				// Even stale visibility flags must not keep a terminal card actionable.
				data["state"] = state
				if templateButtonVisible(t, button, data) {
					t.Fatalf("button remains visible in terminal state %q", state)
				}
				card.State, card.Actions = state, nil
				terminal, err := dingtalk.InteractionData(card)
				if err != nil {
					t.Fatal(err)
				}
				if terminal[actionID+"_visible"] != "false" || terminal[actionID+"_token"] != "" || templateButtonVisible(t, button, terminal) {
					t.Fatalf("terminal data retains an action in %q: %+v", state, terminal)
				}
			}
		})
	}
}

// Evaluate only the documented equality conditions used by this template.
// This checks the SDK/template contract, not DingTalk's native renderer.
func templateButtonVisible(t *testing.T, button templateComponent, data map[string]string) bool {
	t.Helper()
	visible := button.Props.Visible
	if visible.ValueType != "condition" || visible.Condition.Op != "and" || len(visible.Condition.Conditions) != 2 {
		t.Fatalf("unexpected visibility contract on %q: %+v", button.ID, visible)
	}
	result := true
	for _, condition := range visible.Condition.Conditions {
		if condition.Op != "equal" || condition.Type != "variable" || condition.VariableType != "global" || condition.ValueType != "fixed" {
			t.Fatalf("unsupported visibility condition on %q: %+v", button.ID, condition)
		}
		value, ok := data[condition.Variable]
		if !ok {
			t.Fatalf("undefined visibility variable %q", condition.Variable)
		}
		result = result && value == condition.Value
	}
	return result
}
