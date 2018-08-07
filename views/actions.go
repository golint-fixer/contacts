// Views for JSON responses
package views

import "github.com/quorumsco/contacts/models"

// Actions is a type used for JSON request responses
type Actions struct {
	Actions []models.Action `json:"actions"`
}

// Action is a type used for JSON request responses
type Action struct {
	Action *models.Action `json:"action"`
}

type ActionsJson struct {
	ActionsJson models.ActionsJson `json:"actions_json"`
}
