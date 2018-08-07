// Definition of the structures and SQL interaction functions
package models

import (
	"github.com/quorumsco/oauth2/models"
)

// Address represents all the components of a contact's address
type Action struct {
	ID uint `gorm:"primary_key" json:"id"`

	Name string `json:"name"`
	//ActionID uint          `json:"action_id"`
	TypeData string `json:"type_data"`
	//Data     string        `json:"data"`
	Pitch   string             `json:"pitch"`
	Status  string             `json:"status"` //statusdata
	Users   []models.User      `gorm:"save_associations:false;"`
	Facts   []Fact             `json:"facts,omitempty"`
	UserID  []models.UserLight `json:"usersid;many2many:user_actions;"`
	GroupID uint               `json:"group_id"`
}

// Contact represents all the components of a contact
type ActionsJson struct {
	Name     string             `json:"name"`
	TypeData string             `json:"type_data"`
	Pitch    string             `json:"pitch"`
	Status   string             `json:"status"`
	GroupID  uint               `json:"group_id"`
	Users    []models.User      `json:"users"`
	UserID   []models.UserLight `json:"usersid"`
	Facts    []Fact             `json:"facts"`
}

// FactArgs is used in the RPC communications between the gateway and Contacts
type ActionArgs struct {
	Action *Action
}

// FactReply is used in the RPC communications between the gateway and Contacts
type ActionReply struct {
	Action  *Action
	Actions []Action
}
