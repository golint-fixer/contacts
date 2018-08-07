// Definition of the structures and SQL interaction functions
package models

import (
	"github.com/quorumsco/oauth2/models"
)

// Address represents all the components of a contact's address
// type Action struct {
// 	ID uint `gorm:"primary_key" json:"id"`

// 	Name string `json:"name"`
// 	//ActionID uint          `json:"action_id"`
// 	TypeData string        `json:"type_data"`
// 	Data     string        `json:"data"`
// 	Pitch    string        `json:"pitch"`
// 	Status   string        `json:"status"` //statusdata
// 	Users    []models.User `gorm:"ForeignKey:ID"`
// 	Facts    []Fact        `gorm:"ForeignKey:ID"`
// 	//UserID   int64         `json:"user_id"`
// 	GroupID uint `json:"group_id"`
// }

// Contact represents all the components of a contact
type Fact struct {
	ID        uint    `gorm:"primary_key" json:"id"`
	GroupID   uint    `json:"group_id"`
	Date      string  `json:"-"`
	Type      string  `json:"type"`
	Status    string  `json:"status"`
	Contact   Contact `gorm:"save_associations:false" json:"contact"`
	ContactID uint    `json:"contact_id"`
	ActionID  uint    `json:"action_id"`
}

//To represent a GeoPolygon
type Point struct {
	Lat float64 `json:"lat"`
	Lng float64 `json:"lng"`
}

// Contact represents all the components of a contact
type FactsJson struct {
	Name     string `json:"name"`
	TypeData string `json:"type_data"`
	Pitch    string `json:"pitch"`
	//Points   []Point       `json:"points"`
	Filter  string             `json:"filter"`
	Status  string             `json:"status"`
	GroupID uint               `json:"group_id"`
	Search  SearchArgs         `json:"search"`
	Users   []models.User      `json:"users"`
	UsersID []models.UserLight `json:"usersid"`
}

// FactArgs is used in the RPC communications between the gateway and Contacts
type FactArgs struct {
	Fact *Fact
}

// FactReply is used in the RPC communications between the gateway and Contacts
type FactReply struct {
	Fact  *Fact
	Facts []Fact
}
