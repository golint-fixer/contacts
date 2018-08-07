// Definition of the structures and SQL interaction functions
package models

import (
	"time"
	// "github.com/asaskevich/govalidator"
)

// Group represents the components of a group
type Team struct {
	ID            uint       `json:"id"`
	Name          *string    `json:"name,omitempty"`
	GroupID       uint       `json:"group_id"`
	Created       *time.Time `json:"created,omitempty"`
	User_referent *string    `json:"user_referent,omitempty"`
	Users         []*User    `json:"users,omitempty",gorm:"many2many:users;"`
}
