// Definition of the structures and SQL interaction functions
package models

import "time"

// Formdata represents the components of a note
type Formdata struct {
	ID        uint       `gorm:"primary_key" db:"id" json:"id"`
	CreatedAt time.Time  `json:"omitempty"`
	UpdatedAt time.Time  `json:"omitempty"`
	DeletedAt *time.Time `json:"omitempty"`
	Data      string     `db:"data" json:"data"`
	Date      *time.Time `db:"date" json:"date"`

	GroupID     uint `db:"group_id" json:"group_id"`
	ContactID   uint `db:"contact_id" json:"contact_id"`
	FormID      uint `db:"form_id" json:"form_id"`
	Form_ref_id uint `db:"form_ref_id" json:"form_ref_id"`
}

// FormdataArgs is used in the RPC communications between the gateway and Contacts
type FormdataArgs struct {
	GroupID   uint
	ContactID uint
	FactID    uint
	FormID    uint
	Formdata  *Formdata
}

// FormdataReply is used in the RPC communications between the gateway and Contacts
type FormdataReply struct {
	Formdata  *Formdata
	Formdatas []Formdata
}
