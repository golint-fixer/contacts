// Definition of the structures and SQL interaction functions
package models

import "time"

// Formdata represents the components of a note
type Formdata struct {
	ID   uint       `db:"id" json:"id"`
	Data string     `db:"data" json:"data"`
	Date *time.Time `db:"date" json:"date"`

	GroupID     uint `db:"group_id" json:"group_id"`
	ContactID   uint `db:"contact_id" json:"contact_id"`
	Form_ref_id uint `db:"Form_ref_id" json:"Form_ref_id"`
}

// FormdataArgs is used in the RPC communications between the gateway and Contacts
type FormdataArgs struct {
	GroupID   uint
	ContactID uint
	Formdata  *Formdata
}

// FormdataReply is used in the RPC communications between the gateway and Contacts
type FormdataReply struct {
	Formdata  *Formdata
	Formdatas []Formdata
}
