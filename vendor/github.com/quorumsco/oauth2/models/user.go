// Definition of the structures and SQL interaction functions
package models

import (
	"github.com/asaskevich/govalidator"
	"time"
)

// User represent a user in the database
type User struct {
	ID             int64      `json:"id"`
	Mail           *string    `sql:"not null;unique" json:"mail"`
	Password       *string    `sql:"not null" json:"password"`
	Firstname      *string    `sql:"not null" json:"firstname"`
	Surname        *string    `sql:"not null" json:"surname"`
	Role           *string    `json:"role"`
	Cause          *string    `sql:"not null" json:"cause"`
	GroupID        uint       `json:"group_id"`
	OldgroupID     uint       `json:"oldgroup_id"`
	Teams          []*Team    `json:"team_ids",gorm:"many2many:teams;"`
	Validationcode *string    `json:"validationcode"`
	Phone          *string    `json:"phone"`
	Address        *string    `json:"address"`
	Created        *time.Time `json:"created,omitempty"`
}

type UserLight struct {
	ID int64 `json:"id"`
}

// UserArgs is used in the RPC communications between the gateway and Users
type UserArgs struct {
	//MissionID uint
	User *User
}

// UserReply is used in the RPC communications between the gateway and Users
type UserReply struct {
	User  *User
	Users []User
	Teams []Team
	Count int
}

// Validate is used to check if the user infos are correct before inserting it into the database
func (u *User) Validate() map[string]string {
	var errs = make(map[string]string)

	switch {
	case u.Mail == nil:
		errs["mail"] = "is required"
	case u.Mail != nil && !govalidator.IsEmail(*u.Mail):
		errs["mail"] = "is not valid"
	case u.Password == nil:
		errs["password"] = "is required"
	}

	return errs
}

func (u *User) ValidateEmail() map[string]string {
	var errs = make(map[string]string)

	switch {
	case u.Mail == nil:
		errs["mail"] = "is required"
	case u.Mail != nil && !govalidator.IsEmail(*u.Mail):
		errs["mail"] = "is not valid"
	}
	return errs
}
