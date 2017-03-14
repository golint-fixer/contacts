// Definition of the structures and SQL interaction functions
package models

import (
	"time"

	// "github.com/asaskevich/govalidator"
)

// Address represents all the components of a contact's address
type Address struct {
	ID uint `json:"id"`

	HouseNumber    string `json:"housenumber,omitempty"`
	Street         string `json:"street,omitempty"`
	PostalCode     string `json:"postalcode,omitempty"`
	CityCode       string `json:"citycode,omitempty"`
	City           string `json:"city,omitempty"`
	County         string // Département
	State          string // Région
	Country        string
	Addition       string // Complément d'adresse
	PollingStation string // Code bureau de vote

	Latitude  string `json:"latitude,omitempty"`
	Longitude string `json:"longitude,omitempty"`
	Location  string `json:"location,omitempty"` // as "lat,lon" (for elasticsearch)
}

// Contact represents all the components of a contact
type Contact struct {
	ID           uint       `gorm:"primary_key" json:"id"`
	Firstname    string     `sql:"not null" json:"firstname"`
	Surname      string     `json:"surname"`
	MarriedName  *string    `db:"married_name" json:"married_name,omitempty"`
	Gender       *string    `json:"gender,omitempty"`
	Birthdate    *time.Time `json:"birthdate,omitempty"`
	AgeCategory	 uint    		`json:"age_category"`
	BirthDept    *string    `json:"birthdept"`
	BirthCity    *string    `json:"birthcity"`
	BirthCountry *string    `json:"birthcountry"`
	Mail         *string    `json:"mail"`
	Phone        *string    `json:"phone,omitempty"`
	Mobile       *string    `json:"mobile,omitempty"`
	Address      Address    `json:"address,omitempty"`
	AddressID    uint       `json:"-" db:"address_id"`

	LastChange    *time.Time `json:"lastchange,omitempty"`
	LastChangeUserID    uint `json:"lastchangeuserid,omitempty"`

	GroupID uint `sql:"not null" db:"group_id" json:"group_id"`

	Notes     []Note     `json:"notes,omitempty"`
	Tags      []Tag      `json:"tags,omitempty" gorm:"many2many:contact_tags;"`
	Formdatas []Formdata `json:"formdatas,omitempty"`
}

// ContactArgs is used in the RPC communications between the gateway and Contacts
type ContactArgs struct {
	MissionID uint
	Contact   *Contact
}

// ContactReply is used in the RPC communications between the gateway and Contacts
type ContactReply struct {
	Contact  *Contact
	Contacts []Contact
}

type Geometry struct {
	Type        string
	Coordinates []float64
}

type Feature struct {
	Geometry Geometry
}

type Filter struct {
	Citycode string
}

type Ban struct {
	Filters     Filter
	Features    []Feature
	Attribution string
	Licence     string
	Limit       uint
	Query       string
	Type        string
	Version     string
}

// Validate checks if the contact is valid
func (c *Contact) Validate() map[string]string {
	var errs = make(map[string]string)

	// if c.Firstname == "" {
	// 	errs["firstname"] = "is required"
	// }

	// if c.Surname == "" {
	// 	errs["surname"] = "is required"
	// }

	// if c.Mail != nil && !govalidator.IsEmail(*c.Mail) {
	// 	errs["mail"] = "is not valid"
	// }

	return errs
}
