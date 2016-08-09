// Definition of the structures and SQL interaction functions
package models

import "github.com/jinzhu/gorm"

// FormdataDS implements the FormdataSQL methods
type FormdataDS interface {
	Save(*Formdata, FormdataArgs) error
	Delete(*Formdata, FormdataArgs) error
	DeleteAll(*Formdata, FormdataArgs) error
	First(FormdataArgs) (*Formdata, error)
	Find(FormdataArgs) ([]Formdata, error)
}

// Formdatastore returns a FormdataDS implementing CRUD methods for the formdatas and containing a gorm client
func FormdataStore(db *gorm.DB) FormdataDS {
	return &FormdataSQL{DB: db}
}
