// Definition of the structures and SQL interaction functions
package models

import "github.com/jinzhu/gorm"

// ActionDS implements the ActionSQL methods
type ActionDS interface {
	Save(*Action, ActionArgs) error
	Delete(*Action, ActionArgs) error
	First(ActionArgs) (*Action, error)
	Find(ActionArgs) ([]Action, error)
}

// Actionstore returns a ActionDS implementing CRUD methods for the facts and containing a gorm client
func ActionStore(db *gorm.DB) ActionDS {
	return &ActionSQL{DB: db}
}
