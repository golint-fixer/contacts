// Definition of the structures and SQL interaction functions
package models

import "github.com/jinzhu/gorm"

// GroupDS implements the GroupSQL methods
type GroupDS interface {
	Save(*Group) error
	Delete(*Group) error
	First(*Group, uint) error
	FirstByCodeCause(*Group, string) error
	Find() ([]Group, error)

}

// GroupSQL contains a Gorm client and the group and gorm related methods
type GroupSQL struct {
	DB *gorm.DB
}

// Groupstore returns a GroupDS implementing CRUD methods for the groups and containing a gorm client
func GroupStore(db *gorm.DB) GroupDS {
	return &GroupSQL{DB: db}
}

// Save inserts a new group into the database
func (s *GroupSQL) Save(g *Group) error {
	if g.ID == 0 {
		err := s.DB.Create(g).Error

		return err
	}

	err := s.DB.Save(g).Error

	return err
}

// Delete removes a group from the database
func (s *GroupSQL) Delete(g *Group) error {
	err := s.DB.Delete(g).Error

	return err
}

// First returns a group from the database using it's ID
func (s *GroupSQL) First(g *Group, ID uint) error {
	err := s.DB.Where("ID = ?", ID).Find(g).Error

	return err
}

// First returns a group from the database using it's ID
func (s *GroupSQL) FirstByCodeCause(g *Group, Code_cause string) error {
	err := s.DB.Where("Code_cause = ?", Code_cause).Find(g).Error

	return err
}

// First returns every group from the database
func (s *GroupSQL) Find() ([]Group, error) {
	var groups []Group
	err := s.DB.Find(&groups).Error
	return groups, err
}
