// Definition of the structures and SQL interaction functions
package models

import "github.com/jinzhu/gorm"

// TeamDS implements the TeamSQL methods
type TeamDS interface {
	Save(*Team) error
	Delete(*Team) error
	First(*Team, uint) error
	FirstByGroup(*Team, string) error
	Find() ([]Team, error)
}

// TeamSQL contains a Gorm client and the team and gorm related methods
type TeamSQL struct {
	DB *gorm.DB
}

// Teamstore returns a TeamDS implementing CRUD methods for the teams and containing a gorm client
func TeamStore(db *gorm.DB) TeamDS {
	return &TeamSQL{DB: db}
}

// Save inserts a new team into the database
func (s *TeamSQL) Save(g *Team) error {
	if g.ID == 0 {
		err := s.DB.Create(g).Error

		return err
	}

	err := s.DB.Save(g).Error

	return err
}

// Delete removes a team from the database
func (s *TeamSQL) Delete(g *Team) error {
	err := s.DB.Delete(g).Error

	return err
}

// First returns a team from the database using it's ID
func (s *TeamSQL) First(g *Team, ID uint) error {
	err := s.DB.Where("ID = ?", ID).Find(g).Error

	return err
}

// First returns a team from the database using it's ID
func (s *TeamSQL) FirstByGroup(g *Team, Group_id string) error {
	err := s.DB.Where("Group_id = ?", Group_id).Find(g).Error

	return err
}

// First returns every team from the database
func (s *TeamSQL) Find() ([]Team, error) {
	var teams []Team
	err := s.DB.Find(&teams).Error
	return teams, err
}
