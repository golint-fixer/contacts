// Definition of the structures and SQL interaction functions
package models

import (
	"errors"

	"github.com/jinzhu/gorm"
)

// FactSQL contains a Gorm client and the fact and gorm related methods
type FactSQL struct {
	DB *gorm.DB
}

// Save inserts a new fact into the database
func (s *FactSQL) Save(f *Fact, args FactArgs) error {
	if f == nil {
		return errors.New("save: fact is nil")
	}

	f.GroupID = args.Fact.GroupID
	if f.ID == 0 {
		err := s.DB.Create(f).Error
		s.DB.Last(f)
		return err
	}

	return s.DB.Where("group_id = ?", args.Fact.GroupID).Save(f).Error
}

// Delete removes a fact from the database
func (s *FactSQL) Delete(f *Fact, args FactArgs) error {
	if f == nil {
		return errors.New("delete: fact is nil")
	}

	return s.DB.Where("group_id = ?", args.Fact.GroupID).Delete(f).Error
}

// First returns a fact from the database using his ID
func (s *FactSQL) First(args FactArgs) (*Fact, error) {
	var f Fact

	if err := s.DB.Where(args.Fact).First(&f).Error; err != nil {
		if s.DB.Where(args.Fact).First(&f).RecordNotFound() {
			return nil, nil
		}
		return nil, err
	}
	if err := s.DB.Where(f.ActionID).First(&f.Action).Error; err != nil {
		if s.DB.Where(f.ActionID).First(&f.Action).RecordNotFound() {
			return nil, err
		}
		return nil, err
	}
	err := s.DB.Where(f.ContactID).First(&f.Contact).Error
	if err != nil && !s.DB.Where(f.ContactID).First(&f.Contact).RecordNotFound() {
		return nil, err
	}

	if err == nil {
		if err := s.DB.Where(f.Contact.AddressID).First(&f.Contact.Address).Error; err != nil && !s.DB.Where(f.Contact.AddressID).First(&f.Contact.Address).RecordNotFound() {
			return nil, err
		}
	}

	return &f, nil
}

// Find returns all the facts with a given groupID from the database
func (s *FactSQL) Find(args FactArgs) ([]Fact, error) {
	var facts []Fact

	err := s.DB.Where("group_id = ?", args.Fact.GroupID).Find(&facts).Error
	if err != nil {
		return nil, err
	}

	return facts, nil
}
