// Definition of the structures and SQL interaction functions
package models

import (
	"errors"

	"github.com/jinzhu/gorm"
)

// ActionSQL contains a Gorm client and the action and gorm related methods
type ActionSQL struct {
	DB *gorm.DB
}

// Save inserts a new action into the database
func (s *ActionSQL) Save(f *Action, args ActionArgs) error {
	if f == nil {
		return errors.New("save: action is nil")
	}

	f.GroupID = args.Action.GroupID
	if f.ID == 0 {
		err := s.DB.Create(f).Error
		s.DB.Last(f)
		return err
	}

	return s.DB.Where("group_id = ?", args.Action.GroupID).Save(f).Error
}

// Delete removes a action from the database
func (s *ActionSQL) Delete(f *Action, args ActionArgs) error {
	if f == nil {
		return errors.New("delete: action is nil")
	}

	return s.DB.Where("group_id = ?", args.Action.GroupID).Delete(f).Error
}

// First returns a action from the database using his ID
func (s *ActionSQL) First(args ActionArgs) (*Action, error) {
	var f Action

	if err := s.DB.Where(args.Action).Preload("Facts").First(&f).Error; err != nil {
		if s.DB.Where(args.Action).First(&f).RecordNotFound() {
			return nil, nil
		}
		return nil, err
	}
	// if err := s.DB.Where(f.ActionID).First(&f.Action).Error; err != nil {
	// 	if s.DB.Where(f.ActionID).First(&f.Action).RecordNotFound() {
	// 		return nil, err
	// 	}
	// 	return nil, err
	// }
	// err := s.DB.Where(f.ContactID).First(&f.Contact).Error
	// if err != nil && !s.DB.Where(f.ContactID).First(&f.Contact).RecordNotFound() {
	// 	return nil, err
	// }

	// if err == nil {
	// 	if err := s.DB.Where(f.Contact.AddressID).First(&f.Contact.Address).Error; err != nil && !s.DB.Where(f.Contact.AddressID).First(&f.Contact.Address).RecordNotFound() {
	// 		return nil, err
	// 	}
	// }

	return &f, nil
}

// Find returns all the actions with a given groupID from the database
func (s *ActionSQL) Find(args ActionArgs) ([]Action, error) {
	var actions []Action

	//err := s.DB.Where("group_id = ?", args.Action.GroupID).Find(&actions).Error
	err := s.DB.Where("group_id = ?", args.Action.GroupID).Preload("Facts").Preload("Contacts").Find(&actions).Error

	if err != nil {
		return nil, err
	}

	return actions, nil
}
