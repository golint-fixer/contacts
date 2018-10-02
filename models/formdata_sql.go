// Definition of the structures and SQL interaction functions
package models

import (
	"errors"

	"github.com/jinzhu/gorm"
)

// FormdataSQL contains a Gorm client and the formdata and gorm related methods
type FormdataSQL struct {
	DB *gorm.DB
}

// Save inserts a new formdata into the database
func (s *FormdataSQL) Save(n *Formdata, args FormdataArgs) error {
	if n == nil {
		return errors.New("save: formdata is nil")
	}

	n.GroupID = args.Formdata.GroupID
	if n.ID == 0 {
		err := s.DB.Create(n).Error
		s.DB.Last(n)
		return err
	}

	return s.DB.Where("group_id = ?", args.Formdata.GroupID).Save(n).Error
}

// Delete removes a formdata from the database
func (s *FormdataSQL) Delete(n *Formdata, args FormdataArgs) error {
	if n == nil {
		return errors.New("delete: formdata is nil")
	}

	return s.DB.Where("group_id = ?", args.Formdata.GroupID).Delete(n).Error
}

// DeleteAll removes formdata from the database for a dedicated form
func (s *FormdataSQL) DeleteAll(n *Formdata, args FormdataArgs) error {
	if n == nil {
		return errors.New("delete: formdata is nil")
	}

	return s.DB.Where("group_id = ?", args.Formdata.GroupID).Where("contact_id = ?", args.Formdata.ContactID).Where("form_id = ?", args.Formdata.FormID).Delete(n).Error
}

// First returns a formdata from the database usin it's ID
func (s *FormdataSQL) First(args FormdataArgs) (*Formdata, error) {
	var n Formdata

	if err := s.DB.Where(args.Formdata).First(&n).Error; err != nil {
		if s.DB.Where(args.Formdata).First(&n).RecordNotFound() {
			return nil, nil
		}
		return nil, err
	}

	return &n, nil
}

// Find returns all the formdatas containing a given groupID from the database
func (s *FormdataSQL) Find(args FormdataArgs) ([]Formdata, error) {
	var formdatas []Formdata
	if args.FactID > 0 {
		err := s.DB.Where("group_id = ?", args.Formdata.GroupID).Where("fact_id = ?", args.FactID).Find(&formdatas).Error
		if err != nil {
			return nil, err
		}
	} else {
		err := s.DB.Where("group_id = ?", args.Formdata.GroupID).Where("contact_id = ?", args.ContactID).Find(&formdatas).Error
		if err != nil {
			return nil, err
		}
	}

	return formdatas, nil
}
