// Definition of the structures and SQL interaction functions
package models

import (
	"errors"
	"github.com/jinzhu/gorm"
	"strconv"
	"github.com/quorumsco/logs"
)

// ContactSQL contains a Gorm client and the contact and gorm related methods
type ContactSQL struct {
	DB *gorm.DB
}

// Save inserts a new contact into the database
func (s *ContactSQL) Save(c *Contact, args ContactArgs) error {
	if c == nil {
		return errors.New("save: contact is nil")
	}

	c.GroupID = args.Contact.GroupID
	if c.ID == 0 {
		err := s.DB.Create(c).Error
		s.DB.Last(c)
		return err
	}

	// pour les formulaires à détruire lorsque le champ "Data" est mis à la valeur "ToDelete"
	for i := len(c.Formdatas) - 1; i >= 0; i-- {
    formdata := c.Formdatas[i]

    // Condition to decide if current element has to be deleted:
    if (formdata.Data=="ToDelete"){
			if (formdata.ID>0){
				if err := s.DB.Delete(c.Formdatas[i]).Error; err != nil {
					return err
				}else{
					s := c.Formdatas
			    s = append(s[:i], s[i+1:]...)
			    c.Formdatas = s
				}
		   }else{
			 logs.Error("ENTER TO DELETE WITHOUT ID- contacter le support")
			 return errors.New("ENTER TO DELETE - contacter le support")
		 }
    }
	}
	return s.DB.Where("group_id = ?", args.Contact.GroupID).Save(c).Error
}

// Delete removes a contact from the database
func (s *ContactSQL) Delete(c *Contact, args ContactArgs) error {
	if c == nil {
		return errors.New("delete: contact is nil")
	}

	return s.DB.Where("group_id = ?", args.Contact.GroupID).Delete(c).Error
}

// First returns a contact from the database using his ID
func (s *ContactSQL) First(args ContactArgs) (*Contact, error) {
	var c Contact

	if err := s.DB.Where(args.Contact).Preload("Address").Preload("Formdatas").First(&c).Error; err != nil {
		if s.DB.Where(args.Contact).First(&c).RecordNotFound() {
			return nil, nil
		}
		return nil, err
	}
	// ancienne méthode permettant d'ajouter l'address au contact. Maintenant c'est fait avec Preload

	// if err := s.DB.Where(c.AddressID).First(&c.Address).Error; err != nil {
	// 	if s.DB.Where(c.AddressID).First(&c.Address).RecordNotFound() {
	// 		return nil, nil
	// 	}
	// 	return nil, err
	// }

	return &c, nil
}

// Find returns all the contacts with a given groupID from the database
func (s *ContactSQL) Find(args ContactArgs) ([]Contact, error) {
	var contacts []Contact

	err := s.DB.Where("group_id = ?", args.Contact.GroupID).Limit(1000).Find(&contacts).Error
	if err != nil {
		return nil, err
	}

	return contacts, nil
}

// FindByMission returns all the contacts from in a mission from the database
func (s *ContactSQL) FindByMission(m *Mission, args ContactArgs) ([]Contact, error) {
	var contacts []Contact
	err := s.DB.Model(m).Related(&contacts, "Contacts").Error

	return contacts, err
}
