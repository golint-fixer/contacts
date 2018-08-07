// Definition of the structures and SQL interaction functions
package models

import (
	"github.com/jinzhu/gorm"
)

// GroupDS implements the GroupSQL methods
type UserDS interface {
	Save(*User) error
	Delete(*User) error
	First(*User) error
	Update(*User) error
	UpdateGroupIDtoZero(*User) error
	UpdateGroupIDandOldGroupIdtoZero(*User) error
	Find(*UserReply, int, int, string) error
	FindByTeam(*UserReply, int, int, string) error
}

// UserSQL contains a Gorm client and the user and gorm related methods
type UserSQL struct {
	DB *gorm.DB
}

// Userstore returns a UserDS implementing CRUD methods for the users and containing a gorm client
func UserStore(db *gorm.DB) UserDS {
	return &UserSQL{DB: db}
}

// Save inserts a new user into the database
func (s *UserSQL) Save(u *User) error {
	if u.ID == 0 {
		err := s.DB.Create(u).Error

		return err
	}

	err := s.DB.Save(u).Error

	return err
}

// Update user
func (s *UserSQL) Update(u *User) error {
	var err error
	if u.ID != 0 {
		err = s.DB.Table("users").Where("id = ?", u.ID).Updates(u).Error
	} else {
		err = s.DB.Table("users").Where("mail = ?", u.Mail).Updates(u).Error
	}
	return err
}

// Update the group_id to zero
func (s *UserSQL) UpdateGroupIDtoZero(u *User) error {
	err := s.DB.Table("users").Where("mail = ?", u.Mail).Updates(map[string]interface{}{"group_id": 0}).Error
	return err
}

func (s *UserSQL) UpdateGroupIDandOldGroupIdtoZero(u *User) error {
	err := s.DB.Table("users").Where("ID = ?", u.ID).Updates(map[string]interface{}{"group_id": 0, "oldgroup_id": 0}).Error
	return err
}

// Delete removes a user from the database
func (s *UserSQL) Delete(u *User) error {
	err := s.DB.Delete(u).Error

	return err
}

// First return a user from the database using his ID
func (s *UserSQL) First(u *User) error {
	var err error

	if u.Mail != nil && u.Password != nil {
		err = s.DB.Where("mail = ?", u.Mail).Find(u).Error
	} else {
		err = s.DB.Find(u).Error
	}

	return err
}

// func (s *ContactSQL) Find(args ContactArgs) ([]Contact, error) {
// 	var contacts []Contact
//
// 	err := s.DB.Where("group_id = ?", args.Contact.GroupID).Limit(1000).Find(&contacts).Error
// 	if err != nil {
// 		return nil, err
// 	}
//
// 	return contacts, nil
// }

// Find returns every user with a given groupID from the database
func (s *UserSQL) Find(u *UserReply, limit int, offset int, sort string) error {
	var err error

	if u.User.GroupID != 0 {
		err = s.DB.Order("surname "+sort+",firstname "+sort).Where("group_id = ?", u.User.GroupID).Where("not mail  ~ '@quorum.co$'").Offset(offset).Limit(limit).Find(&u.Users).Offset(-1).Limit(-1).Count(&u.Count).Error

	} else {
		err = s.DB.Order("surname " + sort + ",firstname " + sort).Offset(offset).Limit(limit).Find(&u.Users).Offset(-1).Limit(-1).Count(&u.Count).Error

	}
	return err
}

// Find returns every user with a given teamID from the database
func (s *UserSQL) FindByTeam(u *UserReply, limit int, offset int, sort string) error {
	var err error

	if u.User.GroupID != 0 {
		err = s.DB.Order("surname "+sort+",firstname "+sort).Where("team_id = ?", u.User.Teams[0]).Where("not mail  ~ '@quorum.co$'").Offset(offset).Limit(limit).Find(&u.Users).Offset(-1).Limit(-1).Count(&u.Count).Error

	} else {
		err = s.DB.Order("surname " + sort + ",firstname " + sort).Offset(offset).Limit(limit).Find(&u.Users).Offset(-1).Limit(-1).Count(&u.Count).Error

	}
	return err
}
