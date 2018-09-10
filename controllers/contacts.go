// Bundle of functions managing the CRUD and the elasticsearch engine
package controllers

import (
	"github.com/jinzhu/gorm"
	"github.com/quorumsco/contacts/models"
	"github.com/quorumsco/logs"
)

// Contact contains the contact related methods and a gorm client
type Contact struct {
	DB *gorm.DB
}

// RetrieveCollection calls the ContactSQL Find method and returns the results via RPC
func (t *Contact) RetrieveCollection(args models.ContactArgs, reply *models.ContactReply) error {
	var (
		contactStore = models.ContactStore(t.DB)
		err          error
	)

	if reply.Contacts, err = contactStore.Find(args); err != nil {
		logs.Error(err)
		return err
	}

	return nil
}

// Retrieve calls the ContactSQL First method and returns the results via RPC
func (t *Contact) Retrieve(args models.ContactArgs, reply *models.ContactReply) error {
	var (
		contactStore = models.ContactStore(t.DB)
		err          error
	)

	if reply.Contact, err = contactStore.First(args); err != nil {
		logs.Error(err)
		return err
	}

	return nil
}

// Update calls the ContactSQL Update method and returns the results via RPC
func (t *Contact) Update(args models.ContactArgs, reply *models.ContactReply) error {
	var (
		contactStore = models.ContactStore(t.DB)
		err          error
	)

	if err = contactStore.Save(args.Contact, args); err != nil {
		logs.Error(err)
		return err
	}

	args.Contact.Address = models.Address{}
	args.Contact.Formdatas = nil

	if reply.Contact, err = contactStore.First(args); err != nil {
		return err
	}

	return nil
}

// Create calls the ContactSQL Create method and returns the results via RPC
func (t *Contact) Create(args models.ContactArgs, reply *models.ContactReply) error {
	var (
		contactStore = models.ContactStore(t.DB)
		err          error
	)

	if err = contactStore.Save(args.Contact, args); err != nil {
		logs.Error(err)
		return err
	}

	args.Contact.Address = models.Address{}
	args.Contact.Formdatas = nil

	//reply.Contact = args.Contact
	if reply.Contact, err = contactStore.First(args); err != nil {
		return err
	}

	return nil
}

// Delete calls the ContactSQL Delete method and returns the results via RPC
func (t *Contact) Delete(args models.ContactArgs, reply *models.ContactReply) error {
	var (
		contactStore = models.ContactStore(t.DB)
		err          error
	)

	if err = contactStore.Delete(args.Contact, args); err != nil {
		logs.Debug(err)
		return err
	}

	return nil
}
