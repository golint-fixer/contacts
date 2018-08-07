// Bundle of functions managing the CRUD and the elasticsearch engine
package controllers

import (
	"github.com/jinzhu/gorm"
	"github.com/quorumsco/contacts/models"
	"github.com/quorumsco/logs"
)

// Action contains the action related methods and a gorm client
type Action struct {
	DB *gorm.DB
}

// RetrieveCollection calls the ActionSQL Find method and returns the results via RPC
func (t *Action) RetrieveCollection(args models.ActionArgs, reply *models.ActionReply) error {
	var (
		actionStore = models.ActionStore(t.DB)
		err         error
	)

	if reply.Actions, err = actionStore.Find(args); err != nil {
		logs.Error(err)
		return err
	}

	return nil
}

// Retrieve calls the ActionSQL First method and returns the results via RPC
func (t *Action) Retrieve(args models.ActionArgs, reply *models.ActionReply) error {
	var (
		actionStore = models.ActionStore(t.DB)
		err         error
	)

	if reply.Action, err = actionStore.First(args); err != nil {
		logs.Error(err)
		return err
	}

	return nil
}

// Create calls the ActionSQL Create method and returns the results via RPC
func (t *Action) Create(args models.ActionArgs, reply *models.ActionReply) error {
	var (
		actionStore = models.ActionStore(t.DB)
		err         error
	)
	logs.Debug(*args.Action)
	//args.Action.Contact = models.Contact{}

	if err = actionStore.Save(args.Action, args); err != nil {
		logs.Error(err)
		return err
	}

	ID := args.Action.ID
	args.Action = &models.Action{ID: ID}

	if reply.Action, err = actionStore.First(args); err != nil {
		logs.Error(err)
		return err
	}

	return nil
}

// Delete calls the ActionSQL Delete method and returns the results via RPC
func (t *Action) Delete(args models.ActionArgs, reply *models.ActionReply) error {
	var (
		actionStore = models.ActionStore(t.DB)
		err         error
	)

	if err = actionStore.Delete(args.Action, args); err != nil {
		logs.Debug(err)
		return err
	}

	return nil
}
