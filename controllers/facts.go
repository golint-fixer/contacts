// Bundle of functions managing the CRUD and the elasticsearch engine
package controllers

import (
	"github.com/jinzhu/gorm"
	"github.com/quorumsco/contacts/models"
	"github.com/quorumsco/logs"
)

// Fact contains the fact related methods and a gorm client
type Fact struct {
	DB *gorm.DB
}

// RetrieveCollection calls the FactSQL Find method and returns the results via RPC
func (t *Fact) RetrieveCollection(args models.FactArgs, reply *models.FactReply) error {
	var (
		factStore = models.FactStore(t.DB)
		err       error
	)

	if reply.Facts, err = factStore.Find(args); err != nil {
		logs.Error(err)
		return err
	}

	return nil
}

// Retrieve calls the FactSQL First method and returns the results via RPC
func (t *Fact) Retrieve(args models.FactArgs, reply *models.FactReply) error {
	var (
		factStore = models.FactStore(t.DB)
		err       error
	)

	if reply.Fact, err = factStore.First(args); err != nil {
		logs.Error(err)
		return err
	}

	return nil
}

// Create calls the FactSQL Create method and returns the results via RPC
func (t *Fact) Create(args models.FactArgs, reply *models.FactReply) error {
	var (
		factStore = models.FactStore(t.DB)
		err       error
	)
	logs.Debug(*args.Fact)
	args.Fact.Contact = models.Contact{}

	if err = factStore.Save(args.Fact, args); err != nil {
		logs.Error(err)
		return err
	}

	ID := args.Fact.ID
	args.Fact = &models.Fact{ID: ID}

	if reply.Fact, err = factStore.First(args); err != nil {
		logs.Error(err)
		return err
	}

	return nil
}

// Delete calls the FactSQL Delete method and returns the results via RPC
func (t *Fact) Delete(args models.FactArgs, reply *models.FactReply) error {
	var (
		factStore = models.FactStore(t.DB)
		err       error
	)

	if err = factStore.Delete(args.Fact, args); err != nil {
		logs.Debug(err)
		return err
	}

	return nil
}
