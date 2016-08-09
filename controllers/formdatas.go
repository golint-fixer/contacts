// Bundle of functions managing the CRUD and the elasticsearch engine
package controllers

import (
	"github.com/jinzhu/gorm"
	"github.com/quorumsco/contacts/models"
	"github.com/quorumsco/logs"
)

// Formdata contains the Formdata related methods and a gorm client
type Formdata struct {
	DB *gorm.DB
}

// RetrieveCollection calls the FormdataSQL Find method and returns the results via RPC
func (t *Formdata) RetrieveCollection(args models.FormdataArgs, reply *models.FormdataReply) error {
	var (
		err error

		FormdataStore = models.FormdataStore(t.DB)
	)

	reply.Formdatas, err = FormdataStore.Find(args)
	if err != nil {
		logs.Error(err)
		return err
	}

	return nil
}

// Retrieve calls the FormdataSQL First method and returns the results via RPC
func (t *Formdata) Retrieve(args models.FormdataArgs, reply *models.FormdataReply) error {
	var (
		FormdataStore = models.FormdataStore(t.DB)
		err           error
	)

	if reply.Formdata, err = FormdataStore.First(args); err != nil {
		logs.Error(err)
		return err
	}

	return nil
}

// Create calls the FormdataSQL Save method and returns the results via RPC
func (t *Formdata) Create(args models.FormdataArgs, reply *models.FormdataReply) error {
	var (
		err error

		FormdataStore = models.FormdataStore(t.DB)
	)

	if err = FormdataStore.Save(args.Formdata, args); err != nil {
		logs.Error(err)
		return err
	}

	reply.Formdata = args.Formdata

	return nil
}

// Delete calls the FormdataSQL Delete method and returns the results via RPC
func (t *Formdata) Delete(args models.FormdataArgs, reply *models.FormdataReply) error {
	var (
		err error

		FormdataStore = models.FormdataStore(t.DB)
	)

	if err = FormdataStore.Delete(args.Formdata, args); err != nil {
		logs.Debug(err)
		return err
	}

	return nil
}

// Delete calls the FormdataSQL Delete method and returns the results via RPC
func (t *Formdata) DeleteAll(args models.FormdataArgs, reply *models.FormdataReply) error {
	var (
		err error

		FormdataStore = models.FormdataStore(t.DB)
	)

	if err = FormdataStore.DeleteAll(args.Formdata, args); err != nil {
		logs.Debug(err)
		return err
	}

	return nil
}
