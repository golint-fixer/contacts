// Views for JSON responses
package views

import "github.com/quorumsco/contacts/models"

// Contacts is a type used for JSON request responses
type Contacts struct {
	Contacts []models.Contact `json:"contacts"`
}

// Contact is a type used for JSON request responses
type Contact struct {
	Contact *models.Contact `json:"contact"`
}

type AddressAggs struct {
	AddressAggs []models.AddressAggReply `json:"AddressAggs"`
}

type Kpi struct {
	Kpi []models.KpiAggs `json:"kpi"`
}
