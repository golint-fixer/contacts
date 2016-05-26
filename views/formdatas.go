// Views for JSON responses
package views

import "github.com/quorumsco/contacts/models"

// Formdatas is a type used for JSON request responses
type Formdatas struct {
	Formdatas []models.Formdata `json:"formdatas"`
}

// Form is a type used for JSON request responses
type Formdata struct {
	Formdata *models.Formdata `json:"formdata"`
}
