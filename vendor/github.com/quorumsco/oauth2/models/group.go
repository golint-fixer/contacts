// Definition of the structures and SQL interaction functions
package models

import (
	"time"
	// "github.com/asaskevich/govalidator"
)

// Group represents the components of a group
type Group struct {
	ID                             uint       `json:"id"`
	Name                           *string    `json:"name,omitempty"`
	Endofcampain                   *time.Time `json:"endofcampain"`
	Parti                          *string    `json:"parti,omitempty"`
	Echelle                        *string    `json:"echelle,omitempty"`
	Zone                           *string    `json:"zone,omitempty"`
	Display_surname                *string    `json:"display_surname,omitempty"`
	Display_married_name           *string    `json:"display_married_name,omitempty"`
	Display_firstname              *string    `json:"display_firstname,omitempty"`
	Display_city                   *string    `json:"display_city,omitempty"`
	Display_address                *string    `json:"display_address,omitempty"`
	Display_tel                    *string    `json:"display_tel,omitempty"`
	Display_mail                   *string    `json:"display_mail,omitempty"`
	Display_gender                 *string    `json:"display_gender,omitempty"`
	Display_age                    *string    `json:"display_age,omitempty"`
	Display_sendmail               *string    `json:"display_sendmail,omitempty"`
	Display_autosendmail_confirm   *string    `json:"display_autosendmail_confirm,omitempty"`
	Display_presence_new           *string    `json:"display_presence_new,omitempty"`
	Display_presence_around        *string    `json:"display_presence_around,omitempty"`
	Display_presence_search        *string    `json:"display_presence_search,omitempty"`
	Display_custom_address_details *string    `json:"display_custom_address_details,omitempty"`
	Create_contact_onreject        *string    `json:"create_contact_onreject,omitempty"`
	Email_referent                 *string    `json:"email_referent,omitempty"`
	Code_cause                     *string    `json:"code_cause,omitempty"`
	Code_edit_form                 *string    `json:"code_edit_form,omitempty"`
	Users                          []User     `json:"users,omitempty"`
	Teams                          []Team     `json:"teams,omitempty"`
}
