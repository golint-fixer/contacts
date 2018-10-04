package models

import "errors"

// NoContactID contains the error returned when the contact ID is nil
var NoContactID = errors.New("No contact_id specified")

// NoGroupID contains the error returned when the group ID isn't informed as the first field []string{} parameter
var NoGroupID = errors.New("No group_id specified as first field parameter")

// NoTerm contains the error returned when the field second's parameter (which should contain the queried term) is expected
var NoTerm = errors.New("No term specified as second field parameter")
