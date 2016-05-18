// Definition of the structures and SQL interaction functions
package models

// Search represents the search arguments
type Search struct {
	Query   string   `json:"query,omitempty"`
	Filter  string   `json:"filter,omitempty"`
	Fields  []string `json:"fields,omitempty"`
	Polygon []Point  `json:"polygon,omitempty"` //Declared in fact.go
}

// SearchArgs is used in the RPC communications between the gateway and Contacts
type SearchArgs struct {
	Search *Search
}

// SearchReply is used in the RPC communications between the gateway and Contacts
type SearchReply struct {
	Contacts []Contact
	Facts    []Fact
	IDs      []uint
}
