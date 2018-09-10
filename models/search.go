// Definition of the structures and SQL interaction functions
package models

//To represent a GeoPolygon
type Point struct {
	Lat float64 `json:"lat"`
	Lng float64 `json:"lng"`
}

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

type KpiReply struct {
	Key       string
	Doc_count int64
}

type KpiAggs struct {
	KpiReplies []KpiReply
}

// SearchReply is used in the RPC communications between the gateway and Contacts
type SearchReply struct {
	Contacts          []Contact
	IDs               []uint
	AddressAggs       []AddressAggReply
	AddressStreetAggs []AddressStreetAggReply
	Kpi               []KpiAggs
	Aggregation       [][]string
	Data              []GenericMap
}

type AddressAggReply struct {
	Contacts []Contact
}

type AddressStreetAggReply struct {
	Addresses []AddressAggReply
}

type GenericMap struct {
	Key   string
	Value string
	Map   [][]interface{}
}
