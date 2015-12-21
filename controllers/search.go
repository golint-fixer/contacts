// Bundle of functions managing the CRUD and the elasticsearch engine
package controllers

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/quorumsco/contacts/models"
	"github.com/quorumsco/elastic"
	"github.com/quorumsco/logs"
)

// Search contains the search related methods and a gorm client
type Search struct {
	Client *elastic.Client
}

type respID struct {
	ContactID uint `json:"contact_id"`
}

// Index indexes a contact into elasticsearch
func (s *Search) Index(args models.ContactArgs, reply *models.ContactReply) error {
	id := strconv.Itoa(int(args.Contact.ID))
	if id == "" {
		logs.Error("id is nil")
		return errors.New("id is nil")
	}

	if args.Contact.Address.Latitude != "" && args.Contact.Address.Longitude != "" {
		args.Contact.Address.Location = fmt.Sprintf("%s,%s", args.Contact.Address.Latitude, args.Contact.Address.Longitude)
	}

	_, err := s.Client.Index().
		Index("contacts").
		Type("contact").
		Id(id).
		BodyJson(args.Contact).
		Do()
	if err != nil {
		logs.Critical(err)
		return err
	}

	return nil
}

// Index indexes a contact into elasticsearch
func (s *Search) IndexFact(args models.FactArgs, reply *models.FactReply) error {
	args.Fact.Contact.Address.Location = fmt.Sprintf("%s,%s", args.Fact.Contact.Address.Latitude, args.Fact.Contact.Address.Longitude)
	_, err := s.Client.Index().
		Index("facts").
		Type("fact").
		BodyJson(args.Fact).
		Do()
	if err != nil {
		logs.Critical(err)
		return err
	}

	return nil
}

// UnIndex unindexes a contact from elasticsearch
func (s *Search) UnIndex(args models.ContactArgs, reply *models.ContactReply) error {
	id := strconv.Itoa(int(args.Contact.ID))
	if id == "" {
		logs.Error("id is nil")
		return errors.New("id is nil")
	}

	_, err := s.Client.Delete().
		Index("contacts").
		Type("contact").
		Id(id).
		Do()
	if err != nil {
		logs.Critical(err)
		return err
	}

	return nil
}

// SearchContacts performs a cross_field search request to elasticsearch and returns the results via RPC
func (s *Search) SearchContacts(args models.SearchArgs, reply *models.SearchReply) error {
	logs.Debug("args.Search.Query:%s", args.Search.Query)
	logs.Debug("args.Search.Fields:%s", args.Search.Fields)
	Query := elastic.NewMultiMatchQuery(args.Search.Query) //A remplacer par fields[] plus tard
	Query = Query.Type("cross_fields")
	Query = Query.Operator("and")

	if args.Search.Fields[0] == "firstname" {
		logs.Debug("firstname search")
		Query = Query.Field("firstname")
	} else {
		Query = Query.Field("surname")
		Query = Query.Field("firstname")
		Query = Query.Field("address")
	}

	source := elastic.NewFetchSourceContext(true)
	source = source.Include("id")
	source = source.Include("firstname")
	source = source.Include("surname")

	searchResult, err := s.Client.Search().
		Index("contacts").
		FetchSourceContext(source).
		Query(&Query).
		Size(30000).
		Sort("surname", true).
		Do()
	if err != nil {
		logs.Critical(err)
		return err
	}

	if searchResult.Hits != nil {
		for _, hit := range searchResult.Hits.Hits {
			var c models.Contact
			err := json.Unmarshal(*hit.Source, &c)
			if err != nil {
				logs.Error(err)
				return err
			}
			reply.Contacts = append(reply.Contacts, c)
		}
	} else {
		reply.Contacts = nil
	}

	return nil
}

// SearchViaGeopolygon performs a GeoPolygon search request to elasticsearch and returns the results via RPC
func (s *Search) SearchIDViaGeoPolygon(args models.SearchArgs, reply *models.SearchReply) error {
	Filter := elastic.NewGeoPolygonFilter("location")
	Filter2 := elastic.NewTermFilter("status", args.Search.Filter)

	var point models.Point
	for _, point = range args.Search.Polygon {
		geoPoint := elastic.GeoPointFromLatLon(point.Lat, point.Lon)
		Filter = Filter.AddPoint(geoPoint)
	}

	Query := elastic.NewFilteredQuery(elastic.NewMatchAllQuery())
	Query = Query.Filter(Filter)
	if args.Search.Filter != "" {
		Query = Query.Filter(Filter2)
	}

	source := elastic.NewFetchSourceContext(true)
	source = source.Include("contact_id")

	searchResult, err := s.Client.Search().
		Index("facts").
		FetchSourceContext(source).
		Query(&Query).
		Size(10000000).
		Do()
	if err != nil {
		logs.Critical(err)
		return err
	}

	if searchResult.Hits != nil {
		for _, hit := range searchResult.Hits.Hits {
			var c respID
			err := json.Unmarshal(*hit.Source, &c)
			if err != nil {
				logs.Error(err)
				return err
			}
			reply.IDs = append(reply.IDs, c.ContactID)
		}
	} else {
		reply.IDs = nil
	}

	return nil
}

// RetrieveContacts performs a match_all query to elasticsearch and returns the results via RPC
func (s *Search) RetrieveContacts(args models.SearchArgs, reply *models.SearchReply) error {
	Query := elastic.NewFilteredQuery(elastic.NewMatchAllQuery())
	source := elastic.NewFetchSourceContext(true)
	source = source.Include("id")
	source = source.Include("firstname")
	source = source.Include("surname")

	searchResult, err := s.Client.Search().
		Index("contacts").
		FetchSourceContext(source).
		Query(&Query).
		Size(10000000).
		Sort("surname", true).
		Do()
	if err != nil {
		logs.Critical(err)
		return err
	}

	if searchResult.Hits != nil {

		for _, hit := range searchResult.Hits.Hits {
			var c models.Contact
			err := json.Unmarshal(*hit.Source, &c)
			if err != nil {
				logs.Error(err)
				return err
			}
			reply.Contacts = append(reply.Contacts, c)
		}
	} else {

		reply.Contacts = nil
	}

	return nil
}
