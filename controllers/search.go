// Bundle of functions managing the CRUD and the elasticsearch engine
package controllers

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

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
// search sur le firstname, surname, street et city. Les résultats renvoyés sont globaux.
func (s *Search) SearchContacts(args models.SearchArgs, reply *models.SearchReply) error {
	logs.Debug("args.Search.Query:%s", args.Search.Query)
	logs.Debug("args.Search.Fields:%s", args.Search.Fields)
	Query := elastic.NewMultiMatchQuery(args.Search.Query) //A remplacer par fields[] plus tard

	//https://www.elastic.co/guide/en/elasticsearch/reference/1.7/query-dsl-multi-match-query.html#type-phrase
	Query = Query.Type("cross_fields")
	Query = Query.Operator("and")

	if args.Search.Fields[0] == "firstname" {
		//logs.Debug("firstname search")
		Query = Query.Field("firstname")
	} else if args.Search.Fields[0] == "name" {
		//champs dans lesquels chercher
		Query = Query.Field("surname")
	} else if args.Search.Fields[0] == "fullname" {
		//champs dans lesquels chercher
		Query = Query.Field("firstname")
		Query = Query.Field("surname")
	} else if args.Search.Fields[0] == "address" {
		//champs dans lesquels chercher
		Query = Query.Field("address.street")
		Query = Query.Field("address.city")
	} else if args.Search.Fields[0] == "all" {
		//champs dans lesquels chercher
		Query = Query.Field("surname")
		Query = Query.Field("firstname")
		Query = Query.Field("address.street")
		Query = Query.Field("address.city")
	}
	// donneées à récupérer dans le résultat
	source := elastic.NewFetchSourceContext(true)
	source = source.Include("id")
	source = source.Include("firstname")
	source = source.Include("surname")
	source = source.Include("address.street")
	source = source.Include("address.housenumber")
	source = source.Include("address.city")

	searchResult, err := s.Client.Search().
		Index("contacts").
		FetchSourceContext(source).
		Query(&Query).
		Size(10000).
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
			//logs.Debug(reply.Contacts)
			reply.Contacts = append(reply.Contacts, c)
		}
	} else {
		reply.Contacts = nil
	}

	return nil
}

// SearchAddressesAggs performs a cross_field search request to elasticsearch and returns the results via RPC
// search sur le firstname, surname, street et city. Les résultats renvoyés sont globaux.
func (s *Search) SearchAddressesAggs(args models.SearchArgs, reply *models.SearchReply) error {
	//logs.Debug("args.Search.Query:%s", args.Search.Query)
	//logs.Debug("args.Search.Fields:%s", args.Search.Fields)
	Query := elastic.NewMultiMatchQuery(args.Search.Query) //A remplacer par fields[] plus tard

	//https://www.elastic.co/guide/en/elasticsearch/reference/1.7/query-dsl-multi-match-query.html#type-phrase
	Query = Query.Type("cross_fields")
	Query = Query.Operator("and")

	Query = Query.Field("address.street")
	Query = Query.Field("address.city")

	// donneées à récupérer dans le résultat
	source := elastic.NewFetchSourceContext(true)
	source = source.Include("address.street")
	source = source.Include("address.housenumber")
	source = source.Include("address.city")

	// create an aggregation
	aggreg_lattitude := elastic.NewTermsAggregation().Field("address.latitude").Size(5000)
	subaggreg_unique := elastic.NewTopHitsAggregation().Size(1000)
	aggreg_lattitude = aggreg_lattitude.SubAggregation("result_subaggreg", subaggreg_unique)

	searchResult, err := s.Client.Search().
		Index("contacts").
		FetchSourceContext(source).
		Query(&Query).
		Size(0).
		Aggregation("result_aggreg", aggreg_lattitude).
		Sort("surname", true).
		Do()
	if err != nil {
		logs.Critical(err)
		return err
	}

	agg, found := searchResult.Aggregations.Terms("result_aggreg")
	if !found {
		logs.Debug("we sould have a terms aggregation called %q", "aggreg_lattitude")
	}
	if searchResult.Aggregations != nil {
		for _, bucket := range agg.Buckets {
			subaggreg_unique, found := bucket.TopHits("result_subaggreg")
			if found {
				// pour chaque addresse aggrégée
				var cs models.AddressAggReply
				for _, addresse := range subaggreg_unique.Hits.Hits {
					//on utilise le modèle Contact uniquement pour stocker l'adresse aggrégée
					var c models.Contact
					err := json.Unmarshal(*addresse.Source, &c)
					if err != nil {
						logs.Error(err)
						return err
					}
					cs.Contacts = append(cs.Contacts, c)
				}
				reply.AddressAggs = append(reply.AddressAggs, cs)
			}
		}
	} else {
		reply.Contacts = nil
	}
	return nil
}

// SearchAddressesAggs performs a cross_field search request to elasticsearch and returns the results via RPC
// search sur le firstname, surname, street et city. Les résultats renvoyés sont globaux.
func (s *Search) SearchContactsGeoloc(args models.SearchArgs, reply *models.SearchReply) error {
	logs.Debug("args.Search.Query:%s", args.Search.Query)
	logs.Debug("args.Search.Fields:%s", args.Search.Fields)
	Query := elastic.NewMultiMatchQuery(args.Search.Query) //A remplacer par fields[] plus tard

	//https://www.elastic.co/guide/en/elasticsearch/reference/1.7/query-dsl-multi-match-query.html#type-phrase
	Query = Query.Type("cross_fields")
	Query = Query.Operator("and")

	Query = Query.Field("address.street")
	Query = Query.Field("address.city")

	// donneées à récupérer dans le résultat
	source := elastic.NewFetchSourceContext(true)
	source = source.Include("address.street")
	source = source.Include("address.housenumber")
	source = source.Include("address.city")

	// create an aggregation
	//Point(-70, 40)

	var geopoint = strings.Split(args.Search.Query, ",")
	a, err := strconv.ParseFloat(geopoint[0], 64)
	if err != nil {
		logs.Critical(err)
		return err
	}
	b, err := strconv.ParseFloat(geopoint[1], 64)
	if err != nil {
		logs.Critical(err)
		return err
	}
	logs.Debug(a)
	logs.Debug(b)
	//aggreg_sortGeodistance := elastic.NewTopHitsAggregation().SortBy(elastic.NewGeoDistanceSort("address.location").Point(a, b).Order(true).Unit("km").SortMode("min").GeoDistance("sloppy_arc")).Size(500)
	aggreg_sortGeodistance := elastic.NewTopHitsAggregation().Size(100).SortBy(elastic.NewGeoDistanceSort("address.location").Point(a, b).Unit("km").GeoDistance("sloppy_arc"))
	searchResult, err := s.Client.Search().
		Index("contacts").
		FetchSourceContext(source).
		Aggregation("aggreg_sortGeodistance", aggreg_sortGeodistance).
		Do()
	if err != nil {
		logs.Critical(err)
		return err
	}

	agg, found := searchResult.Aggregations.TopHits("aggreg_sortGeodistance")
	if !found {
		logs.Debug("we sould have a terms aggregation called %q", "aggreg_sortGeodistance")
	}
	if agg != nil {

		//subaggreg_unique, found := bucket.TopHits("aggreg_sortGeodistance")
		//logs.Debug("bucket.TopHits(aggreg_sortGeodistance) au moins de 1")

		for _, res_contact := range agg.Hits.Hits {

			//on utilise le modèle Contact uniquement pour stocker l'adresse aggrégée
			var c models.Contact

			err := json.Unmarshal(*res_contact.Source, &c)
			if err != nil {
				logs.Error(err)
				return err
			}

			// retourne une liste d'adresses aggrégées uniques.
			reply.Contacts = append(reply.Contacts, c)
		}
		//logs.Debug(reply.Contacts)

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
	source = source.Include("address.street")
	source = source.Include("address.housenumber")
	source = source.Include("address.city")

	searchResult, err := s.Client.Search().
		Index("contacts").
		FetchSourceContext(source).
		Query(&Query).
		Size(1000).
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
