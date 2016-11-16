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
// exemple de requête elastic exécutée:
/*
{
  "_source": {
    "excludes": [],
    "includes": [
      "id",
      "firstname",
      "surname",
      "address.street",
      "address.housenumber",
      "address.city"
    ]
  },
  "query": {
    "bool": {
      "must": [
        {
          "multi_match": {
            "operator": "and",
            "query": "rue des menuts",
            "tie_breaker": 0,
            "type": "cross_fields",
            "fields": [
              "surname",
              "firstname",
              "city",
              "street"
            ]
          }
        },
        {
          "term": {
            "group_id": "1"
          }
        }
      ]
    }
  },
  "aggs": {
    "agg_gender": {
      "terms": {
        "field": "gender"
      }
    },
    "agg_pollingstation": {
      "terms": {
        "field": "address.PollingStation"
      }
    },
    "agg_birthdate": {
      "date_histogram": {
        "field": "birthdate",
        "interval": "year"
      }
    },
    "agg_lastChange": {
      "date_histogram": {
        "field": "lastchange",
        "interval": "week"
      }
    },
    "agg_agecategory": {
      "terms": {
        "field": "age_category"
      }
    }
  }
}
*/
func (s *Search) SearchContacts(args models.SearchArgs, reply *models.SearchReply) error {
	logs.Debug("SearchContacts - search.go")
	logs.Debug("args.Search.Query:%s", args.Search.Query)
	logs.Debug("args.Search.Fields:%s", args.Search.Fields)

	//variable permettant de savoir si nous souhaitons une requête renvoyant des contacts ou des KPI
	var KPI =false
	//query au cas où il y'a quelque chose dans la barre de recherche
 	Query := elastic.NewMultiMatchQuery(args.Search.Query)//A remplacer par fields[] plus tard
	//query au cas où il n'y a rien dans la barre de recherche
	QueryVide := elastic.NewMatchAllQuery()

	//si il y'a une recherche à faire sur un ou des termes
	if args.Search.Query!="" {
			//https://www.elastic.co/guide/en/elasticsearch/reference/1.7/query-dsl-multi-match-query.html#type-phrase
			Query = Query.Type("cross_fields")
			Query = Query.Operator("and")
			//Query2 := elastic.NewTermQuery("group_id", args.Search.Fields[0])

			if args.Search.Fields[1] == "firstname" {
				//logs.Debug("firstname search")
				Query = Query.Field("firstname")
			} else if args.Search.Fields[1] == "name" {
				//champs dans lesquels chercher
				Query = Query.Field("surname")
			} else if args.Search.Fields[1] == "fullname" {
				//champs dans lesquels chercher
				Query = Query.Field("firstname")
				Query = Query.Field("surname")
			} else if args.Search.Fields[1] == "street" {
				//champs dans lesquels chercher
				Query = Query.Field("address.street")
				Query = Query.Field("address.city")
			} else if args.Search.Fields[1] == "all" {
				//champs dans lesquels chercher
				Query = Query.Field("surname")
				Query = Query.Field("firstname")
				Query = Query.Field("address.street")
				Query = Query.Field("address.city")
			} else if args.Search.Fields[1] == "city&name" {
				//champs dans lesquels chercher
				Query = Query.Field("address.city")
				Query = Query.Field("surname")
			} else if args.Search.Fields[1] == "city&name&street" {
				//champs dans lesquels chercher
				Query = Query.Field("address.city")
				Query = Query.Field("surname")
				Query = Query.Field("address.street")
			}
	}

	// donneées à récupérer dans le résultat
	source := elastic.NewFetchSourceContext(true)
	source = source.Include("id")
	source = source.Include("firstname")
	source = source.Include("surname")
	source = source.Include("address.street")
	source = source.Include("address.housenumber")
	source = source.Include("address.city")

	bq := elastic.NewBoolQuery()
	// on ajoute à la bool query la requete vide ou pas
	if args.Search.Query!="" {
		bq = bq.Must(Query)
	}else{
		bq = bq.Must(QueryVide)
	}

	// filtre la recherche sur un groupe en particulier !!!! pas d'authorisation nécessaire !!!!
	bq = bq.Must(elastic.NewTermQuery("group_id", args.Search.Fields[0]))

	// contrôle si on est en recherche simple (mobile) ou avancée (desktop)
	// si recherche avancée cad plus de 3 paramêtres dans Fields
	if len(args.Search.Fields)>3{

		//KPI------------------------------------------------------------

		if boobool, err := strconv.ParseBool(args.Search.Fields[9]); err == nil {
			KPI=boobool
		}
		//gender ------------------------------------------------------------

		var gender_filter = args.Search.Fields[4]
		if gender_filter != ""{
			bq = bq.Must(elastic.NewTermQuery("gender", gender_filter))
		}
		//pollingstation ------------------------------------------------------------

		var pollingstation_filter = args.Search.Fields[5]
		if pollingstation_filter != ""{
			//affectation des différentes polling station dans un tableau de string
			var dataSlice_pollingstation []string = strings.Split(pollingstation_filter, "/")
			//création d'un array d'interface
			var interfaceSlice_pollingstation []interface{} = make([]interface{}, len(dataSlice_pollingstation))
			//affectation du tableau de sting au tab d'interface
			for i, d := range dataSlice_pollingstation {
			    interfaceSlice_pollingstation[i] = d
			}
			//injection de la query
			bq = bq.Must(elastic.NewTermsQuery("address.PollingStation", interfaceSlice_pollingstation...))
		}
		//age_category & birthdate ----------------------------------------------------

		var agecategory = args.Search.Fields[6]
		if agecategory != ""{

			//affectation des différentes catégories d'âge dans un tableau de string
			var dataSlice_agecategory []string = strings.Split(agecategory, "/")
			//création d'un array d'interface
			var interfaceSlice_agecategory []interface{} = make([]interface{}, len(dataSlice_agecategory))

			// pour chaque catégorie d'âge à filtrer
			for i, category := range dataSlice_agecategory {
					//affectation du tableau de sting au tab d'interface
			    interfaceSlice_agecategory[i] = category
					// injection d'une requête Should pour chaque tranche de birthdate à retenir
					//bq = bq.Should(elastic.NewRangeQuery("birthdate").From("1980-12-01").To("1981-04-01"))
					switch category {
					case "0":
      			bq = bq.Should(elastic.NewRangeQuery("birthdate").Gte("now-18y/d"))
      		case "1":
      			bq = bq.Should(elastic.NewRangeQuery("birthdate").Gte("now-25y/d").Lte("now-18y/d"))
      		case "2":
      			bq = bq.Should(elastic.NewRangeQuery("birthdate").Gte("now-35y/d").Lte("now-25y/d"))
      		case "3":
      			bq = bq.Should(elastic.NewRangeQuery("birthdate").Gte("now-50y/d").Lte("now-35y/d"))
      		case "4":
      			bq = bq.Should(elastic.NewRangeQuery("birthdate").Gte("now-65y/d").Lte("now-50y/d"))
      		case "5":
      			bq = bq.Should(elastic.NewRangeQuery("birthdate").Lte("now-65y/d"))
					}
			}

			//injection de la query pour catégorie d'âge
			bq = bq.Should(elastic.NewTermsQuery("age_category", interfaceSlice_agecategory...))
			bq = bq.MinimumShouldMatch("1")
		}
	}

	// positionner le nombre de résultats attendus : nb de contacts -----------------
	var size_requete int
	var from_requete int
	// on vérifie que l'on a au moins 3 paramêtres ------
	if len(args.Search.Fields) >= 3 {
		i, err := strconv.Atoi(args.Search.Fields[2])
		if err == nil {
			size_requete = i
		} else {
			// par défaut
			size_requete = 1000
		}
		// cas de recherche avancée: on définit le from de la requête ------
		if len(args.Search.Fields) > 3 {
			i, err = strconv.Atoi(args.Search.Fields[3])
			if err == nil {
				from_requete = i
			} else {
				// par défaut
				from_requete = 0
			}
		}

	} else {
		// par défaut
		size_requete = 1000
		from_requete = 0
	}

	// sort --------------------------------------
	var sort string
	var asc bool

	if len(args.Search.Fields) > 8 {
		sort = args.Search.Fields[7]
		if asc2, err := strconv.ParseBool(args.Search.Fields[8]); err == nil {
			asc=asc2
		}
	} else{
		sort = "surname"
		asc = true
	}

	//----------------------------------------------------------------------------------
	//aggregation pour KPI
	if KPI==true{

	aggreg_kpi_gender := elastic.NewTermsAggregation().Field("gender")
	aggreg_kpi_pollingstation := elastic.NewTermsAggregation().Field("address.PollingStation")
	aggreg_kpi_agecategory := elastic.NewTermsAggregation().Field("age_category")
	aggreg_kpi_birthdate := elastic.NewDateHistogramAggregation().Field("birthdate").Interval("year")
	aggreg_kpi_lastchange := elastic.NewDateHistogramAggregation().Field("lastchange").Interval("week")

	searchResult, err := s.Client.Search().
		Index("contacts").
		FetchSourceContext(source).
		Query(&bq).
		Size(size_requete).
		Aggregation("gender_aggreg", aggreg_kpi_gender).
		Aggregation("pollingstation_aggreg", aggreg_kpi_pollingstation).
		Aggregation("agecategory_aggreg", aggreg_kpi_agecategory).
		Aggregation("birthdate_aggreg", aggreg_kpi_birthdate).
		Aggregation("lastchange_aggreg", aggreg_kpi_lastchange).
		Do()

	if err != nil {
		logs.Critical(err)
		return err
	}

	gender_agg, found := searchResult.Aggregations.Terms("gender_aggreg")
	pollingstation_agg, found2 := searchResult.Aggregations.Terms("pollingstation_aggreg")
	agecategory_agg, found3 := searchResult.Aggregations.Terms("agecategory_aggreg")
	birthdate_agg, found4 := searchResult.Aggregations.DateHistogram("birthdate_aggreg")
	lastchange_agg, found5 := searchResult.Aggregations.DateHistogram("lastchange_aggreg")

	if !found {
		logs.Debug("we sould have a terms aggregation called %q", "gender_aggreg")
	}
	if !found2 {
		logs.Debug("we sould have a terms aggregation called %q", "pollingstation_aggreg")
	}
	if !found3 {
		logs.Debug("we sould have a terms aggregation called %q", "agecategory_aggreg")
	}
	if !found4 {
		logs.Debug("we sould have a terms aggregation called %q", "birthdate_aggreg")
	}
	if !found5 {
		logs.Debug("we sould have a terms aggregation called %q", "lastchange_aggreg")
	}

	if searchResult.Aggregations != nil {

		// ---- stockage réponses pour gender_aggreg ----------------------
		var tab_kpiAtom models.KpiAggs
		for _, bucket := range gender_agg.Buckets {
			var kpiAtom models.KpiReply
			kpiAtom.Key=bucket.Key.(string)
			kpiAtom.Doc_count=bucket.DocCount
			tab_kpiAtom.KpiReplies=append(tab_kpiAtom.KpiReplies, kpiAtom)
		}
		reply.Kpi=append(reply.Kpi, tab_kpiAtom)
		tab_kpiAtom = models.KpiAggs{}

		// ---- stockage réponses pour pollingstation_aggreg -----------------------
		for _, bucket := range pollingstation_agg.Buckets {
			var kpiAtom models.KpiReply
			kpiAtom.Key=bucket.Key.(string)
			kpiAtom.Doc_count=bucket.DocCount
			tab_kpiAtom.KpiReplies=append(tab_kpiAtom.KpiReplies, kpiAtom)
		}
		reply.Kpi=append(reply.Kpi, tab_kpiAtom)
		tab_kpiAtom = models.KpiAggs{}

		// ---- stockage réponses pour agecategory_aggreg -----------------------
		for _, bucket := range agecategory_agg.Buckets {
			var kpiAtom models.KpiReply
			logs.Debug("for _, bucket := range agecategory_agg.Buckets ")
			logs.Debug(bucket)
			//kpiAtom.Key=bucket.Key.(string)
			kpiAtom.Key= strconv.FormatFloat(bucket.Key.(float64), 'f', -1, 64)
			kpiAtom.Doc_count=bucket.DocCount
			tab_kpiAtom.KpiReplies=append(tab_kpiAtom.KpiReplies, kpiAtom)
		}
		reply.Kpi=append(reply.Kpi, tab_kpiAtom)
		tab_kpiAtom = models.KpiAggs{}

		// ---- stockage réponses pour birthdate_aggreg -----------------------
		for _, bucket := range birthdate_agg.Buckets {
			var kpiAtom models.KpiReply
			kpiAtom.Key=*bucket.KeyAsString
			kpiAtom.Doc_count=bucket.DocCount
			tab_kpiAtom.KpiReplies=append(tab_kpiAtom.KpiReplies, kpiAtom)
		}
		reply.Kpi=append(reply.Kpi, tab_kpiAtom)
		tab_kpiAtom = models.KpiAggs{}

		// ---- stockage réponses pour lastchange_aggreg -----------------------
		for _, bucket := range lastchange_agg.Buckets {
			var kpiAtom models.KpiReply
			kpiAtom.Key=*bucket.KeyAsString
			kpiAtom.Doc_count=bucket.DocCount
			tab_kpiAtom.KpiReplies=append(tab_kpiAtom.KpiReplies, kpiAtom)
		}
		reply.Kpi=append(reply.Kpi, tab_kpiAtom)
		tab_kpiAtom = models.KpiAggs{}

	} else {
		reply.Kpi = nil
	}

} else //-------- findcontacts classique -----------------------------------------------
{
	searchResult, err := s.Client.Search().
		Index("contacts").
		FetchSourceContext(source).
		Query(&bq).
		Size(size_requete).
		From(from_requete).
		Sort(sort, asc).
		Do()

	if err != nil {
		logs.Critical(err)
		return err
	}

	// traitements des hits --------------------------------
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
}
	return nil
}

func (s *Search) KpiContacts(args models.SearchArgs, reply *models.SearchReply) error {
	logs.Debug("SearchContacts - search.go")
	logs.Debug("args.Search.Query:%s", args.Search.Query)
	logs.Debug("args.Search.Fields:%s", args.Search.Fields)

	//variable permettant de savoir si nous souhaitons une requête renvoyant des contacts ou des KPI
	var KPI =false
	//query au cas où il y'a quelque chose dans la barre de recherche
 	Query := elastic.NewMultiMatchQuery(args.Search.Query)//A remplacer par fields[] plus tard
	//query au cas où il n'y a rien dans la barre de recherche
	QueryVide := elastic.NewMatchAllQuery()

	//si il y'a une recherche à faire sur un ou des termes
	if args.Search.Query!="" {
			//https://www.elastic.co/guide/en/elasticsearch/reference/1.7/query-dsl-multi-match-query.html#type-phrase
			Query = Query.Type("cross_fields")
			Query = Query.Operator("and")
			//Query2 := elastic.NewTermQuery("group_id", args.Search.Fields[0])

			if args.Search.Fields[1] == "firstname" {
				//logs.Debug("firstname search")
				Query = Query.Field("firstname")
			} else if args.Search.Fields[1] == "name" {
				//champs dans lesquels chercher
				Query = Query.Field("surname")
			} else if args.Search.Fields[1] == "fullname" {
				//champs dans lesquels chercher
				Query = Query.Field("firstname")
				Query = Query.Field("surname")
			} else if args.Search.Fields[1] == "street" {
				//champs dans lesquels chercher
				Query = Query.Field("address.street")
				Query = Query.Field("address.city")
			} else if args.Search.Fields[1] == "all" {
				//champs dans lesquels chercher
				Query = Query.Field("surname")
				Query = Query.Field("firstname")
				Query = Query.Field("address.street")
				Query = Query.Field("address.city")
			} else if args.Search.Fields[1] == "city&name" {
				//champs dans lesquels chercher
				Query = Query.Field("address.city")
				Query = Query.Field("surname")
			} else if args.Search.Fields[1] == "city&name&street" {
				//champs dans lesquels chercher
				Query = Query.Field("address.city")
				Query = Query.Field("surname")
				Query = Query.Field("address.street")
			}
	}

	// donneées à récupérer dans le résultat
	source := elastic.NewFetchSourceContext(true)
	source = source.Include("id")
	source = source.Include("firstname")
	source = source.Include("surname")
	source = source.Include("address.street")
	source = source.Include("address.housenumber")
	source = source.Include("address.city")

	bq := elastic.NewBoolQuery()
	// on ajoute à la bool query la requete vide ou pas
	if args.Search.Query!="" {
		bq = bq.Must(Query)
	}else{
		bq = bq.Must(QueryVide)
	}

	// filtre la recherche sur un groupe en particulier !!!! pas d'authorisation nécessaire !!!!
	bq = bq.Must(elastic.NewTermQuery("group_id", args.Search.Fields[0]))

	// contrôle si on est en recherche simple (mobile) ou avancée (desktop)
	// si recherche avancée cad plus de 3 paramêtres dans Fields
	if len(args.Search.Fields)>3{

		//KPI------------------------------------------------------------

		if boobool, err := strconv.ParseBool(args.Search.Fields[9]); err == nil {
			KPI=boobool
		}
		//gender ------------------------------------------------------------

		var gender_filter = args.Search.Fields[4]
		if gender_filter != ""{
			bq = bq.Must(elastic.NewTermQuery("gender", gender_filter))
		}
		//pollingstation ------------------------------------------------------------

		var pollingstation_filter = args.Search.Fields[5]
		if pollingstation_filter != ""{
			//affectation des différentes polling station dans un tableau de string
			var dataSlice_pollingstation []string = strings.Split(pollingstation_filter, "/")
			//création d'un array d'interface
			var interfaceSlice_pollingstation []interface{} = make([]interface{}, len(dataSlice_pollingstation))
			//affectation du tableau de sting au tab d'interface
			for i, d := range dataSlice_pollingstation {
			    interfaceSlice_pollingstation[i] = d
			}
			//injection de la query
			bq = bq.Must(elastic.NewTermsQuery("address.PollingStation", interfaceSlice_pollingstation...))
		}
		//age_category & birthdate ----------------------------------------------------

		var agecategory = args.Search.Fields[6]
		if agecategory != ""{

			//affectation des différentes catégories d'âge dans un tableau de string
			var dataSlice_agecategory []string = strings.Split(agecategory, "/")
			//création d'un array d'interface
			var interfaceSlice_agecategory []interface{} = make([]interface{}, len(dataSlice_agecategory))

			// pour chaque catégorie d'âge à filtrer
			for i, category := range dataSlice_agecategory {
					//affectation du tableau de sting au tab d'interface
			    interfaceSlice_agecategory[i] = category
					// injection d'une requête Should pour chaque tranche de birthdate à retenir
					//bq = bq.Should(elastic.NewRangeQuery("birthdate").From("1980-12-01").To("1981-04-01"))
					switch category {
					case "0":
      			bq = bq.Should(elastic.NewRangeQuery("birthdate").Gte("now-18y/d"))
      		case "1":
      			bq = bq.Should(elastic.NewRangeQuery("birthdate").Gte("now-25y/d").Lte("now-18y/d"))
      		case "2":
      			bq = bq.Should(elastic.NewRangeQuery("birthdate").Gte("now-35y/d").Lte("now-25y/d"))
      		case "3":
      			bq = bq.Should(elastic.NewRangeQuery("birthdate").Gte("now-50y/d").Lte("now-35y/d"))
      		case "4":
      			bq = bq.Should(elastic.NewRangeQuery("birthdate").Gte("now-65y/d").Lte("now-50y/d"))
      		case "5":
      			bq = bq.Should(elastic.NewRangeQuery("birthdate").Lte("now-65y/d"))
					}
			}

			//injection de la query pour catégorie d'âge
			bq = bq.Should(elastic.NewTermsQuery("age_category", interfaceSlice_agecategory...))
			bq = bq.MinimumShouldMatch("1")
		}
	}

	// positionner le nombre de résultats attendus : nb de contacts -----------------
	var size_requete int
	var from_requete int
	// on vérifie que l'on a au moins 3 paramêtres ------
	if len(args.Search.Fields) >= 3 {
		i, err := strconv.Atoi(args.Search.Fields[2])
		if err == nil {
			size_requete = i
		} else {
			// par défaut
			size_requete = 1000
		}
		// cas de recherche avancée: on définit le from de la requête ------
		if len(args.Search.Fields) > 3 {
			i, err = strconv.Atoi(args.Search.Fields[3])
			if err == nil {
				from_requete = i
			} else {
				// par défaut
				from_requete = 0
			}
		}

	} else {
		// par défaut
		size_requete = 1000
		from_requete = 0
	}

	// sort --------------------------------------
	var sort string
	var asc bool

	if len(args.Search.Fields) > 8 {
		sort = args.Search.Fields[7]
		if asc2, err := strconv.ParseBool(args.Search.Fields[8]); err == nil {
			asc=asc2
		}
	} else{
		sort = "surname"
		asc = true
	}

	//----------------------------------------------------------------------------------
	//aggregation pour KPI
	if KPI==true{

	aggreg_kpi_gender := elastic.NewTermsAggregation().Field("gender")
	aggreg_kpi_pollingstation := elastic.NewTermsAggregation().Field("address.PollingStation")
	aggreg_kpi_agecategory := elastic.NewTermsAggregation().Field("age_category")
	aggreg_kpi_birthdate := elastic.NewDateHistogramAggregation().Field("birthdate").Interval("year")
	aggreg_kpi_lastchange := elastic.NewDateHistogramAggregation().Field("lastchange").Interval("week")

	searchResult, err := s.Client.Search().
		Index("contacts").
		FetchSourceContext(source).
		Query(&bq).
		Size(size_requete).
		Aggregation("gender_aggreg", aggreg_kpi_gender).
		Aggregation("pollingstation_aggreg", aggreg_kpi_pollingstation).
		Aggregation("agecategory_aggreg", aggreg_kpi_agecategory).
		Aggregation("birthdate_aggreg", aggreg_kpi_birthdate).
		Aggregation("lastchange_aggreg", aggreg_kpi_lastchange).
		Do()

	if err != nil {
		logs.Critical(err)
		return err
	}

	gender_agg, found := searchResult.Aggregations.Terms("gender_aggreg")
	pollingstation_agg, found2 := searchResult.Aggregations.Terms("pollingstation_aggreg")
	agecategory_agg, found3 := searchResult.Aggregations.Terms("agecategory_aggreg")
	birthdate_agg, found4 := searchResult.Aggregations.DateHistogram("birthdate_aggreg")
	lastchange_agg, found5 := searchResult.Aggregations.DateHistogram("lastchange_aggreg")

	if !found {
		logs.Debug("we sould have a terms aggregation called %q", "gender_aggreg")
	}
	if !found2 {
		logs.Debug("we sould have a terms aggregation called %q", "pollingstation_aggreg")
	}
	if !found3 {
		logs.Debug("we sould have a terms aggregation called %q", "agecategory_aggreg")
	}
	if !found4 {
		logs.Debug("we sould have a terms aggregation called %q", "birthdate_aggreg")
	}
	if !found5 {
		logs.Debug("we sould have a terms aggregation called %q", "lastchange_aggreg")
	}


	if searchResult.Aggregations != nil {
		var tab_kpiAtom models.KpiAggs
		// ---- stockage nombre de résultats de la requête ----------------------
		if searchResult.Hits != nil {
			var kpiAtom models.KpiReply
			kpiAtom.Key="total"
			kpiAtom.Doc_count=searchResult.Hits.TotalHits
			tab_kpiAtom.KpiReplies=append(tab_kpiAtom.KpiReplies, kpiAtom)
			reply.Kpi=append(reply.Kpi, tab_kpiAtom)
			tab_kpiAtom = models.KpiAggs{}
		}
		// ---- stockage réponses pour gender_aggreg ----------------------
		for _, bucket := range gender_agg.Buckets {
			var kpiAtom models.KpiReply
			kpiAtom.Key=bucket.Key.(string)
			kpiAtom.Doc_count=bucket.DocCount
			tab_kpiAtom.KpiReplies=append(tab_kpiAtom.KpiReplies, kpiAtom)
		}
		reply.Kpi=append(reply.Kpi, tab_kpiAtom)
		tab_kpiAtom = models.KpiAggs{}

		// ---- stockage réponses pour pollingstation_aggreg -----------------------
		for _, bucket := range pollingstation_agg.Buckets {
			var kpiAtom models.KpiReply
			kpiAtom.Key=bucket.Key.(string)
			kpiAtom.Doc_count=bucket.DocCount
			tab_kpiAtom.KpiReplies=append(tab_kpiAtom.KpiReplies, kpiAtom)
		}
		reply.Kpi=append(reply.Kpi, tab_kpiAtom)
		tab_kpiAtom = models.KpiAggs{}

		// ---- stockage réponses pour agecategory_aggreg -----------------------
		for _, bucket := range agecategory_agg.Buckets {
			var kpiAtom models.KpiReply
			logs.Debug("for _, bucket := range agecategory_agg.Buckets ")
			logs.Debug(bucket)
			//kpiAtom.Key=bucket.Key.(string)
			kpiAtom.Key= strconv.FormatFloat(bucket.Key.(float64), 'f', -1, 64)
			kpiAtom.Doc_count=bucket.DocCount
			tab_kpiAtom.KpiReplies=append(tab_kpiAtom.KpiReplies, kpiAtom)
		}
		reply.Kpi=append(reply.Kpi, tab_kpiAtom)
		tab_kpiAtom = models.KpiAggs{}

		// ---- stockage réponses pour birthdate_aggreg -----------------------
		for _, bucket := range birthdate_agg.Buckets {
			var kpiAtom models.KpiReply
			kpiAtom.Key=*bucket.KeyAsString
			kpiAtom.Doc_count=bucket.DocCount
			tab_kpiAtom.KpiReplies=append(tab_kpiAtom.KpiReplies, kpiAtom)
		}
		reply.Kpi=append(reply.Kpi, tab_kpiAtom)
		tab_kpiAtom = models.KpiAggs{}

		// ---- stockage réponses pour lastchange_aggreg -----------------------
		for _, bucket := range lastchange_agg.Buckets {
			var kpiAtom models.KpiReply
			kpiAtom.Key=*bucket.KeyAsString
			kpiAtom.Doc_count=bucket.DocCount
			tab_kpiAtom.KpiReplies=append(tab_kpiAtom.KpiReplies, kpiAtom)
		}
		reply.Kpi=append(reply.Kpi, tab_kpiAtom)
		tab_kpiAtom = models.KpiAggs{}

	} else {
		reply.Kpi = nil
	}

} else //-------- findcontacts classique -----------------------------------------------
{
	searchResult, err := s.Client.Search().
		Index("contacts").
		FetchSourceContext(source).
		Query(&bq).
		Size(size_requete).
		From(from_requete).
		Sort(sort, asc).
		Do()

	if err != nil {
		logs.Critical(err)
		return err
	}

	// traitements des hits --------------------------------
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
}
	return nil
}

//même requête que SearchContacts mais sans les résultats contacts mais avec les KPI associés
func (s *Search) KpiContactsBACK(args models.SearchArgs, reply *models.SearchReply) error {
	logs.Debug("KpiContacts - search.go")
	logs.Debug("args.Search.Query:%s", args.Search.Query)
	logs.Debug("args.Search.Fields:%s", args.Search.Fields)
	Query := elastic.NewMultiMatchQuery(args.Search.Query) //A remplacer par fields[] plus tard

	//https://www.elastic.co/guide/en/elasticsearch/reference/1.7/query-dsl-multi-match-query.html#type-phrase
	Query = Query.Type("cross_fields")
	Query = Query.Operator("and")

	//Query2 := elastic.NewTermQuery("group_id", args.Search.Fields[0])

	if args.Search.Fields[1] == "firstname" {
		//logs.Debug("firstname search")
		Query = Query.Field("firstname")
	} else if args.Search.Fields[1] == "name" {
		//champs dans lesquels chercher
		Query = Query.Field("surname")
	} else if args.Search.Fields[1] == "fullname" {
		//champs dans lesquels chercher
		Query = Query.Field("firstname")
		Query = Query.Field("surname")
	} else if args.Search.Fields[1] == "address" {
		//champs dans lesquels chercher
		Query = Query.Field("address.street")
		Query = Query.Field("address.city")
	} else if args.Search.Fields[1] == "all" {
		//champs dans lesquels chercher
		Query = Query.Field("surname")
		Query = Query.Field("firstname")
		Query = Query.Field("address.street")
		Query = Query.Field("address.city")
	} else if args.Search.Fields[1] == "city&name" {
		//champs dans lesquels chercher
		Query = Query.Field("address.city")
		Query = Query.Field("surname")
	} else if args.Search.Fields[1] == "city&name&street" {
		//champs dans lesquels chercher
		Query = Query.Field("address.city")
		Query = Query.Field("surname")
		Query = Query.Field("address.street")
	}

	// donneées à récupérer dans le résultat
	source := elastic.NewFetchSourceContext(true)
	source = source.Include("id")

	bq := elastic.NewBoolQuery()
	bq = bq.Must(Query)
	bq = bq.Must(elastic.NewTermQuery("group_id", args.Search.Fields[0]))

	if len(args.Search.Fields)>3{
		//gender
		var gender_filter = args.Search.Fields[3]
		if gender_filter != ""{
			bq = bq.Must(elastic.NewTermQuery("gender", gender_filter))
		}
		//pollingstation
		//var pollingstation_filter := args.Search.Fields[4]
		//age_category
		//var agecategory := args.Search.Fields[5]
	}

	// positionner le nombre de résultats attendus : nb de contacts
	var size_requete int
	size_requete = 0

	//aggregation pour KPI

	aggreg_kpi_gender := elastic.NewTermsAggregation().Field("gender")
	aggreg_kpi_pollingstation := elastic.NewTermsAggregation().Field("address.PollingStation")
	aggreg_kpi_agecategory := elastic.NewTermsAggregation().Field("age_category")
	aggreg_kpi_birthdate := elastic.NewDateHistogramAggregation().Field("birthdate").Interval("year")
	aggreg_kpi_lastchange := elastic.NewDateHistogramAggregation().Field("lastchange").Interval("week")

	//subaggreg_unique := elastic.NewTopHitsAggregation().Size(size_nb_address_aggrege)
	//aggreg_kpi = aggreg_kpi.SubAggregation("result_subaggreg", subaggreg_unique)

	searchResult, err := s.Client.Search().
		Index("contacts").
		FetchSourceContext(source).
		Query(&bq).
		Size(size_requete).
		Aggregation("gender_aggreg", aggreg_kpi_gender).
		Aggregation("pollingstation_aggreg", aggreg_kpi_pollingstation).
		Aggregation("agecategory_aggreg", aggreg_kpi_agecategory).
		Aggregation("birthdate_aggreg", aggreg_kpi_birthdate).
		Aggregation("lastchange_aggreg", aggreg_kpi_lastchange).
		Do()

	if err != nil {
		logs.Critical(err)
		return err
	}

	// traitements des hits --------------------------------
	reply.Contacts = nil
	//traitements des aggs - KPI ---------------------------

	gender_agg, found := searchResult.Aggregations.Terms("gender_aggreg")
	pollingstation_agg, found2 := searchResult.Aggregations.Terms("pollingstation_aggreg")
	agecategory_agg, found3 := searchResult.Aggregations.Terms("agecategory_aggreg")
	birthdate_agg, found4 := searchResult.Aggregations.DateHistogram("birthdate_aggreg")
	lastchange_agg, found5 := searchResult.Aggregations.DateHistogram("lastchange_aggreg")


	if !found {
		logs.Debug("we sould have a terms aggregation called %q", "gender_aggreg")
	}
	if !found2 {
		logs.Debug("we sould have a terms aggregation called %q", "pollingstation_aggreg")
	}
	if !found3 {
		logs.Debug("we sould have a terms aggregation called %q", "agecategory_aggreg")
	}
	if !found4 {
		logs.Debug("we sould have a terms aggregation called %q", "birthdate_aggreg")
	}
	if !found5 {
		logs.Debug("we sould have a terms aggregation called %q", "lastchange_aggreg")
	}

	if searchResult.Aggregations != nil {

		// ---- stockage réponses pour gender_aggreg ----------------------
		var tab_kpiAtom models.KpiAggs
		for _, bucket := range gender_agg.Buckets {
			var kpiAtom models.KpiReply
			kpiAtom.Key=bucket.Key.(string)
			kpiAtom.Doc_count=bucket.DocCount
			tab_kpiAtom.KpiReplies=append(tab_kpiAtom.KpiReplies, kpiAtom)
		}
		reply.Kpi=append(reply.Kpi, tab_kpiAtom)
		tab_kpiAtom = models.KpiAggs{}

		// ---- stockage réponses pour pollingstation_aggreg -----------------------
		for _, bucket := range pollingstation_agg.Buckets {
			var kpiAtom models.KpiReply
			kpiAtom.Key=bucket.Key.(string)
			kpiAtom.Doc_count=bucket.DocCount
			tab_kpiAtom.KpiReplies=append(tab_kpiAtom.KpiReplies, kpiAtom)
		}
		reply.Kpi=append(reply.Kpi, tab_kpiAtom)
		tab_kpiAtom = models.KpiAggs{}

		// ---- stockage réponses pour agecategory_aggreg -----------------------
		for _, bucket := range agecategory_agg.Buckets {
			var kpiAtom models.KpiReply
			logs.Debug("for _, bucket := range agecategory_agg.Buckets ")
			logs.Debug(bucket)
			//kpiAtom.Key=bucket.Key.(string)
			kpiAtom.Key= strconv.FormatFloat(bucket.Key.(float64), 'f', -1, 64)
			kpiAtom.Doc_count=bucket.DocCount
			tab_kpiAtom.KpiReplies=append(tab_kpiAtom.KpiReplies, kpiAtom)
		}
		reply.Kpi=append(reply.Kpi, tab_kpiAtom)
		tab_kpiAtom = models.KpiAggs{}

		// ---- stockage réponses pour birthdate_aggreg -----------------------
		for _, bucket := range birthdate_agg.Buckets {
			var kpiAtom models.KpiReply
			kpiAtom.Key=*bucket.KeyAsString
			kpiAtom.Doc_count=bucket.DocCount
			tab_kpiAtom.KpiReplies=append(tab_kpiAtom.KpiReplies, kpiAtom)
		}
		reply.Kpi=append(reply.Kpi, tab_kpiAtom)
		tab_kpiAtom = models.KpiAggs{}

		// ---- stockage réponses pour lastchange_aggreg -----------------------
		for _, bucket := range lastchange_agg.Buckets {
			var kpiAtom models.KpiReply
			kpiAtom.Key=*bucket.KeyAsString
			kpiAtom.Doc_count=bucket.DocCount
			tab_kpiAtom.KpiReplies=append(tab_kpiAtom.KpiReplies, kpiAtom)
		}
		reply.Kpi=append(reply.Kpi, tab_kpiAtom)
		tab_kpiAtom = models.KpiAggs{}

	} else {
		reply.Kpi = nil
	}
	return nil
}


// SearchAddressesAggs performs a cross_field search request to elasticsearch and returns the results via RPC
// search sur le firstname, surname, street et city. Les résultats renvoyés sont globaux.
// exemple de requête elastic exécutée:
/*

{
  "_source": {
    "includes": [
      "id",
      "firstname",
      "surname",
      "address.street",
      "address.housenumber",
      "address.city"
    ]
  },
  "query": {
    "bool": {
      "must": [
        {
          "term": {
            "group_id": "2"
          }
        },
        {
          "term": {
            "address.street": "men"
          }
        }
      ]
    }
  },
  "aggs": {
    "subdistinct": {
      "terms": {
        "field": "address.latitude",
        "size": 5000
      },
      "aggs": {
        "subdistinct": {
          "top_hits": {
            "size": 100,
            "_source": [
              "id",
              "surname",
              "address.id",
              "address.housenumber",
              "address.street",
              "address.city",
              "address.postalcode"
            ]
          }
        }
      }
    }
  },
  "from": 0,
  "size": 0,
  "sort": []
}

*/

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

	// positionner le nombre de résultats attendus : nb de contacts ----------
	var size_nb_contact_par_address int
	var size_nb_address_aggrege int
	if len(args.Search.Fields) == 4 {
		i, err := strconv.Atoi(args.Search.Fields[2])
		if err == nil {
			size_nb_contact_par_address = i
		} else {
			// par défaut
			size_nb_contact_par_address = 1000
		}
		j, err := strconv.Atoi(args.Search.Fields[3])
		if err == nil {
			size_nb_address_aggrege = j
		} else {
			// par défaut
			size_nb_address_aggrege = 1000
		}
	} else {
		// par défaut
		size_nb_contact_par_address = 1000
		size_nb_address_aggrege = 1000
	}

	bq := elastic.NewBoolQuery()
	bq = bq.Must(Query)
	bq = bq.Must(elastic.NewTermQuery("group_id", args.Search.Fields[0]))

	// create an aggregation
	aggreg_lattitude := elastic.NewTermsAggregation().Field("address.latitude").Size(size_nb_contact_par_address)
	subaggreg_unique := elastic.NewTopHitsAggregation().Size(size_nb_address_aggrege)
	aggreg_lattitude = aggreg_lattitude.SubAggregation("result_subaggreg", subaggreg_unique)

	searchResult, err := s.Client.Search().
		Index("contacts").
		FetchSourceContext(source).
		Query(&bq).
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

	bq := elastic.NewBoolQuery()
	bq = bq.Must(elastic.NewTermQuery("group_id", args.Search.Fields[0]))

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

	// positionner le nombre de résultats attendus : nb de contacts ----------
	var size_nb_address_aggrege int
	if len(args.Search.Fields) == 3 {
		j, err := strconv.Atoi(args.Search.Fields[2])
		if err == nil {
			size_nb_address_aggrege = j
		} else {
			// par défaut
			size_nb_address_aggrege = 1000
		}
	} else {
		// par défaut
		size_nb_address_aggrege = 1000
	}

	//aggreg_sortGeodistance := elastic.NewTopHitsAggregation().SortBy(elastic.NewGeoDistanceSort("address.location").Point(a, b).Order(true).Unit("km").SortMode("min").GeoDistance("sloppy_arc")).Size(500)
	aggreg_sortGeodistance := elastic.NewTopHitsAggregation().Size(size_nb_address_aggrege * 10).SortBy(elastic.NewGeoDistanceSort("address.location").Point(a, b).Unit("km").GeoDistance("sloppy_arc"))
	searchResult, err := s.Client.Search().
		Index("contacts").
		FetchSourceContext(source).
		Query(&bq).
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
		Size(20000).
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
