// Bundle of functions managing the CRUD and the elasticsearch engine
package controllers

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/quorumsco/contacts/models"
	//"github.com/quorumsco/elastic"
	"github.com/quorumsco/logs"
	elastic "gopkg.in/olivere/elastic.v2"
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

func SliceIndex(limit int, predicate func(i int) bool) int {
	for i := 0; i < limit; i++ {
		if predicate(i) {
			return i
		}
	}
	return -1
}

func BuildQuery(args models.SearchArgs, bq *elastic.BoolQuery) error {
	//query au cas où il y'a quelque chose dans la barre de recherche
	Query := elastic.NewMultiMatchQuery(strings.ToLower(args.Search.Query)) //A remplacer par fields[] plus tard
	//query au cas où il n'y a rien dans la barre de recherche
	QueryVide := elastic.NewMatchAllQuery()

	*bq = elastic.NewBoolQuery()
	//si il y'a une recherche à faire sur un ou des termes
	if args.Search.Query != "" {
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
			Query = Query.Field("married_name")
		} else if args.Search.Fields[1] == "fullname" {
			//champs dans lesquels chercher
			Query = Query.Field("firstname")
			Query = Query.Field("surname")
			Query = Query.Field("married_name")
		} else if args.Search.Fields[1] == "street" {
			//champs dans lesquels chercher
			Query = Query.Field("address.street")
			Query = Query.Field("address.city")
		} else if args.Search.Fields[1] == "all" {
			//champs dans lesquels chercher
			Query = Query.Field("surname")
			Query = Query.Field("firstname")
			Query = Query.Field("married_name")
			Query = Query.Field("address.street")
			Query = Query.Field("address.city")
		} else if args.Search.Fields[1] == "city&name" {
			//champs dans lesquels chercher
			Query = Query.Field("address.city")
			Query = Query.Field("surname")
			Query = Query.Field("married_name")
		} else if args.Search.Fields[1] == "city&name&street" {
			//champs dans lesquels chercher
			Query = Query.Field("address.city")
			Query = Query.Field("surname")
			Query = Query.Field("married_name")
			Query = Query.Field("address.street")
		} else if args.Search.Fields[1] == "address_tophits" || args.Search.Fields[1] == "address_aggreg" || args.Search.Fields[1] == "address_aggreg_first_part" || args.Search.Fields[1] == "address" {
			//champs dans lesquels chercher
			Query = Query.Field("address.street")
			Query = Query.Field("address.housenumber")
			Query = Query.Field("address.city")
		}
	}

	// on ajoute à la bool query la requete vide ou pas
	if args.Search.Query != "" {
		*bq = bq.Must(Query)
	} else {
		*bq = bq.Must(QueryVide)
	}

	// filtre la recherche sur un groupe en particulier !!!! pas d'authorisation nécessaire !!!!
	*bq = bq.Must(elastic.NewTermQuery("group_id", args.Search.Fields[0]))

	// contrôle si on est en recherche simple (mobile) ou avancée (desktop)
	// si recherche avancée cad plus de 3 paramêtres dans Fields
	if len(args.Search.Fields) > 4 {

		//--------------------------------gender ------------------------------------------------------------

		var gender_filter = args.Search.Fields[4]
		if gender_filter != "" {
			//bq = bq.Must(elastic.NewTermQuery("gender", gender_filter))
			var dataSlice_gender []string = strings.Split(gender_filter, "/")

			//création d'un array d'interface
			var interfaceSlice_gender []interface{} = make([]interface{}, len(dataSlice_gender))
			//affectation du tableau de sting au tab d'interface
			for i, d := range dataSlice_gender {
				interfaceSlice_gender[i] = d
			}
			//injection de la query
			*bq = bq.Must(elastic.NewTermsQuery("gender", interfaceSlice_gender...))
		}

		//--------------------------------pollingstation ------------------------------------------------------------

		var pollingstation_filter = args.Search.Fields[5]
		if pollingstation_filter != "" {
			//affectation des différentes polling station dans un tableau de string
			var dataSlice_pollingstation []string = strings.Split(pollingstation_filter, "/")
			//vérifie si l'on passé comme argument le mot clé missing. Si c'est le cas (index>-1), alors la requête bq.must n'est pas la même
			var index = SliceIndex(len(dataSlice_pollingstation), func(i int) bool { return dataSlice_pollingstation[i] == "missing" })
			if index > -1 {
				dataSlice_pollingstation = append(dataSlice_pollingstation[:index], dataSlice_pollingstation[index+1:]...)
				dataSlice_pollingstation = append(dataSlice_pollingstation, "")
				//création d'un array d'interface
				var interfaceSlice_pollingstation []interface{} = make([]interface{}, len(dataSlice_pollingstation))
				//affectation du tableau de sting au tab d'interface
				for i, d := range dataSlice_pollingstation {
					interfaceSlice_pollingstation[i] = d
				}
				var bq_child1 elastic.BoolQuery = elastic.NewBoolQuery()
				bq_child1 = bq_child1.Should(elastic.NewFilteredQuery(elastic.NewMatchAllQuery()).Filter(elastic.NewMissingFilter("address.PollingStation")))
				bq_child1 = bq_child1.Should(elastic.NewTermsQuery("address.PollingStation", interfaceSlice_pollingstation...))
				bq_child1 = bq_child1.MinimumShouldMatch("1")
				*bq = bq.Must(bq_child1)
			} else {
				//création d'un array d'interface
				var interfaceSlice_pollingstation []interface{} = make([]interface{}, len(dataSlice_pollingstation))
				//affectation du tableau de sting au tab d'interface
				for i, d := range dataSlice_pollingstation {
					interfaceSlice_pollingstation[i] = d
				}
				//injection de la query
				*bq = bq.Must(elastic.NewTermsQuery("address.PollingStation", interfaceSlice_pollingstation...))
			}
		}
		//--------------------------------Forms ------------------------------------------------------------
		// si la taille est supérieure à 9, c'est que l'on transmet des arguments de filtre de type FORM
		if len(args.Search.Fields) > 11 {
			err := BuildQueryForm(args, bq)
			if err != nil {
				logs.Error(err)
				return err
			}
		}

		//-------------------------age_category & birthdate ----------------------------------------------------

		var agecategory = args.Search.Fields[6]
		if agecategory != "" {

			//affectation des différentes catégories d'âge dans un tableau de string
			var dataSlice_agecategory []string = strings.Split(agecategory, "/")
			// -------- GESTION DE LA GATEGORIE 0 --------------------------------------------
			// on extrait la valeur 0 du slice si elle existe
			var index = SliceIndex(len(dataSlice_agecategory), func(i int) bool { return dataSlice_agecategory[i] == "0" })
			if index > -1 {
				dataSlice_agecategory = append(dataSlice_agecategory[:index], dataSlice_agecategory[index+1:]...)
				var bq_child1 elastic.BoolQuery = elastic.NewBoolQuery()
				bq_child1 = bq_child1.Should(elastic.NewFilteredQuery(elastic.NewMatchAllQuery()).Filter(elastic.NewMissingFilter("birthdate")))
				bq_child1 = bq_child1.Should(elastic.NewFilteredQuery(elastic.NewMatchAllQuery()).Filter(elastic.NewMissingFilter("age_category")))
				bq_child1 = bq_child1.Should(elastic.NewFilteredQuery(elastic.NewMatchAllQuery()).Filter(elastic.NewTermFilter("age_category", "0")))
				bq_child1 = bq_child1.MinimumShouldMatch("2")
				*bq = bq.Should(bq_child1)
			}
			// -------- FIN GESTION DE LA GATEGORIE 0 --------------------------------------------
			//création d'un array d'interface
			var interfaceSlice_agecategory []interface{} = make([]interface{}, len(dataSlice_agecategory))

			// pour chaque catégorie d'âge à filtrer
			for i, category := range dataSlice_agecategory {
				//affectation du tableau de sting au tab d'interface
				//interfaceSlice_agecategory[i] = category
				// injection d'une requête Should pour chaque tranche de birthdate à retenir
				//bq = bq.Should(elastic.NewRangeQuery("birthdate").From("1980-12-01").To("1981-04-01"))
				switch category {
				case "1":
					*bq = bq.Should(elastic.NewRangeQuery("birthdate").Gte("now-18y/d"))
					interfaceSlice_agecategory[i] = category
				case "2":
					*bq = bq.Should(elastic.NewRangeQuery("birthdate").Gte("now-25y/d").Lte("now-18y/d"))
					interfaceSlice_agecategory[i] = category
				case "3":
					*bq = bq.Should(elastic.NewRangeQuery("birthdate").Gte("now-35y/d").Lte("now-25y/d"))
					interfaceSlice_agecategory[i] = category
				case "4":
					*bq = bq.Should(elastic.NewRangeQuery("birthdate").Gte("now-50y/d").Lte("now-35y/d"))
					interfaceSlice_agecategory[i] = category
				case "5":
					*bq = bq.Should(elastic.NewRangeQuery("birthdate").Gte("now-65y/d").Lte("now-50y/d"))
					interfaceSlice_agecategory[i] = category
				case "6":
					*bq = bq.Should(elastic.NewRangeQuery("birthdate").Lte("now-65y/d"))
					interfaceSlice_agecategory[i] = category
				default:
					logs.Critical("wrong age_category parameter")
					err := errors.New("wrong age_category parameter")
					return err
				}
			}
			//injection de la query pour catégorie d'âge
			*bq = bq.Should(elastic.NewTermsQuery("age_category", interfaceSlice_agecategory...))
			*bq = bq.MinimumShouldMatch("1")
		}

		//--------------------------------------LASTCHANGE --------------------------------------------

		if len(args.Search.Fields) > 9 {
			var lastchange_filter = args.Search.Fields[9]
			if lastchange_filter != "" {
				*bq = bq.Must(elastic.NewRangeQuery("lastchange").Gte(lastchange_filter))
			}
		}
		//--------------------------------------EMAIL FILTER --------------------------------------------
		if len(args.Search.Fields) > 10 {
			var email_filter = args.Search.Fields[10]
			if email_filter != "" {
				var dataSlice_email []string = strings.Split(email_filter, "/")
				if len(dataSlice_email) == 1 {
					if dataSlice_email[0] == "SET" {
						*bq = bq.MustNot(elastic.NewFilteredQuery(elastic.NewMatchAllQuery()).Filter(elastic.NewMissingFilter("mail")))
					} else {
						*bq = bq.Must(elastic.NewFilteredQuery(elastic.NewMatchAllQuery()).Filter(elastic.NewMissingFilter("mail")))
					}
				}
			}
		}

	}
	return nil
}

//--------------------------------Forms ------------------------------------------------------------

/*
type + form_id + form_ref_id + exist or not + value or range
for Text -> interfaceSlice_form[]=["TEXT",123,true,666,"bénévole"]
for Text -> interfaceSlice_form[]=["TEXT",123,false,666,"bénévole"]
for Text -> interfaceSlice_form[]=["TEXT",123,true]
for Text -> interfaceSlice_form[]=["TEXT",123,false]

for Radio -> interfaceSlice_form[]=["RADIO",354,false]
for Radio -> interfaceSlice_form[]=["RADIO",354,true]
for Radio -> interfaceSlice_form[]=["RADIO",354,false,123]
for Radio -> interfaceSlice_form[]=["RADIO",354,true,123]

for Checkbox -> interfaceSlice_form[]=["CHECKBOX",123,true]
for Checkbox -> interfaceSlice_form[]=["CHECKBOX",123,false]
for Checkbox -> interfaceSlice_form[]=["CHECKBOX",123,true,333]
for Checkbox -> interfaceSlice_form[]=["CHECKBOX",123,false,333]

for Range -> interfaceSlice_form[]=["RANGE",123,true]
for Range -> interfaceSlice_form[]=["RANGE",123,false]
for Range -> interfaceSlice_form[]=["RANGE",123,true,645,"43"]
for Range -> interfaceSlice_form[]=["RANGE",123,false,645,"43"]
for Range -> interfaceSlice_form[]=["RANGE",123,true,645,"43","98"]
for Range -> interfaceSlice_form[]=["RANGE",123,false,645,"43","98"]

for Date -> interfaceSlice_form[]=["DATE",123,true]
for Date -> interfaceSlice_form[]=["DATE",123,false]
for Date -> interfaceSlice_form[]=["DATE",123,true,645,"23/12/2015"]
for Date -> interfaceSlice_form[]=["DATE",123,false,645,"23/12/2015"]
for Date -> interfaceSlice_form[]=["DATE",123,true,645,"23/12/2015","27/12/2016"]
for Date -> interfaceSlice_form[]=["DATE",123,false,645,"23/12/2015","27/12/2016"]
*/

// method dedicated for the Forms part of the query
func BuildQueryForm(args models.SearchArgs, bq *elastic.BoolQuery) error {
	//pour chacun des forms où l'on souhaite faire une requête

	for i := 10; i < len(args.Search.Fields); i++ {
		var dataSlice_form []string = strings.Split(args.Search.Fields[i], "/")
		//création d'un array d'interface
		var interfaceSlice_form []interface{} = make([]interface{}, len(dataSlice_form))

		//affectation du tableau de sting au tab d'interface
		for index, argument := range dataSlice_form {
			switch index {

			case 0:
				interfaceSlice_form[index] = argument

			case 1:
				temp, err := strconv.Atoi(argument)
				if err != nil {
					logs.Error(err)
					err = errors.New("Contactez le support.(bad arguments in the filtering of forms)-1")
					interfaceSlice_form[index] = argument
					//return err
				}
				interfaceSlice_form[index] = temp

			case 2:
				temp, err := strconv.ParseBool(argument)
				if err != nil {
					logs.Error(err)
					err = errors.New("Contactez le support.(bad arguments in the filtering of forms)-2")
					return err
				}
				interfaceSlice_form[index] = temp

			case 3:
				temp, err := strconv.Atoi(argument)
				if err != nil {
					logs.Error(err)
					err = errors.New("Contactez le support.(bad arguments in the filtering of forms)-1")
					return err
				}
				interfaceSlice_form[index] = temp
			case 4, 5:
				if dataSlice_form[0] == "TEXT" {
					interfaceSlice_form[index] = argument
				}

				if dataSlice_form[0] == "DATE" {
					temp, err := time.Parse(time.RFC3339, argument)
					if err != nil {
						logs.Error(err)
						err = errors.New("Contactez le support.(bad arguments in the filtering of forms)-3")
						return err
					}
					temp2 := int(temp.Unix()) * 1000
					interfaceSlice_form[index] = temp2
				}

				if dataSlice_form[0] == "RANGE" {
					temp, err := strconv.Atoi(argument)
					if err != nil {
						logs.Error(err)
						err = errors.New("Contactez le support.(bad arguments in the filtering of forms)-4")
						return err
					}
					interfaceSlice_form[index] = temp
				}

			case 8:
				if dataSlice_form[0] == "TEXT" {
					interfaceSlice_form[index] = argument
				}
				if dataSlice_form[0] == "DATE" {
					temp, err := strconv.Atoi(argument)
					if err != nil {
						logs.Error(err)
						err = errors.New("Contactez le support.(bad arguments in the filtering of forms)-5")
						return err
					}
					interfaceSlice_form[index] = temp
				}
				if dataSlice_form[0] == "RANGE" {
					temp, err := strconv.Atoi(argument)
					if err != nil {
						logs.Error(err)
						err = errors.New("Contactez le support.(bad arguments in the filtering of forms)-6")
						return err
					}
					interfaceSlice_form[index] = temp
				}

			default:
				interfaceSlice_form[index] = argument
			}
		}
		// si la requête pour Form ne contient que trois éléménts, alors cela est soit une reqûete de présence ou d'absence de formdata (répondu, pas répondu)

		if len(interfaceSlice_form) == 3 {

			var bq_child1 elastic.BoolQuery = elastic.NewBoolQuery()
			var bq_child2 elastic.BoolQuery = elastic.NewBoolQuery()

			if interfaceSlice_form[2].(bool) {

				*bq = bq.Must(elastic.NewTermsQuery("formdatas.form_id", interfaceSlice_form[1]))
			} else {

				bq_child1 = bq_child1.Should(elastic.NewFilteredQuery(elastic.NewMatchAllQuery()).Filter(elastic.NewMissingFilter("formdatas.form_ref_id")))
				bq_child2 = bq_child2.MustNot(elastic.NewTermQuery("formdatas.form_id", interfaceSlice_form[1]))
				bq_child1 = bq_child1.Should(bq_child2)
				bq_child1 = bq_child1.MinimumShouldMatch("1")
				*bq = bq.Must(bq_child1)
				//*bq = bq.Must(elastic.NewFilteredQuery(elastic.NewMatchAllQuery()).Filter(elastic.NewMissingFilter("formdatas.form_ref_id")))
			}
		}
		// si la requête pour Form contient quatre éléménts, alors cela est soit une reqûete pour radio ou checkbox pour vérifier que le form_ref_id correspondant à une valeur est dans le formdata
		if len(interfaceSlice_form) == 4 {

			var bq_child1 elastic.BoolQuery = elastic.NewBoolQuery()
			var bq_child2 elastic.BoolQuery = elastic.NewBoolQuery()
			if interfaceSlice_form[2].(bool) {

				*bq = bq.Must(elastic.NewTermsQuery("formdatas.form_ref_id", interfaceSlice_form[3]))
			} else {

				bq_child1 = bq_child1.Should(elastic.NewFilteredQuery(elastic.NewMatchAllQuery()).Filter(elastic.NewMissingFilter("formdatas.form_ref_id")))
				bq_child2 = bq_child2.MustNot(elastic.NewTermQuery("formdatas.form_ref_id", interfaceSlice_form[3]))
				bq_child1 = bq_child1.Should(bq_child2)
				bq_child1 = bq_child1.MinimumShouldMatch("1")
				*bq = bq.Must(bq_child1)
			}
		}
		// requête avec valeur positionnée ----------------
		if len(interfaceSlice_form) == 5 {
			var bq_child1 elastic.BoolQuery = elastic.NewBoolQuery()
			var bq_child2 elastic.BoolQuery = elastic.NewBoolQuery()
			var bq_child3 elastic.BoolQuery = elastic.NewBoolQuery()
			var bq_child4 elastic.BoolQuery = elastic.NewBoolQuery()
			var bq_child_nested elastic.NestedQuery

			if dataSlice_form[0] == "DATE" {
				var interfaceTemp interface{}
				interfaceTemp = interfaceSlice_form[4].(int) + 86399000
				if interfaceSlice_form[2].(bool) {
					bq_child1 = bq_child1.Must(elastic.NewTermQuery("formdatas.form_ref_id", interfaceSlice_form[3]))
					bq_child1 = bq_child1.Must(elastic.NewRangeQuery("formdatas.data.strictdata").Gte(interfaceSlice_form[4]).Lte(interfaceTemp))
					bq_child_nested = elastic.NewNestedQuery("formdatas").Query(bq_child1)
					*bq = bq.Must(bq_child_nested)
				} else {

					bq_child1 = bq_child1.Should(elastic.NewFilteredQuery(elastic.NewMatchAllQuery()).Filter(elastic.NewMissingFilter("formdatas.form_ref_id")))
					bq_child2 = bq_child2.MustNot(elastic.NewRangeQuery("formdatas.data.strictdata").Gte(interfaceSlice_form[4]).Lte(interfaceTemp))
					bq_child2 = bq_child2.Must(elastic.NewTermQuery("formdatas.form_ref_id", interfaceSlice_form[3]))
					bq_child1 = bq_child1.Should(bq_child2)
					bq_child1 = bq_child1.MinimumShouldMatch("1")
					bq_child_nested = elastic.NewNestedQuery("formdatas").Query(bq_child1)

					bq_child4 = bq_child4.Should(bq_child_nested)

					bq_child3 = bq_child3.MustNot(elastic.NewTermQuery("formdatas.form_ref_id", interfaceSlice_form[3]))
					bq_child4 = bq_child4.Should(bq_child3)

					bq_child4 = bq_child4.MinimumShouldMatch("1")

					*bq = bq.Must(bq_child4)
				}
			} else if dataSlice_form[0] == "TEXT" {
				//pour découper (espace) le query du text afin de faire plusieurs arguments
				var texts = strings.Split(dataSlice_form[4], " ")
				for index, text := range texts {
					if text == "" {
						texts = append(texts[:index], texts[index+1:]...)
					}
				}
				//création d'un array d'interface
				var interfaceSlice_dataSlice_form4 []interface{} = make([]interface{}, len(texts))
				for index, text := range texts {
					interfaceSlice_dataSlice_form4[index] = strings.ToLower(text)
				}

				if interfaceSlice_form[2].(bool) {
					bq_child1 = bq_child1.Must(elastic.NewTermQuery("formdatas.form_ref_id", interfaceSlice_form[3]))
					bq_child1 = bq_child1.Must(elastic.NewTermsQuery("formdatas.data", interfaceSlice_dataSlice_form4...))
					bq_child_nested = elastic.NewNestedQuery("formdatas").Query(bq_child1)
					*bq = bq.Must(bq_child_nested)
				} else {
					bq_child1 = bq_child1.Should(elastic.NewFilteredQuery(elastic.NewMatchAllQuery()).Filter(elastic.NewMissingFilter("formdatas.form_ref_id")))
					bq_child2 = bq_child2.MustNot(elastic.NewTermsQuery("formdatas.data", interfaceSlice_form[4]))
					bq_child2 = bq_child2.Must(elastic.NewTermQuery("formdatas.form_ref_id", interfaceSlice_form[3]))
					bq_child1 = bq_child1.Should(bq_child2)
					bq_child1 = bq_child1.MinimumShouldMatch("1")
					bq_child_nested = elastic.NewNestedQuery("formdatas").Query(bq_child1)

					bq_child4 = bq_child4.Should(bq_child_nested)

					bq_child3 = bq_child3.MustNot(elastic.NewTermQuery("formdatas.form_ref_id", interfaceSlice_form[3]))
					bq_child4 = bq_child4.Should(bq_child3)

					bq_child4 = bq_child4.MinimumShouldMatch("1")

					*bq = bq.Must(bq_child4)
				}

			} else {
				if interfaceSlice_form[2].(bool) {
					bq_child1 = bq_child1.Must(elastic.NewTermQuery("formdatas.form_ref_id", interfaceSlice_form[3]))
					bq_child1 = bq_child1.Must(elastic.NewTermsQuery("formdatas.data.strictdata", interfaceSlice_form[4]))
					bq_child_nested = elastic.NewNestedQuery("formdatas").Query(bq_child1)
					*bq = bq.Must(bq_child_nested)
				} else {

					bq_child1 = bq_child1.Should(elastic.NewFilteredQuery(elastic.NewMatchAllQuery()).Filter(elastic.NewMissingFilter("formdatas.form_ref_id")))
					bq_child2 = bq_child2.MustNot(elastic.NewTermsQuery("formdatas.data.strictdata", interfaceSlice_form[4]))
					bq_child2 = bq_child2.Must(elastic.NewTermQuery("formdatas.form_ref_id", interfaceSlice_form[3]))
					bq_child1 = bq_child1.Should(bq_child2)
					bq_child1 = bq_child1.MinimumShouldMatch("1")

					bq_child_nested = elastic.NewNestedQuery("formdatas").Query(bq_child1)

					bq_child4 = bq_child4.Should(bq_child_nested)

					bq_child3 = bq_child3.MustNot(elastic.NewTermQuery("formdatas.form_ref_id", interfaceSlice_form[3]))
					bq_child4 = bq_child4.Should(bq_child3)

					bq_child4 = bq_child4.MinimumShouldMatch("1")

					*bq = bq.Must(bq_child4)

				}

			}
		}
		// plusieurs dates ou integer--------
		if len(interfaceSlice_form) == 6 {

			var bq_child1 elastic.BoolQuery = elastic.NewBoolQuery()
			var bq_child2 elastic.BoolQuery = elastic.NewBoolQuery()
			var bq_child3 elastic.BoolQuery = elastic.NewBoolQuery()

			if interfaceSlice_form[2].(bool) {
				bq_child1 = bq_child1.Must(elastic.NewTermQuery("formdatas.form_ref_id", interfaceSlice_form[3]))
				bq_child1 = bq_child1.Must(elastic.NewRangeQuery("formdatas.data").Gte(interfaceSlice_form[4]).Lte(interfaceSlice_form[5]))
				*bq = bq.Must(bq_child1)
			} else {

				bq_child1 = bq_child1.Should(elastic.NewFilteredQuery(elastic.NewMatchAllQuery()).Filter(elastic.NewMissingFilter("formdatas.form_ref_id")))
				bq_child2 = bq_child2.MustNot(elastic.NewRangeQuery("formdatas.data").Gte(interfaceSlice_form[4]).Lte(interfaceSlice_form[5]))
				bq_child2 = bq_child2.Must(elastic.NewTermQuery("formdatas.form_ref_id", interfaceSlice_form[3]))
				bq_child1 = bq_child1.Should(bq_child2)
				bq_child3 = bq_child3.MustNot(elastic.NewTermQuery("formdatas.form_ref_id", interfaceSlice_form[3]))
				bq_child1 = bq_child1.Should(bq_child3)
				bq_child1 = bq_child1.MinimumShouldMatch("1")
				*bq = bq.Must(bq_child1)
			}
		}
	}
	return nil
}

//--------------------------------------------------------------------------------------------------------------------

func (s *Search) SearchContacts(args models.SearchArgs, reply *models.SearchReply) error {
	logs.Debug("SearchContacts - search.go")
	logs.Debug(args.Search == nil)
	if args.Search == nil {
		err := errors.New("no args in searchContacts")
		return err
	}
	// utiliser pour savoir si la requête provient d'une adresse vide (street)
	temp_query := args.Search.Query

	//supprimer "undefined" de la requête si ça provient d'une adresse vide (street)
	args.Search.Query = strings.Replace(args.Search.Query, "undefined", "", 1)

	logs.Debug("temp_query:%s", temp_query)

	logs.Debug("args.Search.Query:%s", args.Search.Query)
	logs.Debug("args.Search.Fields:%s", args.Search.Fields)
	logs.Debug("args.Search.Polygon:%s", args.Search.Polygon)

	// TEMPORY PATCH FOR MOBILE COMPATIBILITY 0.1.4 (and inferior) -> delete the 4th parameters of "address" request
	logs.Debug("len(args.Search.Fields):")
	logs.Debug(len(args.Search.Fields))

	if (args.Search.Fields[1] == "address_tophits" || args.Search.Fields[1] == "address_aggreg" || args.Search.Fields[1] == "address_aggreg_first_part" || args.Search.Fields[1] == "address") && len(args.Search.Fields) == 4 {
		logs.Debug("args.Search.Fields[3]:")
		logs.Debug(args.Search.Fields[3])
		args.Search.Fields[3] = "0"
	}

	var bq elastic.BoolQuery
	err := BuildQuery(args, &bq)
	if err != nil {
		logs.Error(err)
		return err
	}

	// donneées à récupérer dans le résultat -----------------------------------
	source := elastic.NewFetchSourceContext(true)
	source = source.Include("id")
	source = source.Include("firstname")
	source = source.Include("surname")
	source = source.Include("married_name")
	source = source.Include("address.street")
	source = source.Include("address.housenumber")
	source = source.Include("address.city")
	source = source.Include("address.postalcode")
	source = source.Include("address.latitude")
	source = source.Include("address.longitude")
	source = source.Include("address.Addition")
	source = source.Include("mail")
	source = source.Include("lastchange")
	source = source.Include("user_id")
	source = source.Include("user_surname")
	source = source.Include("user_firstname")

	//source = source.Include("gender")
	//source = source.Include("birthdate")
	//source = source.Include("phone")
	//source = source.Include("mobile")
	//source = source.Include("mail")
	//source = source.Include("lastchange")
	//source = source.Include("formdatas")

	aggregSource := elastic.NewFetchSourceContext(true)
	aggregSource = aggregSource.Include("id")
	aggregSource = aggregSource.Include("firstname")
	aggregSource = aggregSource.Include("surname")
	aggregSource = aggregSource.Include("married_name")
	aggregSource = aggregSource.Include("address.street")
	aggregSource = aggregSource.Include("address.housenumber")
	aggregSource = aggregSource.Include("address.city")
	aggregSource = aggregSource.Include("address.postalcode")
	aggregSource = aggregSource.Include("address.Addition")
	aggregSource = aggregSource.Include("address.latitude")
	aggregSource = aggregSource.Include("address.longitude")
	aggregSource = aggregSource.Include("formdatas")

	aggregSource_sub := elastic.NewFetchSourceContext(true)
	aggregSource_sub = aggregSource_sub.Include("address.street")
	aggregSource_sub = aggregSource_sub.Include("address.housenumber")
	aggregSource_sub = aggregSource_sub.Include("address.city")
	aggregSource_sub = aggregSource_sub.Include("address.postalcode")
	//aggregSource_sub = aggregSource_sub.Include("address.Addition")
	//aggregSource_sub = aggregSource_sub.Include("address.latitude")
	//aggregSource_sub = aggregSource_sub.Include("address.longitude")

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

	if len(args.Search.Fields) > 8 && args.Search.Fields[7] != "" {
		sort = args.Search.Fields[7]
		if asc2, err := strconv.ParseBool(args.Search.Fields[8]); err == nil {
			asc = asc2
		}
	} else {
		sort = "surname"
		asc = true
	}

	//-------- findcontacts classique -----------------------------------------------

	searchService := s.Client.Search().
		Index("contacts").
		FetchSourceContext(source).
		Query(&bq)

		// address aggs --------------------------------

	if args.Search.Fields[1] == "address_aggreg" {
		aggreg_street := elastic.NewTermsAggregation().Field("address.street.strictdata").Size(size_requete)
		aggreg_lattitude := elastic.NewTermsAggregation().Field("address.location.strictdata").Size(500)
		subaggreg_unique := elastic.NewTopHitsAggregation().Size(500).FetchSourceContext(aggregSource)
		aggreg_lattitude = aggreg_lattitude.SubAggregation("result_subaggreg", subaggreg_unique)
		aggreg_street = aggreg_street.SubAggregation("result_sub_aggreg_latitude", aggreg_lattitude)
		searchService.Size(0).Aggregation("result_aggreg", aggreg_street).Sort("surname", true)

		sourceAgg := aggreg_street.Source()
		data, _ := json.Marshal(sourceAgg)
		fmt.Println("sourceAgg", string(data))

	} else if args.Search.Fields[1] == "address_tophits" || args.Search.Fields[1] == "address" {

		aggreg_housenumber := elastic.NewTermsAggregation().Size(size_requete).Script("try { return Integer.parseInt(_source.address.housenumber); } catch (NumberFormatException e) { return _source.address.housenumber; }")
		subaggreg_unique := elastic.NewTopHitsAggregation().Size(500).FetchSourceContext(aggregSource).Sort("address.location.strictdata", true)

		//TEST JBDA BUG FIX-------------
		aggreg_housenumber_missing := elastic.NewMissingAggregation().Field("address.housenumber")
		subaggreg_unique2 := elastic.NewTopHitsAggregation().Size(500).FetchSourceContext(aggregSource).Sort("address.location.strictdata", true)
		aggreg_housenumber_missing = aggreg_housenumber_missing.SubAggregation("result_sub_aggreg_housenumber_missing", subaggreg_unique2)
		//FIN TEST JBDA BUG FIX-------------

		aggreg_housenumber = aggreg_housenumber.SubAggregation("result_sub_aggreg_housenumber", subaggreg_unique)

		//si jamais ça provient d'une adresse vide, il faut d'abord ne prendre que les adresses vides ...
		if strings.Contains(temp_query, "undefined") {
			logs.Debug("--&&  BUILD AGGREG FOR SECOND PART WITH UNDEFINED  &&--")
			aggreg_street_missing := elastic.NewMissingAggregation().Field("address.street")
			aggreg_street_missing = aggreg_street_missing.SubAggregation("result_aggreg_housenumber_missing", aggreg_housenumber_missing)
			aggreg_street_missing = aggreg_street_missing.SubAggregation("result_aggreg_housenumber", aggreg_housenumber)
			searchService.Size(0).Aggregation("result_aggreg_missing", aggreg_street_missing).Sort("surname", true)

		} else {
			searchService.Size(0).Aggregation("result_aggreg_missing", aggreg_housenumber_missing).Aggregation("result_aggreg", aggreg_housenumber).Sort("surname", true)
		}
		//searchService.Size(0).Aggregation("result_aggreg_missing", aggreg_housenumber_missing).Aggregation("result_aggreg", aggreg_housenumber).Sort("surname", true)

		// sourceAgg := aggreg_housenumber.Source()
		// data, _ := json.Marshal(sourceAgg)
		// fmt.Println("sourceAgg", string(data))

	} else if args.Search.Fields[1] == "address_aggreg_first_part" {
		//elasticsearch request:
		/*
				{
			  "query": {
			    "bool": {
			      "must": [
			        {
			          "multi_match": {
			            "fields": [
			              "address.street",
			              "address.housenumber",
			              "address.city"
			            ],
			            "operator": "and",
			            "query": "ara",
			            "tie_breaker": 0,
			            "type": "cross_fields"
			          }
			        },
			        {
			          "term": {
			            "group_id": "1003"
			          }
			        }
			      ]
			    }
			  },
			  "aggs": {
			    "street": {
			      "terms": {
			        "field": "address.street.strictdata",
			        "size": 20
			      },
			      "aggs": {
			        "house": {
			          "terms": {
			            "script": "try { return Integer.parseInt(_source.address.housenumber); } catch (NumberFormatException e) { return _source.address.housenumber; }",
			            "size": 20,
			            "order": {
			              "_term": "desc"
			            }
			          },
			          "aggs": {
			            "latitude": {
			              "top_hits": {
			                "size": 2000,
			                "sort": "address.location.strictdata",
			                "_source": [
			                  "id",
			                  "surname",
			                  "address.id",
			                  "address.housenumber",
			                  "address.street",
			                  "address.city",
			                  "address.postalcode",
			                  "address.latitude"
			                ]
			              }
			            }
			          }
			        }
			      }
			    }
			  },
			  "from": 0,
			  "size": 0,
			  "sort": [
			    "address.street",
			    "address.housenumber"
			  ]
			}
		*/

		aggreg_street := elastic.NewTermsAggregation().Field("address.street.strictdata").Size(size_requete - 1) //-1 pour prendre en compte une adresse vide si jamais
		aggreg_street_missing := elastic.NewMissingAggregation().Field("address.street")

		aggreg_housenumber := elastic.NewTermsAggregation().Size(400).Script("try { return Integer.parseInt(_source.address.housenumber); } catch (NumberFormatException e) { return _source.address.housenumber; }")
		subaggreg_unique := elastic.NewTopHitsAggregation().Size(1).FetchSourceContext(aggregSource_sub).Sort("address.location.strictdata", true)
		aggreg_housenumber = aggreg_housenumber.SubAggregation("result_sub_aggreg_housenumber", subaggreg_unique)

		//TEST JBDA BUG FIX-------------
		aggreg_housenumber_missing := elastic.NewMissingAggregation().Field("address.housenumber")
		subaggreg_unique2 := elastic.NewTopHitsAggregation().Size(1).FetchSourceContext(aggregSource_sub).Sort("address.location.strictdata", true)
		aggreg_housenumber_missing = aggreg_housenumber_missing.SubAggregation("result_sub_aggreg_housenumber_missing", subaggreg_unique2)

		aggreg_street = aggreg_street.SubAggregation("result_sub_aggreg_street_missing", aggreg_housenumber_missing)

		//FIN TEST JBDA BUG FIX-------------
		aggreg_street = aggreg_street.SubAggregation("result_sub_aggreg_street", aggreg_housenumber)

		aggreg_street_missing = aggreg_street_missing.SubAggregation("result_sub_aggreg_street_missing", aggreg_housenumber_missing)
		aggreg_street_missing = aggreg_street_missing.SubAggregation("result_sub_aggreg_street", aggreg_housenumber)

		searchService.Size(0).Aggregation("result_aggreg", aggreg_street).Aggregation("result_aggreg_missing", aggreg_street_missing).Sort("surname", true)

		/*
			aggreg_street := elastic.NewTermsAggregation().Field("address.street.strictdata").Size(size_requete)
			aggreg_lattitude := elastic.NewTermsAggregation().Field("address.location.strictdata").Size(1000)
			subaggreg_unique := elastic.NewTopHitsAggregation().Size(1).FetchSourceContext(aggregSource_sub)
			aggreg_lattitude = aggreg_lattitude.SubAggregation("result_subaggreg", subaggreg_unique)
		*/
		//sourceAgg := aggreg_street.Source()
		//data, _ := json.Marshal(sourceAgg)
		//fmt.Println("sourceAgg", string(data))

	} else {
		searchService.Size(size_requete).
			From(from_requete).
			Sort(sort, asc).
			Pretty(true)
	}

	/*
		 if (args.Search.Fields[1]=="address"){
			 	logs.Debug("ADD AGGREGATION ADDRESS")
			 	aggreg_lattitude := elastic.NewTermsAggregation().Field("address.street")
			 	subaggreg_unique := elastic.NewTopHitsAggregation().Size(size_requete)
				//subaggreg_unique := elastic.NewTopHitsAggregation()
			 	aggreg_lattitude = aggreg_lattitude.SubAggregation("result_subaggreg", subaggreg_unique)
				searchService.Size(0).Aggregation("result_aggreg", aggreg_lattitude).Sort("surname", true)
				sourceAgg := aggreg_lattitude.Source()
				data, _ := json.Marshal(sourceAgg)
				fmt.Println("sourceAgg", string(data))
		 }else{
			 	searchService.Size(size_requete).
			  From(from_requete).
			  Sort(sort, asc).
			  Pretty(true)
		 }
	*/
	//-------------manage polygon Filter--------------------------

	if len(args.Search.Polygon) > 0 {
		Filter := elastic.NewGeoPolygonFilter("location")
		var point models.Point
		for _, point = range args.Search.Polygon {
			geoPoint := elastic.GeoPointFromLatLon(point.Lat, point.Lng)
			Filter = Filter.AddPoint(geoPoint)
		}
		searchService.PostFilter(Filter)
	}

	//var searchResult = elastic.SearchResult{}
	// searchResult, err := s.Client.Search().
	// Index("contacts").
	// FetchSourceContext(source).
	// Query(&bq).
	// PostFilter(Filter).
	// Size(size_requete).
	// From(from_requete).
	// Sort(sort, asc).
	// Pretty(true).
	// Do()

	searchResult, err := searchService.
		Do()

	if err != nil {
		logs.Critical(err)
		return err
	}
	sourceQuery := bq.Source()
	data, _ := json.Marshal(sourceQuery)
	fmt.Println("sourceQuery", string(data))

	// traitements des hits --------------------------------
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

	// traitement aggrégations -> address aggs -----------------
	if args.Search.Fields[1] == "address_aggreg" || args.Search.Fields[1] == "address_aggreg_first_part" {
		logs.Debug("----------------------ENTER FIRST PART----------------------------")

		//------------------------------------result_aggreg_missing-------------------------------------
		agg_missing, found := searchResult.Aggregations.Terms("result_aggreg_missing")
		if !found {
			logs.Debug("we sould have a terms aggregation called %q", "result_aggreg_missing")
		} else {
			//data, _ := json.Marshal(agg_missing)
			//fmt.Println("FIRST PART :  result_aggreg_missing : ", string(data))
			var ff models.AddressStreetAggReply

			logs.Debug("Enter : street_notmissing")
			street_notmissing, found := agg_missing.Terms("result_sub_aggreg_street")
			if found {
				//data, _ := json.Marshal(street_notmissing)
				//fmt.Println("result_sub_aggreg_street : ", string(data))

				for _, subbucket := range street_notmissing.Buckets {
					subaggreg_unique, found := subbucket.TopHits("result_sub_aggreg_housenumber")
					if found {
						//data, _ := json.Marshal(subaggreg_unique)
						//fmt.Println("result_sub_aggreg_housenumber : ", string(data))
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
						if len(cs.Contacts) > 0 {
							ff.Addresses = append(ff.Addresses, cs)
						}
					} else {
						logs.Error("result_sub_aggreg_housenumber NOT FOUND in address_aggreg or address_aggreg_first_part")
					}
					subaggreg_unique2, found := subbucket.TopHits("result_sub_aggreg_housenumber_missing")
					if found {
						//data, _ := json.Marshal(subaggreg_unique2)
						//fmt.Println("result_sub_aggreg_housenumber_missing : ", string(data))
						// pour chaque addresse aggrégée
						var cs models.AddressAggReply
						for _, addresse := range subaggreg_unique2.Hits.Hits {
							//on utilise le modèle Contact uniquement pour stocker l'adresse aggrégée
							var c models.Contact
							err := json.Unmarshal(*addresse.Source, &c)
							if err != nil {
								logs.Error(err)
								return err
							}
							cs.Contacts = append(cs.Contacts, c)
						}
						if len(cs.Contacts) > 0 {
							ff.Addresses = append(ff.Addresses, cs)
						}
					} else {
						logs.Error("result_sub_aggreg_housenumber NOT FOUND in address_aggreg or address_aggreg_first_part")
					}
				}
			}

			logs.Debug("Enter : street_missing")
			street_missing, found := agg_missing.Aggregations.Terms("result_sub_aggreg_street_missing")
			if found {
				//data, _ := json.Marshal(street_missing)
				//fmt.Println("street_missing: ", string(data))

				street_missing_number, found := street_missing.TopHits("result_sub_aggreg_housenumber")
				if found {
					logs.Debug("Enter : street_missing_number")
					var cs models.AddressAggReply
					for _, addresse := range street_missing_number.Hits.Hits {
						//logs.Debug("#########################################AA1")
						//data, _ := json.Marshal(addresse)
						//fmt.Println("adresse missing : ", string(data))
						//on utilise le modèle Contact uniquement pour stocker l'adresse aggrégée
						var c models.Contact
						err := json.Unmarshal(*addresse.Source, &c)
						if err != nil {
							logs.Error(err)
							return err
						}
						cs.Contacts = append(cs.Contacts, c)
					}
					if len(cs.Contacts) > 0 {
						ff.Addresses = append(ff.Addresses, cs)
					}
					//fin de boucle, je rajoute les "contacts" à l'adresse :

				}
				street_number_missing, found2 := street_missing.TopHits("result_sub_aggreg_housenumber_missing")
				if found2 {
					//logs.Debug("#########################################BB")
					var cs models.AddressAggReply
					for _, addresse := range street_number_missing.Hits.Hits {
						//logs.Debug("#########################################BB1")
						//data, _ := json.Marshal(addresse)
						//fmt.Println("adresse missing : ", string(data))
						//on utilise le modèle Contact uniquement pour stocker l'adresse aggrégée
						var c models.Contact
						err := json.Unmarshal(*addresse.Source, &c)
						if err != nil {
							logs.Error(err)
							return err
						}
						cs.Contacts = append(cs.Contacts, c)
					}
					//fin de boucle, je rajoute les "contacts" à l'adresse :
					if len(cs.Contacts) > 0 {
						ff.Addresses = append(ff.Addresses, cs)
					}

				}

				//}
			}
			if len(ff.Addresses) > 0 {
				reply.AddressStreetAggs = append(reply.AddressStreetAggs, ff)
				//logs.Debug(reply.AddressStreetAggs)
			}
		}
		//---------------------------------------------result_aggreg------------------------------------------
		agg, found := searchResult.Aggregations.Terms("result_aggreg")
		if !found {
			logs.Debug("we sould have a terms aggregation called %q", "result_aggreg")
		} else {
			if searchResult.Aggregations != nil {
				for _, bucket := range agg.Buckets {
					//data, _ := json.Marshal(bucket)
					//fmt.Println("bucket: ", string(data))
					//fmt.Println("args.Search.Fields[1]: ", args.Search.Fields[1])
					//logs.Debug("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")

					//logs.Debug("ENTER")
					var ff models.AddressStreetAggReply

					toto, found := bucket.Terms("result_sub_aggreg_street")
					if found {
						//data, _ := json.Marshal(toto)
						//fmt.Println("PRESENT!!!!!!!! 1 : ", string(data))

						//var ff models.AddressStreetAggReply
						for _, subbucket := range toto.Buckets {
							//data, _ := json.Marshal(subbucket)
							//fmt.Println("PRESENT!!!!!!!! 2 : ", string(data))
							//subaggreg_unique, found := bucket.TopHits("result_subaggreg")
							subaggreg_unique, found := subbucket.TopHits("result_sub_aggreg_housenumber")
							if found {
								//data, _ := json.Marshal(subaggreg_unique)
								//fmt.Println("PRESENT!!!!!!!! 3 : ", string(data))
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
								if len(cs.Contacts) > 0 {
									ff.Addresses = append(ff.Addresses, cs)
								}
							} else {
								logs.Error("result_sub_aggreg_housenumber NOT FOUND in address_aggreg or address_aggreg_first_part")
							}

						}
						//reply.AddressStreetAggs = append(reply.AddressStreetAggs, ff)
					} else {
						logs.Error("result_sub_aggreg_street NOT FOUND in address_aggreg or address_aggreg_first_part")
					}

					titi, found := bucket.Terms("result_sub_aggreg_street_missing")
					if found {
						//data, _ := json.Marshal(titi)
						//fmt.Println("MISSSSSSIIIINNNNGGGG!!!!!!!! 1: ", string(data))
						//var ff models.AddressStreetAggReply

						//for _, subbucket := range titi.Buckets {
						//manage aggreg for addresses with no house number :
						subaggreg_unique2, found := titi.TopHits("result_sub_aggreg_housenumber_missing")
						if found {
							// pour chaque addresse aggrégée
							//data, _ := json.Marshal(subaggreg_unique2)
							//fmt.Println("MISSSSSSIIIINNNNGGGG!!!!!!!! 2: ", string(data))
							var cs models.AddressAggReply
							for _, addresse := range subaggreg_unique2.Hits.Hits {
								//data, _ := json.Marshal(addresse)
								//fmt.Println("adresse missing : ", string(data))
								//on utilise le modèle Contact uniquement pour stocker l'adresse aggrégée
								var c models.Contact
								err := json.Unmarshal(*addresse.Source, &c)
								if err != nil {
									logs.Error(err)
									return err
								}
								cs.Contacts = append(cs.Contacts, c)
							}
							//fin de boucle, je rajoute les "contacts" à l'adresse :
							if len(cs.Contacts) > 0 {
								ff.Addresses = append(ff.Addresses, cs)
							}
						} else {
							logs.Error("result_sub_aggreg_housenumber_missing NOT FOUND in address_aggreg or address_aggreg_first_part")
						}
						//}
						//reply.AddressStreetAggs = append(reply.AddressStreetAggs, ff)
					} else {
						logs.Error("result_sub_aggreg_street_missing NOT FOUND in address_aggreg or address_aggreg_first_part")
					}
					if len(ff.Addresses) > 0 {
						reply.AddressStreetAggs = append(reply.AddressStreetAggs, ff)
					}
					//logs.Debug(reply.AddressStreetAggs)

				} //fin for buckets
			} else {
				reply.Contacts = nil
			}
		} //fin else !found
		logs.Debug("----------------------END FIRST PART--------------------------")
		//---------------------------------------------SECOND PART-----------------------
	} else {
		logs.Debug("----------------------ENTER SECOND PART---------------------------")
		//logs.Debug(args.Search.Fields[1])
		agg, found := searchResult.Aggregations.Terms("result_aggreg")

		if !found {
			logs.Debug("we sould have a terms aggregation called %q", "result_aggreg")
		} else {
			logs.Debug("//////// ENTER result_aggreg ////////")
			//data, _ := json.Marshal(agg)
			//fmt.Println("result_aggreg: ", string(data))
			if searchResult.Aggregations != nil {
				for _, bucket := range agg.Buckets {
					//data, _ := json.Marshal(bucket)
					//fmt.Println("bucket: ", string(data))
					//fmt.Println("args.Search.Fields[1]: ", args.Search.Fields[1])
					//logs.Debug("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
					//logs.Debug("ACHTUNGGGGGGG8!!!!!!!!!!!")
					subaggreg_unique, found := bucket.TopHits("result_sub_aggreg_housenumber")
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
					} else {
						logs.Error("result_sub_aggreg_housenumber NOT FOUND")
					}

				}
			}
		}
		agg_missing, found := searchResult.Aggregations.Terms("result_aggreg_missing")
		//agg, found := searchResult.Aggregations.Terms("result_aggreg")
		if !found {
			logs.Debug("we sould have a terms aggregation called %q", "result_aggreg_missing")
		} else {
			logs.Debug("//////// ENTER result_aggreg_missing ////////")

			if strings.Contains(temp_query, "undefined") {
				logs.Debug("################## undefined #######################")
				titi, found := agg_missing.Aggregations.Terms("result_aggreg_housenumber")
				toto, found2 := agg_missing.Aggregations.Terms("result_aggreg_housenumber_missing")

				if found {
					//logs.Debug("#########################################3")
					//logs.Debug(titi)
					//var ff models.AddressStreetAggReply
					for _, bucket := range titi.Buckets {
						//logs.Debug("#########################################4")
						//logs.Debug(bucket)
						subaggreg_unique, found := bucket.TopHits("result_sub_aggreg_housenumber")
						if found {
							//logs.Debug("#########################################5")
							var cs models.AddressAggReply
							for _, addresse := range subaggreg_unique.Hits.Hits {
								//logs.Debug("#########################################6")
								//data, _ := json.Marshal(addresse)
								//fmt.Println("adresse missing : ", string(data))
								//on utilise le modèle Contact uniquement pour stocker l'adresse aggrégée
								var c models.Contact
								err := json.Unmarshal(*addresse.Source, &c)
								if err != nil {
									logs.Error(err)
									return err
								}
								cs.Contacts = append(cs.Contacts, c)
							}
							//fin de boucle, je rajoute les "contacts" à l'adresse :
							if len(cs.Contacts) > 0 {
								//ff.Addresses = append(ff.Addresses, cs)
								reply.AddressAggs = append(reply.AddressAggs, cs)
							}
						}
						subaggreg_unique2, found2 := bucket.TopHits("result_sub_aggreg_housenumber_missing")
						if found2 {
							//logs.Debug("#########################################7")
							var cs models.AddressAggReply
							for _, addresse := range subaggreg_unique2.Hits.Hits {
								//logs.Debug("#########################################8")
								//data, _ := json.Marshal(addresse)
								//fmt.Println("adresse missing : ", string(data))
								//on utilise le modèle Contact uniquement pour stocker l'adresse aggrégée
								var c models.Contact
								err := json.Unmarshal(*addresse.Source, &c)
								if err != nil {
									logs.Error(err)
									return err
								}
								cs.Contacts = append(cs.Contacts, c)
							}
							//fin de boucle, je rajoute les "contacts" à l'adresse :
							if len(cs.Contacts) > 0 {
								//ff.Addresses = append(ff.Addresses, cs)
								reply.AddressAggs = append(reply.AddressAggs, cs)
							}

						}
					}
					//reply.AddressStreetAggs = append(reply.AddressStreetAggs, ff)
				}

				if found2 {
					//logs.Debug("#########################################10")
					//logs.Debug(toto)
					//data, _ := json.Marshal(toto)
					//fmt.Println("toto : ", string(data))
					//var ff models.AddressStreetAggReply

					//logs.Debug("#########################################11")
					subaggreg_unique, found := toto.TopHits("result_sub_aggreg_housenumber")
					if found {
						//logs.Debug("#########################################12")
						var cs models.AddressAggReply
						for _, addresse := range subaggreg_unique.Hits.Hits {
							//logs.Debug("#########################################13")
							//data, _ := json.Marshal(addresse)
							//fmt.Println("adresse missing : ", string(data))
							//on utilise le modèle Contact uniquement pour stocker l'adresse aggrégée
							var c models.Contact
							err := json.Unmarshal(*addresse.Source, &c)
							if err != nil {
								logs.Error(err)
								return err
							}
							cs.Contacts = append(cs.Contacts, c)
						}
						//fin de boucle, je rajoute les "contacts" à l'adresse :
						if len(cs.Contacts) > 0 {
							reply.AddressAggs = append(reply.AddressAggs, cs)
							//ff.Addresses = append(ff.Addresses, cs)
						}
					}
					subaggreg_unique2, found2 := toto.TopHits("result_sub_aggreg_housenumber_missing")
					if found2 {
						//logs.Debug("#########################################14")
						var cs models.AddressAggReply
						for _, addresse := range subaggreg_unique2.Hits.Hits {
							//logs.Debug("#########################################15")
							//data, _ := json.Marshal(addresse)
							//fmt.Println("adresse missing : ", string(data))
							//on utilise le modèle Contact uniquement pour stocker l'adresse aggrégée
							var c models.Contact
							err := json.Unmarshal(*addresse.Source, &c)
							if err != nil {
								logs.Error(err)
								return err
							}
							cs.Contacts = append(cs.Contacts, c)
						}
						//fin de boucle, je rajoute les "contacts" à l'adresse :
						if len(cs.Contacts) > 0 {
							reply.AddressAggs = append(reply.AddressAggs, cs)
							//ff.Addresses = append(ff.Addresses, cs)
						}

					}

					//reply.AddressStreetAggs = append(reply.AddressStreetAggs, ff)
				}
				// FIN : --- if strings.Contains(temp_query, "undefined") ----
			} else {
				subaggreg_unique_missing, found := agg_missing.TopHits("result_sub_aggreg_housenumber_missing")
				if found {
					// pour chaque addresse aggrégée
					var cs models.AddressAggReply
					for _, addresse := range subaggreg_unique_missing.Hits.Hits {
						//on utilise le modèle Contact uniquement pour stocker l'adresse aggrégée
						var c models.Contact
						err := json.Unmarshal(*addresse.Source, &c)
						if err != nil {
							logs.Error(err)
							return err
						}

						cs.Contacts = append(cs.Contacts, c)
					}
					//fin de boucle, je rajoute les "contacts" à l'adresse :
					if len(cs.Contacts) > 0 {
						reply.AddressAggs = append(reply.AddressAggs, cs)
					}

				} else {
					logs.Error("result_sub_aggreg_housenumber_missing NOT FOUND")
				}
			}
		}
	}

	return nil
}

//-------------------------------------------------------------------------------------------------

func (s *Search) KpiContacts(args models.SearchArgs, reply *models.SearchReply) error {
	logs.Debug("KpiContacts - search.go")
	logs.Debug("args.Search.Query:%s", args.Search.Query)
	logs.Debug("args.Search.Fields:%s", args.Search.Fields)

	var bq elastic.BoolQuery

	//construction de la query - commun avec findcontacts----------
	err := BuildQuery(args, &bq)
	if err != nil {
		logs.Error(err)
		return err
	}

	var refInterval []string
	refInterval = append(refInterval, "now/d")      //0
	refInterval = append(refInterval, "now-18y/d")  //1
	refInterval = append(refInterval, "now-25y/d")  //2
	refInterval = append(refInterval, "now-35y/d")  //3
	refInterval = append(refInterval, "now-50y/d")  //4
	refInterval = append(refInterval, "now-65y/d")  //5
	refInterval = append(refInterval, "now-150y/d") //6

	//----------------------------------------------------------------------------------
	//aggregation pour KPI

	aggreg_kpi_gender := elastic.NewTermsAggregation().Field("gender")
	aggreg_kpi_gender_missing := elastic.NewMissingAggregation().Field("gender")

	aggreg_kpi_pollingstation := elastic.NewTermsAggregation().Field("address.PollingStation").Size(500)
	aggreg_kpi_pollingstation_missing := elastic.NewMissingAggregation().Field("address.PollingStation")

	aggreg_kpi_agecategory := elastic.NewTermsAggregation().Field("age_category")
	aggreg_kpi_lastchange := elastic.NewDateHistogramAggregation().Field("lastchange").Interval("week")
	//aggreg_kpi_birthdate := elastic.NewDateHistogramAggregation().Field("birthdate").Interval("year")

	//var aggreg_kpi_birthdate [7]elastic.DateRangeAggregation
	var aggreg_kpi_birthdate [7]interface{}
	aggreg_kpi_birthdate[0] = elastic.NewMissingAggregation().Field("birthdate")
	for index_agecat := 1; index_agecat < 7; index_agecat++ {
		aggreg_kpi_birthdate[index_agecat] = elastic.NewDateRangeAggregation().Field("birthdate").Between(refInterval[index_agecat], refInterval[index_agecat-1])
	}

	aggreg_kpi_without_email := elastic.NewMissingAggregation().Field("mail")
	aggreg_kpi_without_tel := elastic.NewMissingAggregation().Field("phone")

	//--------------------------------------------------------------------------------------------------------------------

	searchService := s.Client.Search().
		Index("contacts").
		Size(0).
		Aggregation("gender_aggreg", aggreg_kpi_gender).
		Aggregation("gender_missing_aggreg", aggreg_kpi_gender_missing).
		Aggregation("pollingstation_aggreg", aggreg_kpi_pollingstation).
		Aggregation("pollingstation_missing_aggreg", aggreg_kpi_pollingstation_missing).
		Aggregation("agecategory_aggreg", aggreg_kpi_agecategory).
		//Aggregation("birthdate_aggreg", aggreg_kpi_birthdate).
		Aggregation("lastchange_aggreg", aggreg_kpi_lastchange).
		Aggregation("0_aggreg", aggreg_kpi_birthdate[0].(elastic.MissingAggregation)).
		Aggregation("1_aggreg", aggreg_kpi_birthdate[1].(elastic.DateRangeAggregation)).
		Aggregation("2_aggreg", aggreg_kpi_birthdate[2].(elastic.DateRangeAggregation)).
		Aggregation("3_aggreg", aggreg_kpi_birthdate[3].(elastic.DateRangeAggregation)).
		Aggregation("4_aggreg", aggreg_kpi_birthdate[4].(elastic.DateRangeAggregation)).
		Aggregation("5_aggreg", aggreg_kpi_birthdate[5].(elastic.DateRangeAggregation)).
		Aggregation("6_aggreg", aggreg_kpi_birthdate[6].(elastic.DateRangeAggregation)).
		// aggregation email par
		Aggregation("contacts_sans_email_aggreg", aggreg_kpi_without_email).
		Aggregation("contacts_sans_tel_aggreg", aggreg_kpi_without_tel)

	Filter := elastic.NewGeoPolygonFilter("location")
	if len(args.Search.Polygon) > 0 {
		var point models.Point
		for _, point = range args.Search.Polygon {
			geoPoint := elastic.GeoPointFromLatLon(point.Lat, point.Lng)
			Filter = Filter.AddPoint(geoPoint)
		}
		searchService.Query(elastic.NewFilteredQuery(bq).Filter(Filter))
	} else {
		searchService.Query(&bq)
	}
	searchResult, err := searchService.
		Do()

	//--------------------------------------------------------------------------------------------------------------------

	// searchResult, err := s.Client.Search().
	// 	Index("contacts").
	// 	//FetchSourceContext(source).
	// 	Query(toto).
	// 	Size(0).
	// 	Aggregation("gender_aggreg", aggreg_kpi_gender).
	// 	Aggregation("gender_missing_aggreg", aggreg_kpi_gender_missing).
	// 	Aggregation("pollingstation_aggreg", aggreg_kpi_pollingstation).
	// 	Aggregation("pollingstation_missing_aggreg", aggreg_kpi_pollingstation_missing).
	// 	Aggregation("agecategory_aggreg", aggreg_kpi_agecategory).
	// 	//Aggregation("birthdate_aggreg", aggreg_kpi_birthdate).
	// 	Aggregation("lastchange_aggreg", aggreg_kpi_lastchange).
	// 	Aggregation("0_aggreg", aggreg_kpi_birthdate[0].(elastic.MissingAggregation)).
	// 	Aggregation("1_aggreg", aggreg_kpi_birthdate[1].(elastic.DateRangeAggregation)).
	// 	Aggregation("2_aggreg", aggreg_kpi_birthdate[2].(elastic.DateRangeAggregation)).
	// 	Aggregation("3_aggreg", aggreg_kpi_birthdate[3].(elastic.DateRangeAggregation)).
	// 	Aggregation("4_aggreg", aggreg_kpi_birthdate[4].(elastic.DateRangeAggregation)).
	// 	Aggregation("5_aggreg", aggreg_kpi_birthdate[5].(elastic.DateRangeAggregation)).
	// 	Aggregation("6_aggreg", aggreg_kpi_birthdate[6].(elastic.DateRangeAggregation)).
	// 	Do()

	if err != nil {
		logs.Critical(err)
		return err
	}

	gender_agg, found := searchResult.Aggregations.Terms("gender_aggreg")
	gender_missing_agg, found1 := searchResult.Aggregations.Missing("gender_missing_aggreg")

	pollingstation_agg, found2 := searchResult.Aggregations.Terms("pollingstation_aggreg")
	pollingstation_missing_agg, found2bis := searchResult.Aggregations.Missing("pollingstation_missing_aggreg")

	agecategory_agg, found3 := searchResult.Aggregations.Terms("agecategory_aggreg")
	lastchange_agg, found5 := searchResult.Aggregations.DateHistogram("lastchange_aggreg")
	//birthdate_agg, found4 := searchResult.Aggregations.DateHistogram("birthdate_aggreg")

	a0_agg, found6 := searchResult.Aggregations.Missing("0_aggreg")
	a1_agg, found7 := searchResult.Aggregations.DateRange("1_aggreg")
	a2_agg, found8 := searchResult.Aggregations.DateRange("2_aggreg")
	a3_agg, found9 := searchResult.Aggregations.DateRange("3_aggreg")
	a4_agg, found10 := searchResult.Aggregations.DateRange("4_aggreg")
	a5_agg, found11 := searchResult.Aggregations.DateRange("5_aggreg")
	a6_agg, found12 := searchResult.Aggregations.DateRange("6_aggreg")

	contacts_sans_email_agg, found13 := searchResult.Aggregations.Missing("contacts_sans_email_aggreg")
	contacts_sans_tel_agg, found14 := searchResult.Aggregations.Missing("contacts_sans_tel_aggreg")

	if !found {
		logs.Error("we sould have a terms aggregation called %q", "gender_aggreg")
	}
	if !found1 {
		logs.Error("we sould have a terms aggregation called %q", "gender_missing_aggreg")
	}
	if !found2 {
		logs.Error("we sould have a terms aggregation called %q", "pollingstation_aggreg")
	}
	if !found2bis {
		logs.Error("we sould have a terms aggregation called %q", "pollingstation_missing_aggreg")
	}
	if !found3 {
		logs.Error("we sould have a terms aggregation called %q", "agecategory_aggreg")
	}
	// if !found4 {
	// 	logs.Debug("we sould have a terms aggregation called %q", "birthdate_aggreg")
	// }
	if !found5 {
		logs.Error("we sould have a terms aggregation called %q", "lastchange_aggreg")
	}
	if !found6 {
		logs.Error("we sould have a terms aggregation called %q", "0_aggreg")
	}
	if !found7 {
		logs.Error("we sould have a terms aggregation called %q", "1_aggreg")
	}
	if !found8 {
		logs.Error("we sould have a terms aggregation called %q", "2_aggreg")
	}
	if !found9 {
		logs.Error("we sould have a terms aggregation called %q", "3_aggreg")
	}
	if !found10 {
		logs.Error("we sould have a terms aggregation called %q", "4_aggreg")
	}
	if !found11 {
		logs.Error("we sould have a terms aggregation called %q", "5_aggreg")
	}
	if !found12 {
		logs.Error("we sould have a terms aggregation called %q", "6_aggreg")
	}
	if !found13 {
		logs.Error("we sould have a terms aggregation called %q", "contacts_sans_email_aggreg")
	}
	if !found14 {
		logs.Error("we sould have a terms aggregation called %q", "contacts_sans_tel_aggreg")
	}

	if searchResult.Aggregations != nil {
		var tab_kpiAtom models.KpiAggs

		// ---- stockage nombre de résultats de la requête ----------------------
		if searchResult.Hits != nil {
			var kpiAtom models.KpiReply
			kpiAtom.Key = "total"
			kpiAtom.Doc_count = searchResult.Hits.TotalHits
			tab_kpiAtom.KpiReplies = append(tab_kpiAtom.KpiReplies, kpiAtom)
			reply.Kpi = append(reply.Kpi, tab_kpiAtom)
			tab_kpiAtom = models.KpiAggs{}
		}

		// ---- stockage réponses pour gender_aggreg ----------------------
		for _, bucket := range gender_agg.Buckets {
			var kpiAtom models.KpiReply
			kpiAtom.Key = bucket.Key.(string)
			kpiAtom.Doc_count = bucket.DocCount
			tab_kpiAtom.KpiReplies = append(tab_kpiAtom.KpiReplies, kpiAtom)
		}
		//---- gestion du missing gender --------
		var kpiAtom models.KpiReply
		kpiAtom.Key = "missing"
		kpiAtom.Doc_count = gender_missing_agg.DocCount
		if gender_missing_agg.DocCount > 0 {
			tab_kpiAtom.KpiReplies = append(tab_kpiAtom.KpiReplies, kpiAtom)
		}
		// ---------------------------------------
		reply.Kpi = append(reply.Kpi, tab_kpiAtom)
		tab_kpiAtom = models.KpiAggs{}

		// ---- stockage réponses pour pollingstation_aggreg -----------------------
		for _, bucket := range pollingstation_agg.Buckets {
			var kpiAtom models.KpiReply
			if bucket.Key.(string) == "" {
				kpiAtom.Key = "missing"
			} else {
				kpiAtom.Key = bucket.Key.(string)
			}
			kpiAtom.Doc_count = bucket.DocCount
			tab_kpiAtom.KpiReplies = append(tab_kpiAtom.KpiReplies, kpiAtom)
		}
		//---- gestion du missing pollingstation --------
		var kpiAtom2 models.KpiReply
		kpiAtom2.Key = "missing"
		kpiAtom2.Doc_count = pollingstation_missing_agg.DocCount
		if pollingstation_missing_agg.DocCount > 0 {
			tab_kpiAtom.KpiReplies = append(tab_kpiAtom.KpiReplies, kpiAtom2)
		}
		// ---------------------------------------
		reply.Kpi = append(reply.Kpi, tab_kpiAtom)
		tab_kpiAtom = models.KpiAggs{}

		// ---- stockage réponses pour agecategory_aggreg -----------------------

		// for _, bucket := range agecategory_agg.Buckets {
		// 	var kpiAtom models.KpiReply
		// 	kpiAtom.Key= strconv.FormatFloat(bucket.Key.(float64), 'f', -1, 64)
		// 	kpiAtom.Doc_count=bucket.DocCount
		// 	tab_kpiAtom.KpiReplies=append(tab_kpiAtom.KpiReplies, kpiAtom)
		// }
		// reply.Kpi=append(reply.Kpi, tab_kpiAtom)
		// tab_kpiAtom = models.KpiAggs{}

		// ---- stockage réponses pour agecategory_aggreg -----------------------
		// on récupère les données issus du champ age category dans un tab temporaire
		var tab_category_Age [7]int64
		var sum_category_Age int64
		for _, bucket := range agecategory_agg.Buckets {
			tab_category_Age[int(bucket.Key.(float64))] = bucket.DocCount
			sum_category_Age = sum_category_Age + bucket.DocCount
			//logs.Debug("---index_agecategory---")
			//logs.Debug(bucket.Key.(float64))
			//logs.Debug(bucket.DocCount)
		}
		//logs.Debug(tab_category_Age)
		// on aggrege les données pour chaque catégorie d'âge (count aggBirthdate par tranche + count age category)
		for index_agecategory := 0; index_agecategory < 7; index_agecategory++ {

			switch index_agecategory {
			case 0:
				var kpiAtom2 models.KpiReply
				kpiAtom2.Key = "0"
				// calcul du nombre d'âge manquant : somme des missing Birthdate - (somme des aggreg des cat d'âge - cat âge manquant)
				kpiAtom2.Doc_count = a0_agg.DocCount - (sum_category_Age - tab_category_Age[0])
				//on ajoute dans le tableau KpiReplies, la struct de résultats
				tab_kpiAtom.KpiReplies = append(tab_kpiAtom.KpiReplies, kpiAtom2)

			case 1:
				for _, bucket := range a1_agg.Buckets {
					var kpiAtom models.KpiReply
					kpiAtom.Key = "1"
					kpiAtom.Doc_count = bucket.DocCount + tab_category_Age[index_agecategory]
					tab_kpiAtom.KpiReplies = append(tab_kpiAtom.KpiReplies, kpiAtom)
				}
			case 2:
				for _, bucket := range a2_agg.Buckets {
					var kpiAtom models.KpiReply
					kpiAtom.Key = "2"
					kpiAtom.Doc_count = bucket.DocCount + tab_category_Age[index_agecategory]
					tab_kpiAtom.KpiReplies = append(tab_kpiAtom.KpiReplies, kpiAtom)
				}
			case 3:
				for _, bucket := range a3_agg.Buckets {
					var kpiAtom models.KpiReply
					kpiAtom.Key = "3"
					kpiAtom.Doc_count = bucket.DocCount + tab_category_Age[index_agecategory]
					tab_kpiAtom.KpiReplies = append(tab_kpiAtom.KpiReplies, kpiAtom)
				}
			case 4:
				for _, bucket := range a4_agg.Buckets {
					var kpiAtom models.KpiReply
					kpiAtom.Key = "4"
					kpiAtom.Doc_count = bucket.DocCount + tab_category_Age[index_agecategory]
					tab_kpiAtom.KpiReplies = append(tab_kpiAtom.KpiReplies, kpiAtom)
				}
			case 5:
				for _, bucket := range a5_agg.Buckets {
					var kpiAtom models.KpiReply
					kpiAtom.Key = "5"
					kpiAtom.Doc_count = bucket.DocCount + tab_category_Age[index_agecategory]
					tab_kpiAtom.KpiReplies = append(tab_kpiAtom.KpiReplies, kpiAtom)
				}
			case 6:
				for _, bucket := range a6_agg.Buckets {
					var kpiAtom models.KpiReply
					kpiAtom.Key = "6"
					kpiAtom.Doc_count = bucket.DocCount + tab_category_Age[index_agecategory]
					tab_kpiAtom.KpiReplies = append(tab_kpiAtom.KpiReplies, kpiAtom)
				}
			default:
				logs.Critical("problem ")
				err := errors.New("wrong age_category parameter")
				return err
			}
		}
		reply.Kpi = append(reply.Kpi, tab_kpiAtom)
		tab_kpiAtom = models.KpiAggs{}

		// // ---- stockage réponses pour birthdate_aggreg -----------------------
		// for _, bucket := range birthdate_agg.Buckets {
		// 	var kpiAtom models.KpiReply
		// 	kpiAtom.Key=*bucket.KeyAsString
		// 	kpiAtom.Doc_count=bucket.DocCount
		// 	tab_kpiAtom.KpiReplies=append(tab_kpiAtom.KpiReplies, kpiAtom)
		// }
		// reply.Kpi=append(reply.Kpi, tab_kpiAtom)
		// tab_kpiAtom = models.KpiAggs{}

		// ---- stockage réponses pour lastchange_aggreg -----------------------
		for _, bucket := range lastchange_agg.Buckets {
			var kpiAtom models.KpiReply
			kpiAtom.Key = *bucket.KeyAsString
			kpiAtom.Doc_count = bucket.DocCount
			tab_kpiAtom.KpiReplies = append(tab_kpiAtom.KpiReplies, kpiAtom)
		}
		reply.Kpi = append(reply.Kpi, tab_kpiAtom)
		tab_kpiAtom = models.KpiAggs{}

		// ---- nombre de contacts sans email renseigné -----------------------
		kpiAtom = models.KpiReply{}
		kpiAtom.Key = "missing"
		kpiAtom.Doc_count = contacts_sans_email_agg.DocCount
		if contacts_sans_email_agg.DocCount > 0 {
			tab_kpiAtom.KpiReplies = append(tab_kpiAtom.KpiReplies, kpiAtom)
		}
		// ---------------------------------------
		reply.Kpi = append(reply.Kpi, tab_kpiAtom)
		tab_kpiAtom = models.KpiAggs{}

		// ---- nombre de contacts sans email renseigné -----------------------
		kpiAtom = models.KpiReply{}
		kpiAtom.Key = "missing"
		kpiAtom.Doc_count = contacts_sans_tel_agg.DocCount
		if contacts_sans_tel_agg.DocCount > 0 {
			tab_kpiAtom.KpiReplies = append(tab_kpiAtom.KpiReplies, kpiAtom)
		}
		// ---------------------------------------
		reply.Kpi = append(reply.Kpi, tab_kpiAtom)
		tab_kpiAtom = models.KpiAggs{}

		// ---------------------------------------------------------------------

	} else {
		reply.Kpi = nil
	}
	return nil
}

func (s *Search) AggregationContacts(args models.SearchArgs, reply *models.SearchReply) error {
	// Groups are:
	// 1. user_id
	// 2. Date: group by week
	// 3. name_presence (in form_data)

	groupIdStr := args.Search.Fields[0]

	presenceFormId := -1
	if len(args.Search.Fields) > 1 {
		var err error
		presenceFormId, err = strconv.Atoi(args.Search.Fields[1])
		if err != nil {
			logs.Error(err)
			presenceFormId = -1
		}
	}

	bq := elastic.NewBoolQuery() //elastic.BoolQuery
	bq = bq.Must(elastic.NewTermQuery("group_id", groupIdStr))

	/* Filter by date */
	// get min and max dates, either from fields or from the oldest and newest contacts
	minDate := time.Time{}
	maxDate := time.Time{}
	// passedInterval indicates whether the aggregation query was called with a date interval
	// this controls whether a range filter will be added to the query, which eliminates entries with
	// missing dates
	passedInterval := false
	timeFormat := "2006-01-02"
	if len(args.Search.Fields) > 3 && args.Search.Fields[2] != "" && args.Search.Fields[3] != "" {
		// expects the time in the timeFormat yyyy-mm-dd
		var err error
		minDate, err = time.Parse(timeFormat, args.Search.Fields[2])
		if err != nil {
			minDate = time.Time{}
			logs.Error(err)
		}
		maxDate, err = time.Parse(timeFormat, args.Search.Fields[3])
		if err != nil {
			maxDate = time.Time{}
			logs.Error(err)
		}
	}
	// if the dates were not passed, or there was an error parsing them, then set them to the first
	// and last  lastchange date
	if (minDate == time.Time{} || maxDate == time.Time{}) {
		// get newest contact's lastchange time
		newestSearch := s.Client.Search().
			Index("contacts").
			Size(1).
			Sort("lastchange", false)

		newestSearch.Query(&bq)
		newestResult, err := newestSearch.Do()

		if err != nil {
			logs.Error(err)
			return err
		}

		if newestResult.Hits != nil {
			for _, hit := range newestResult.Hits.Hits {
				var c models.Contact
				err := json.Unmarshal(*hit.Source, &c)
				if err != nil {
					logs.Error(err)
					return err
				}
				if c.LastChange != nil {
					maxDate = *c.LastChange //.Format(timeFormat)
				}

			}
		} else {
			message := "Should have gotten one hit when sorting by newest contacts"
			logs.Error(message)
			return errors.New(message)
		}

		// get oldest contact's lastchange time
		oldestSearch := s.Client.Search().
			Index("contacts").
			Size(1).
			Sort("lastchange", true)

		oldestSearch.Query(&bq)
		oldestResult, err := oldestSearch.Do()

		if err != nil {
			logs.Error(err)
			return err
		}

		if oldestResult.Hits != nil {
			for _, hit := range oldestResult.Hits.Hits {
				var c models.Contact
				err := json.Unmarshal(*hit.Source, &c)
				if err != nil {
					logs.Error(err)
					return err
				}
				if c.LastChange != nil {
					minDate = *c.LastChange //.Format(timeFormat)
				}

			}
		} else {
			message := "Should have gotten one hit when sorting by oldest contacts"
			logs.Error(message)
			return errors.New(message)
		}
	} else {
		passedInterval = true
	}

	if passedInterval {
		// The min and maxDates, if passed, should be an inclusive range.  Since only dates are passed (no times)
		// include the entire day.  So, use a Lt on the maxDate after adding one day, to include the entire maxDate
		bq = bq.Must(elastic.NewRangeQuery("lastchange").Gte(minDate).Lt(maxDate.AddDate(0, 0, 1)))
	}

	// 1. user_id
	aggreg_user_id := elastic.NewTermsAggregation().Field("user_id")
	aggreg_user_id_missing := elastic.NewMissingAggregation().Field("user_id")

	// 2. Date
	// TODO - find a more precise value for this, kind of arbitrary
	maxDateBuckets := 41 // 41 = 7 * 6 - 1, so at least 6 weeks will be shown (or months, years...)
	// Choose the finest window where the number of buckets does not exceed maxDateBuckets
	numHours := maxDate.Sub(minDate).Hours()
	// Doesn't need to be exact, so use approximation conversions
	numDays := int(numHours / 24)
	numWeeks := int(numDays / 7)
	numMonths := int(numDays / 30)
	interval := "year"
	if numMonths <= maxDateBuckets {
		if numWeeks <= maxDateBuckets {
			if numDays <= maxDateBuckets {
				interval = "day"
			} else {
				interval = "week"
			}
		} else {
			interval = "month"
		}
	}

	// may be a better place to store this, but send the time start/end and intrval information via kpi
	// this should be the 0th KPI in the array
	var dateData = []models.GenericMap{
		models.GenericMap{
			Key:   "minDate",
			Value: minDate.Format(timeFormat),
		},
		models.GenericMap{
			Key:   "maxDate",
			Value: maxDate.Format(timeFormat),
		},
		models.GenericMap{
			Key:   "interval",
			Value: interval,
		},
	}
	reply.Data = append(reply.Data, dateData...)

	aggreg_date := elastic.NewDateHistogramAggregation().Field("lastchange").Interval(interval).MinDocCount(0).ExtendedBoundsMin(minDate).ExtendedBoundsMax(maxDate)
	aggreg_date_missing := elastic.NewMissingAggregation().Field("lastchange")

	// 3. name_presence
	aggreg_presence := elastic.NewNestedAggregation().Path("formdatas").SubAggregation("filtered_formdatas",
		elastic.NewFilterAggregation().Filter(elastic.NewTermQuery("formdatas.form_id", presenceFormId)).SubAggregation("presence",
			elastic.NewTermsAggregation().Field("formdatas.data.strictdata")))
	aggreg_presence_missing := elastic.NewFilterAggregation().Filter(elastic.NewNotFilter(elastic.NewTermQuery("formdatas.form_id", presenceFormId)))

	// Want these to all be sub aggregations, but for each we need two (missing/not missing)
	// tier order is user, date, presence; build up from bottom
	// bottom sub aggregation - presence
	agg_presence_sub := aggreg_presence
	agg_presence_m_sub := aggreg_presence_missing
	// date
	agg_date_sub := aggreg_date.SubAggregation("presence_agg", agg_presence_sub).SubAggregation("presence_m_agg", agg_presence_m_sub)
	agg_date_m_sub := aggreg_date_missing.SubAggregation("presence_agg", agg_presence_sub).SubAggregation("presence_m_agg", agg_presence_m_sub)
	// top sub aggregation - user
	agg_user_sub := aggreg_user_id.SubAggregation("date_agg", agg_date_sub).SubAggregation("date_m_agg", agg_date_m_sub)
	agg_user_m_sub := aggreg_user_id_missing.SubAggregation("date_agg", agg_date_sub).SubAggregation("date_m_agg", agg_date_m_sub)

	// want all of these to be sub filters; root with user/user missing
	searchService := s.Client.Search().
		Index("contacts").
		Size(0).
		Aggregation("user_agg", agg_user_sub).
		Aggregation("user_m_agg", agg_user_m_sub)

	/* Filter by location */
	// northeast lat, lng (top left) should be args.Search.Fields[4] and [5] (after group_id, presenceFormId and date)
	// southwest lat, lng (top left) should be args.Search.Fields[6] and [7]
	var filter *elastic.GeoPolygonFilter
	if len(args.Search.Fields) > 7 {
		filter = GetLocationFilter(args.Search.Fields[4], args.Search.Fields[5], args.Search.Fields[6], args.Search.Fields[7])
	}
	if filter != nil {
		searchService = searchService.Query(elastic.NewFilteredQuery(bq).Filter(*filter))
	} else {
		searchService = searchService.Query(&bq)
	}

	searchResult, err := searchService.Do()

	if err != nil {
		logs.Error(err)
		return err
	}

	// searchResult is now a hierarchical structure
	// parse it to become columns of aggregated data
	// to make it generic, this means simply being a 2D arrray of string
	// For each entry (row), the first n entries specify the aggregation (for example, userId, lastchange, and presence), and the
	// last entry is a string of the integer count
	// This is a convenient way to store what is effectively a table
	ParseElasticAggregationLevels(searchResult.Aggregations, reply, []string{"user", "date", "presence"}, []string{})

	return nil
}

func (s *Search) DateAggregationContacts(args models.SearchArgs, reply *models.SearchReply) error {
	groupIdStr := args.Search.Fields[0]

	bq := elastic.NewBoolQuery()
	bq = bq.Must(elastic.NewTermQuery("group_id", groupIdStr))

	aggreg_date := elastic.NewDateHistogramAggregation().Field("lastchange").Interval("day").MinDocCount(0)
	date_key := "date_agg"
	searchService := s.Client.Search().
		Index("contacts").
		Size(0).
		Aggregation(date_key, aggreg_date)

	searchService.Query(&bq)
	searchResult, err := searchService.Do()

	if err != nil {
		logs.Error(err)
		return err
	}

	date_agg, found_date := searchResult.Aggregations.DateHistogram(date_key)
	if !found_date {
		logs.Error("we should have a date histogram aggregation called %q", date_key)
	}

	var kpiAggs models.KpiAggs
	for _, bucket := range date_agg.Buckets {
		var kpiAtom models.KpiReply
		kpiAtom.Key = *bucket.KeyAsString
		kpiAtom.Doc_count = bucket.DocCount
		kpiAggs.KpiReplies = append(kpiAggs.KpiReplies, kpiAtom)
	}
	reply.Kpi = append(reply.Kpi, kpiAggs)

	return nil
}

func (s *Search) LocationSummaryContacts(args models.SearchArgs, reply *models.SearchReply) error {
	groupIdStr := args.Search.Fields[0]
	maxResults := 500

	presenceFormId := -1
	if len(args.Search.Fields) > 1 {
		var err error
		presenceFormId, err = strconv.Atoi(args.Search.Fields[1])
		if err != nil {
			logs.Error(err)
			presenceFormId = -1
		}
	}

	bq := elastic.NewBoolQuery()
	bq = bq.Must(elastic.NewTermQuery("group_id", groupIdStr))

	/* Filters */
	// query can be optionally flitered by location (bounding box of map, passed as 1-4th arguments)
	// and optionally by other filter parameters (like date, user, presence) from crossfilter
	// nested and non-nested filters must be treated differently; nested filters must be done in a bool query,
	// whereas non-nested filters must be done in a bool filter
	// (elastic 1.7 only allows a "missing" search in a bool filter, and a must_not match in a query, to handle
	// missing searches)

	/* Filter by date */
	var dateFilter *elastic.RangeQuery
	if len(args.Search.Fields) > 3 {
		dateFilter = GetDateFilter(args.Search.Fields[2], args.Search.Fields[3])
	}

	// add the dateFilter to bq, if it exists
	if dateFilter != nil {
		bq = bq.Must(*dateFilter)
	}

	// Now, set up the boolFilter
	// first, generate a filtered query for the non-nested (topLevel) filters like user, geoPosition, etc.
	boolFilter := elastic.NewBoolQuery()
	// elastic complains if there's an empty bool query, so track if anything is added to boolFilter
	boolFilterExists := false

	/* Filter by location */
	// northeast lat, lng (top left) should be args.Search.Fields[4] and [5] (after group_id, presenceFormId and dates)
	// southwest lat, lng (top left) should be args.Search.Fields[6] and [7]
	var geoFilter *elastic.GeoPolygonFilter
	if len(args.Search.Fields) > 7 {
		geoFilter = GetLocationFilter(args.Search.Fields[4], args.Search.Fields[5], args.Search.Fields[6], args.Search.Fields[7])
	}

	// add the geoFilter to boolFilter, if it exists
	if geoFilter != nil {
		boolFilter = boolFilter.Must(*geoFilter)
		boolFilterExists = true
	}

	// parse the remaining
	fieldsSeparator := ";"
	fieldMissing := "N/A"
	// filters will be in the 7th and later fields array, (first 7 are group_id, location, and date)
	for i := 8; i < len(args.Search.Fields); i++ {
		if args.Search.Fields[i] != "" {
			fields := strings.Split(args.Search.Fields[i], fieldsSeparator)

			// parse the filters
			filterType := fields[0]
			includeMissing := false
			filters := fields[1:]
			for i := 1; i < len(fields); i++ {
				if fields[i] == fieldMissing {
					includeMissing = true
					filters = append(fields[1:i], fields[i+1:]...)
					break
				}
			}

			// (currently) the only valid top level filter is user:
			if filterType == "user" {
				userBool := elastic.NewBoolQuery()
				userFilterExist := false

				for j := 0; j < len(filters); j++ {
					userBool = userBool.Should(elastic.NewTermQuery("user_id", filters[j]))
					boolFilterExists = true
					userFilterExist = true
				}

				if includeMissing {
					userBool = userBool.Should(elastic.NewMissingFilter("user_id"))
					boolFilterExists = true
					userFilterExist = true
				}

				if userFilterExist {
					boolFilter = boolFilter.Must(userBool)
				}
			}

			// (currently) the only valid nested filter is presence
			if filterType == "presence" {
				presenceBool := elastic.NewBoolQuery()
				presenceFilterExist := false

				if len(filters) > 0 {
					nestedBool := elastic.NewBoolQuery()

					for j := 0; j < len(filters); j++ {
						nestedBool = nestedBool.Should(elastic.NewTermQuery("formdatas.data.strictdata", filters[j]))
					}

					// nested path is the formdatas array
					nestedFilter := elastic.NewNestedFilter("formdatas").
						Filter(nestedBool)

					presenceBool = presenceBool.Should(nestedFilter)

					presenceFilterExist = true
				}

				if includeMissing {
					matchQuery := elastic.NewMatchQuery("formdatas.data.strictdata", presenceFormId)
					presenceBool = presenceBool.Should(elastic.NewBoolQuery().MustNot(matchQuery))
					presenceFilterExist = true
				}

				if presenceFilterExist {
					bq = bq.Must(presenceBool)
				}
			}
		}
	}

	countSearch := s.Client.Count().
		Index("contacts")

	if boolFilterExists {
		countSearch = countSearch.Query(elastic.NewFilteredQuery(bq).Filter(boolFilter))
	} else {
		countSearch = countSearch.Query(bq)
	}

	/* Count of contacts matching filter */
	totalResults, err := countSearch.Do()
	if err != nil {
		logs.Error(err)
		return err
	}

	reply.Data = append(reply.Data, models.GenericMap{
		Key:   "totalResults",
		Value: strconv.FormatInt(totalResults, 10),
	})

	/* Random search of contacts */
	// max number of contacts to send over the network

	maxNumResults := maxResults

	numResults := int(totalResults)
	if numResults > maxNumResults {
		numResults = maxNumResults
	}

	random := elastic.NewRandomFunction()
	functionScoreQuery := elastic.NewFunctionScoreQuery().
		AddScoreFunc(random)

	if boolFilterExists {
		functionScoreQuery = functionScoreQuery.Query(elastic.NewFilteredQuery(bq).Filter(boolFilter))
	} else {
		functionScoreQuery = functionScoreQuery.Query(bq)
	}

	source := elastic.NewFetchSourceContext(true).
		//Include("group_id").
		Include("address.latitude").
		Include("address.longitude")

	searchService := s.Client.Search().
		Index("contacts").
		Size(numResults).
		FetchSourceContext(source).
		Query(functionScoreQuery)

	searchResult, err := searchService.Do()

	if err != nil {
		logs.Error(err)
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

func (s *Search) LocationSummaryContactsGeoHashWithSearchFilter(args models.SearchArgs, reply *models.SearchReply) error {
	logs.Debug("LocationSummaryContactsGeoHashWithSearchFilter")
	logs.Debug("args.Search.Query:%s", args.Search.Query)
	logs.Debug("args.Search.Fields:%s", args.Search.Fields)
	var bq elastic.BoolQuery

	//construction de la query - commun avec findcontacts----------
	err := BuildQuery(args, &bq)
	if err != nil {
		logs.Error(err)
		return err
	}

	// precision, err := strconv.Atoi(args.Search.Fields[11])
	// if err != nil {
	// 	logs.Error(err)
	// 	precision = 5
	// }
	precision := 8

	// /* Filter by location */
	// // northeast lat, lng (top left) should be args.Search.Fields[4] and [5] (after group_id, presenceFormId and dates)
	// // southwest lat, lng (top left) should be args.Search.Fields[6] and [7]
	// var geoFilter *elastic.GeoPolygonFilter
	// if len(args.Search.Fields) > 7 {
	// 	geoFilter = GetLocationFilter(args.Search.Fields[4], args.Search.Fields[5], args.Search.Fields[6], args.Search.Fields[7])
	// }

	// // add the geoFilter to boolFilter, if it exists
	// if geoFilter != nil {
	// 	boolFilter = boolFilter.Must(*geoFilter)
	// 	boolFilterExists = true
	// }

	// /* Random search of contacts */
	// // max number of contacts to send over the network

	// random := elastic.NewRandomFunction()
	// functionScoreQuery := elastic.NewFunctionScoreQuery().
	// 	AddScoreFunc(random)

	// if boolFilterExists {
	// 	functionScoreQuery = functionScoreQuery.Query(elastic.NewFilteredQuery(bq).Filter(boolFilter))
	// } else {
	// 	functionScoreQuery = functionScoreQuery.Query(bq)
	// }

	source := elastic.NewFetchSourceContext(true).
		//was include, but for what?
		//Include("group_id").
		Include("address.latitude").
		Include("address.longitude")

	searchService := s.Client.Search().
		Index("contacts").
		Size(1).
		FetchSourceContext(source)

	Filter := elastic.NewGeoPolygonFilter("location")
	if len(args.Search.Polygon) > 0 {
		var point models.Point
		for _, point = range args.Search.Polygon {
			geoPoint := elastic.GeoPointFromLatLon(point.Lat, point.Lng)
			Filter = Filter.AddPoint(geoPoint)
		}
		searchService.Query(elastic.NewFilteredQuery(bq).Filter(Filter))
	} else {
		searchService.Query(&bq)
	}

	//aggHash := elastic.NewGeoHashGridAggregation()
	aggHash := elastic.NewGeoHashGridAggregation()
	//aggHash := elastic.NewDateHistogramAggregation()
	aggHash.Field("location").Precision(precision)

	center_lat := elastic.NewAvgAggregation().Script("doc['location'].lat")
	center_lon := elastic.NewAvgAggregation().Script("doc['location'].lon")
	aggHash = aggHash.SubAggregation("center_lat", center_lat)
	aggHash = aggHash.SubAggregation("center_lon", center_lon)

	searchService.Aggregation("cells", aggHash)

	searchResult, err := searchService.Do()

	if err != nil {
		logs.Error(err)
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

	//var kpiAggs models.KpiAggs

	//searchResult.Aggregations.cells
	//logs.Debug("searchResult.Aggregations:")
	//logs.Debug(searchResult.Aggregations.GeoHash("cells"))
	resul_AGG, found := searchResult.Aggregations.GeoHash("cells")

	if !found {
		logs.Error("we sould have a GeoHash aggregation called %q", "cells")
	}

	//logs.Debug(len(resul_AGG.Buckets))
	var tab_point [][]interface{}
	for _, buck := range resul_AGG.Buckets {
		//logs.Debug(buck.Key)
		//logs.Debug(buck.DocCount)
		lat, found := buck.Avg("center_lat")
		if !found {
			logs.Error("we sould have a GeoHash aggregation called %q", "center_lat")
		}
		lon, found := buck.Avg("center_lon")
		if !found {
			logs.Error("we sould have a GeoHash aggregation called %q", "center_lon")
		}
		var temp_map []interface{}
		temp_map = append(temp_map, strconv.FormatFloat(*lat.Value, 'f', -1, 64))
		temp_map = append(temp_map, strconv.FormatFloat(*lon.Value, 'f', -1, 64))
		temp_map = append(temp_map, buck.DocCount)
		tab_point = append(tab_point, temp_map)
		//logs.Debug(strconv.FormatFloat(*lat.Value, 'f', -1, 64))
		//logs.Debug(strconv.FormatFloat(*lon.Value, 'f', -1, 64))
	}
	reply.Data = append(reply.Data, models.GenericMap{
		Map: tab_point,
	})

	return nil
}

func (s *Search) LocationSummaryContactsGeoHash(args models.SearchArgs, reply *models.SearchReply) error {
	groupIdStr := args.Search.Fields[0]
	maxResults, err := strconv.Atoi(args.Search.Fields[10])
	if err != nil {
		logs.Error(err)
		maxResults = 500
	}
	precision, err := strconv.Atoi(args.Search.Fields[11])
	if err != nil {
		logs.Error(err)
		precision = 5
	}

	presenceFormId := -1
	if len(args.Search.Fields) > 1 {
		var err error
		presenceFormId, err = strconv.Atoi(args.Search.Fields[1])
		if err != nil {
			logs.Error(err)
			presenceFormId = -1
		}
	}

	bq := elastic.NewBoolQuery()
	bq = bq.Must(elastic.NewTermQuery("group_id", groupIdStr))

	/* Filters */
	// query can be optionally flitered by location (bounding box of map, passed as 1-4th arguments)
	// and optionally by other filter parameters (like date, user, presence) from crossfilter
	// nested and non-nested filters must be treated differently; nested filters must be done in a bool query,
	// whereas non-nested filters must be done in a bool filter
	// (elastic 1.7 only allows a "missing" search in a bool filter, and a must_not match in a query, to handle
	// missing searches)

	/* Filter by date */
	var dateFilter *elastic.RangeQuery
	if len(args.Search.Fields) > 3 {
		dateFilter = GetDateFilter(args.Search.Fields[2], args.Search.Fields[3])
	}

	// add the dateFilter to bq, if it exists
	if dateFilter != nil {
		bq = bq.Must(*dateFilter)
	}

	// Now, set up the boolFilter
	// first, generate a filtered query for the non-nested (topLevel) filters like user, geoPosition, etc.
	boolFilter := elastic.NewBoolQuery()
	// elastic complains if there's an empty bool query, so track if anything is added to boolFilter
	boolFilterExists := false

	/* Filter by location */
	// northeast lat, lng (top left) should be args.Search.Fields[4] and [5] (after group_id, presenceFormId and dates)
	// southwest lat, lng (top left) should be args.Search.Fields[6] and [7]
	var geoFilter *elastic.GeoPolygonFilter
	if len(args.Search.Fields) > 7 {
		geoFilter = GetLocationFilter(args.Search.Fields[4], args.Search.Fields[5], args.Search.Fields[6], args.Search.Fields[7])
	}

	// add the geoFilter to boolFilter, if it exists
	if geoFilter != nil {
		boolFilter = boolFilter.Must(*geoFilter)
		boolFilterExists = true
	}

	// parse the remaining
	fieldsSeparator := ";"
	fieldMissing := "N/A"
	// filters will be in the 7th and later fields array, (first 7 are group_id, location, and date)
	for i := 8; i < len(args.Search.Fields); i++ {
		if args.Search.Fields[i] != "" {
			fields := strings.Split(args.Search.Fields[i], fieldsSeparator)

			// parse the filters
			filterType := fields[0]
			includeMissing := false
			filters := fields[1:]
			for i := 1; i < len(fields); i++ {
				if fields[i] == fieldMissing {
					includeMissing = true
					filters = append(fields[1:i], fields[i+1:]...)
					break
				}
			}

			// (currently) the only valid top level filter is user:
			if filterType == "user" {
				userBool := elastic.NewBoolQuery()
				userFilterExist := false

				for j := 0; j < len(filters); j++ {
					userBool = userBool.Should(elastic.NewTermQuery("user_id", filters[j]))
					boolFilterExists = true
					userFilterExist = true
				}

				if includeMissing {
					userBool = userBool.Should(elastic.NewMissingFilter("user_id"))
					boolFilterExists = true
					userFilterExist = true
				}

				if userFilterExist {
					boolFilter = boolFilter.Must(userBool)
				}
			}

			// (currently) the only valid nested filter is presence
			if filterType == "presence" {
				presenceBool := elastic.NewBoolQuery()
				presenceFilterExist := false

				if len(filters) > 0 {
					nestedBool := elastic.NewBoolQuery()

					for j := 0; j < len(filters); j++ {
						nestedBool = nestedBool.Should(elastic.NewTermQuery("formdatas.data.strictdata", filters[j]))
					}

					// nested path is the formdatas array
					nestedFilter := elastic.NewNestedFilter("formdatas").
						Filter(nestedBool)

					presenceBool = presenceBool.Should(nestedFilter)

					presenceFilterExist = true
				}

				if includeMissing {
					matchQuery := elastic.NewMatchQuery("formdatas.data.strictdata", presenceFormId)
					presenceBool = presenceBool.Should(elastic.NewBoolQuery().MustNot(matchQuery))
					presenceFilterExist = true
				}

				if presenceFilterExist {
					bq = bq.Must(presenceBool)
				}
			}
		}
	}

	// countSearch := s.Client.Count().
	// 	Index("contacts")

	// if boolFilterExists {
	// 	countSearch = countSearch.Query(elastic.NewFilteredQuery(bq).Filter(boolFilter))
	// } else {
	// 	countSearch = countSearch.Query(bq)
	// }

	//  //Count of contacts matching filter
	// totalResults, err := countSearch.Do()
	// if err != nil {
	// 	logs.Error(err)
	// 	return err
	// }

	// reply.Data = append(reply.Data, models.GenericMap{
	// 	Key:   "totalResults",
	// 	Value: strconv.FormatInt(totalResults, 10),
	// })

	/* Random search of contacts */
	// max number of contacts to send over the network

	random := elastic.NewRandomFunction()
	functionScoreQuery := elastic.NewFunctionScoreQuery().
		AddScoreFunc(random)

	if boolFilterExists {
		functionScoreQuery = functionScoreQuery.Query(elastic.NewFilteredQuery(bq).Filter(boolFilter))
	} else {
		functionScoreQuery = functionScoreQuery.Query(bq)
	}

	source := elastic.NewFetchSourceContext(true).
		//was include, but for what?
		//Include("group_id").
		Include("address.latitude").
		Include("address.longitude")

	searchService := s.Client.Search().
		Index("contacts").
		Size(maxResults).
		FetchSourceContext(source).
		Query(functionScoreQuery)

	//aggHash := elastic.NewGeoHashGridAggregation()
	aggHash := elastic.NewGeoHashGridAggregation()
	//aggHash := elastic.NewDateHistogramAggregation()
	aggHash.Field("location").Precision(precision)

	center_lat := elastic.NewAvgAggregation().Script("doc['location'].lat")
	center_lon := elastic.NewAvgAggregation().Script("doc['location'].lon")
	aggHash = aggHash.SubAggregation("center_lat", center_lat)
	aggHash = aggHash.SubAggregation("center_lon", center_lon)

	searchService.Aggregation("cells", aggHash)

	searchResult, err := searchService.Do()

	if err != nil {
		logs.Error(err)
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

	//var kpiAggs models.KpiAggs

	//searchResult.Aggregations.cells
	//logs.Debug("searchResult.Aggregations:")
	//logs.Debug(searchResult.Aggregations.GeoHash("cells"))
	resul_AGG, found := searchResult.Aggregations.GeoHash("cells")

	if !found {
		logs.Error("we sould have a GeoHash aggregation called %q", "cells")
	}

	//logs.Debug(len(resul_AGG.Buckets))
	var tab_point [][]interface{}
	for _, buck := range resul_AGG.Buckets {
		//logs.Debug(buck.Key)
		//logs.Debug(buck.DocCount)
		lat, found := buck.Avg("center_lat")
		if !found {
			logs.Error("we sould have a GeoHash aggregation called %q", "center_lat")
		}
		lon, found := buck.Avg("center_lon")
		if !found {
			logs.Error("we sould have a GeoHash aggregation called %q", "center_lon")
		}
		var temp_map []interface{}
		temp_map = append(temp_map, strconv.FormatFloat(*lat.Value, 'f', -1, 64))
		temp_map = append(temp_map, strconv.FormatFloat(*lon.Value, 'f', -1, 64))
		temp_map = append(temp_map, buck.DocCount)
		tab_point = append(tab_point, temp_map)
		//logs.Debug(strconv.FormatFloat(*lat.Value, 'f', -1, 64))
		//logs.Debug(strconv.FormatFloat(*lon.Value, 'f', -1, 64))
	}
	reply.Data = append(reply.Data, models.GenericMap{
		Map: tab_point,
	})

	return nil
}

func GetLocationFilter(neLatStr string, neLngStr string, swLatStr string, swLngStr string) *elastic.GeoPolygonFilter {
	// first an easy check - make sure none of the strings are empty
	if neLatStr == "" || neLngStr == "" || swLatStr == "" || swLngStr == "" {
		return nil
	}

	validBoundary := true
	neLat, err := strconv.ParseFloat(neLatStr, 64)
	if err != nil {
		logs.Error(err)
		validBoundary = false
	}

	neLng, err := strconv.ParseFloat(neLngStr, 64)
	if err != nil {
		logs.Error(err)
		validBoundary = false
	}

	swLat, err := strconv.ParseFloat(swLatStr, 64)
	if err != nil {
		logs.Error(err)
		validBoundary = false
	}

	swLng, err := strconv.ParseFloat(swLngStr, 64)
	if err != nil {
		logs.Error(err)
		validBoundary = false
	}

	if validBoundary {
		// elastic.v2 has no GeoBoundingBox, so use a GeoPolygonFilter
		// this may be slower (not sure if significant), TODO analyze
		filter := elastic.NewGeoPolygonFilter("address.location").
			AddPoint(elastic.GeoPointFromLatLon(neLat, neLng)).
			AddPoint(elastic.GeoPointFromLatLon(neLat, swLng)).
			AddPoint(elastic.GeoPointFromLatLon(swLat, swLng)).
			AddPoint(elastic.GeoPointFromLatLon(swLat, neLng))
		return &filter
	}

	return nil
}

// try to parse the min and max dates from the passed fields if they are
// valid dates, return a rangeFilter pointer, otherwise return nil the dates
// should be strings in the format timeFormat (defined below as "2006-01-02")
func GetDateFilter(minDateStr string, maxDateStr string) *elastic.RangeQuery {
	validDates := true
	timeFormat := "2006-01-02"

	// expects the time in the timeFormat yyyy-mm-dd
	minDate, err := time.Parse(timeFormat, minDateStr)
	if err != nil {
		logs.Error(err)
		validDates = false
	}
	maxDate, err := time.Parse(timeFormat, maxDateStr)
	if err != nil {
		logs.Error(err)
		validDates = false
	}

	if validDates {
		// The min and maxDates, if given, should be an inclusive range.  Since only dates are passed (no times)
		// include the entire day.  So, use a Lt on the maxDate after adding one day, to include the entire maxDate
		rangeFilter := elastic.NewRangeQuery("lastchange").Gte(minDate).Lt(maxDate.AddDate(0, 0, 1))
		return &rangeFilter
	}

	return nil
}

// This function parses an aggregations, traversing through the sub aggregations according to the order array and accumulating the fields
// in aggAccumulator, writing the result to reply.Aggregations when the end of order has been reached
// It uses a switch to choose which helper funciton to run to parse the current level (TODO - could be improved, string switch feels brittle)
func ParseElasticAggregationLevels(aggs elastic.Aggregations, reply *models.SearchReply, order []string, aggAccumulator []string) error {
	if len(order) <= 0 {
		logs.Error("ParseElasticAggregationLevels was called with no levels!")
		return nil // TODO - should be an error
	} else {
		level, order := order[0], order[1:]

		// TODO - don't think this is the best method
		switch level {
		case "user":
			return ParseUsers(aggs, reply, order, aggAccumulator)
		case "date":
			return ParseDates(aggs, reply, order, aggAccumulator)
		case "presence":
			return ParsePresences(aggs, reply, order, aggAccumulator)
		default:
			logs.Error("%q is not a valid level to parse", level)
			return nil // TODO - should be an error
		}
	}
}

// these functions rely on the naming convention from AggregationContacts; this makes it brittle
// TODO - find safer way of parsing this
func ParseUsers(aggs elastic.Aggregations, reply *models.SearchReply, order []string, aggAccumulator []string) error {
	user_key := "user_agg"
	user_missing_key := "user_m_agg"

	var ac []string

	user_agg, found_user := aggs.Terms(user_key)
	if !found_user {
		logs.Error("we should have a terms aggregation called %q", user_key)
	}

	for _, bucket := range user_agg.Buckets {
		ac = append(aggAccumulator, strconv.FormatFloat(bucket.Key.(float64), 'f', -1, 64))
		if len(order) <= 0 {
			ac = append(ac, strconv.FormatInt(bucket.DocCount, 10))
			reply.Aggregation = append(reply.Aggregation, append(ac))
		} else {
			ParseElasticAggregationLevels(bucket.Aggregations, reply, order, ac)
		}
	}

	user_m_agg, found_user_m := aggs.Missing(user_missing_key)
	if !found_user_m {
		logs.Error("we should have a missing aggregation called %q", user_missing_key)
	}

	ac = append(aggAccumulator, "N/A")
	if len(order) <= 0 {
		ac = append(ac, strconv.FormatInt(user_m_agg.DocCount, 10))
		reply.Aggregation = append(reply.Aggregation, append(ac))
	} else {
		ParseElasticAggregationLevels(user_m_agg.Aggregations, reply, order, ac)
	}

	return nil
}

func ParseDates(aggs elastic.Aggregations, reply *models.SearchReply, order []string, aggAccumulator []string) error {
	date_key := "date_agg"
	date_missing_key := "date_m_agg"

	var ac []string

	date_agg, found_date := aggs.DateHistogram(date_key)
	if !found_date {
		logs.Error("we should have a date histogram aggregation called %q", date_key)
	}

	for _, bucket := range date_agg.Buckets {
		ac = append(aggAccumulator, *bucket.KeyAsString)
		if len(order) <= 0 {
			ac = append(ac, strconv.FormatInt(bucket.DocCount, 10))
			reply.Aggregation = append(reply.Aggregation, append(ac))
		} else {
			ParseElasticAggregationLevels(bucket.Aggregations, reply, order, ac)
		}
	}

	date_m_agg, found_date_m := aggs.Missing(date_missing_key)
	if !found_date_m {
		logs.Error("we should have a missing aggregation called %q", date_missing_key)
	}

	ac = append(aggAccumulator, "N/A")
	if len(order) <= 0 {
		ac = append(ac, strconv.FormatInt(date_m_agg.DocCount, 10))
		reply.Aggregation = append(reply.Aggregation, append(ac))
	} else {
		ParseElasticAggregationLevels(date_m_agg.Aggregations, reply, order, ac)
	}

	return nil
}

func ParsePresences(aggs elastic.Aggregations, reply *models.SearchReply, order []string, aggAccumulator []string) error {
	presence_key := "presence_agg"
	presence_filtered_key := "filtered_formdatas"
	presence_formdatas_key := "presence"
	presence_missing_key := "presence_m_agg"

	var ac []string

	presence_agg, found_presence := aggs.Nested(presence_key)
	if !found_presence {
		logs.Error("we should have a nested aggregation called %q", presence_key)
	}

	filtered_formdatas, found_filtered := presence_agg.Aggregations.Filter(presence_filtered_key)
	if !found_filtered {
		logs.Error("we should have a terms aggregation called %q", presence_filtered_key)
	}

	presence, found_presence := filtered_formdatas.Aggregations.Terms(presence_formdatas_key)
	if !found_presence {
		logs.Error("we should have a terms aggregation called %q", presence_formdatas_key)
	}

	for _, bucket := range presence.Buckets {
		ac = append(aggAccumulator, bucket.Key.(string))
		if len(order) <= 0 {
			ac = append(ac, strconv.FormatInt(bucket.DocCount, 10))
			reply.Aggregation = append(reply.Aggregation, append(ac))
		} else {
			ParseElasticAggregationLevels(presence.Aggregations, reply, order, ac)
		}
	}

	presence_m_agg, found_presence_m := aggs.Filter(presence_missing_key)
	if !found_presence_m {
		logs.Error("we should have a filter aggregation called %q", presence_missing_key)
	}

	ac = append(aggAccumulator, "N/A")
	if len(order) <= 0 {
		ac = append(ac, strconv.FormatInt(presence_m_agg.DocCount, 10))
		reply.Aggregation = append(reply.Aggregation, append(ac))
	} else {
		ParseElasticAggregationLevels(presence_m_agg.Aggregations, reply, order, ac)
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
/*
func (s *Search) SearchAddressesAggs(args models.SearchArgs, reply *models.SearchReply) error {
	//logs.Debug("args.Search.Query:%s", args.Search.Query)
	//logs.Debug("args.Search.Fields:%s", args.Search.Fields)
	Query := elastic.NewMultiMatchQuery(strings.ToLower(args.Search.Query)) //A remplacer par fields[] plus tard

	//https://www.elastic.co/guide/en/elasticsearch/reference/1.7/query-dsl-multi-match-query.html#type-phrase
	Query = Query.Type("cross_fields")
	Query = Query.Operator("and")

	Query = Query.Field("address.street")
	Query = Query.Field("address.housenumber")
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
*/
//
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
		geoPoint := elastic.GeoPointFromLatLon(point.Lat, point.Lng)
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
	source = source.Include("married_name")
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
