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
	Query := elastic.NewMultiMatchQuery(strings.ToLower(args.Search.Query))//A remplacer par fields[] plus tard
	//query au cas où il n'y a rien dans la barre de recherche
	QueryVide := elastic.NewMatchAllQuery()

	*bq = elastic.NewBoolQuery()
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
			} else if args.Search.Fields[1] == "address" {
				//champs dans lesquels chercher
				Query = Query.Field("address.street")
			  Query = Query.Field("address.housenumber")
			  Query = Query.Field("address.city")
			}
	}

	// on ajoute à la bool query la requete vide ou pas
	if args.Search.Query!="" {
		*bq = bq.Must(Query)
	}else{
		*bq = bq.Must(QueryVide)
	}

	// filtre la recherche sur un groupe en particulier !!!! pas d'authorisation nécessaire !!!!
	*bq = bq.Must(elastic.NewTermQuery("group_id", args.Search.Fields[0]))

	// contrôle si on est en recherche simple (mobile) ou avancée (desktop)
	// si recherche avancée cad plus de 3 paramêtres dans Fields
	if len(args.Search.Fields)>4{

		//--------------------------------gender ------------------------------------------------------------

		var gender_filter = args.Search.Fields[4]
		if gender_filter != ""{
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
		if pollingstation_filter != ""{
			//affectation des différentes polling station dans un tableau de string
			var dataSlice_pollingstation []string = strings.Split(pollingstation_filter, "/")
			//vérifie si l'on passé comme argument le mot clé missing. Si c'est le cas (index>-1), alors la requête bq.must n'est pas la même
			var index = SliceIndex(len(dataSlice_pollingstation), func(i int) bool { return dataSlice_pollingstation[i] == "missing" })
			if index > -1 {
				dataSlice_pollingstation = append(dataSlice_pollingstation[:index], dataSlice_pollingstation[index+1:]...)
				dataSlice_pollingstation = append(dataSlice_pollingstation,"")
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
			}else{
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
		if len(args.Search.Fields)>11{
			err := BuildQueryForm(args,bq)
			if err != nil {
				logs.Error(err)
				return err
			}
		}

		//-------------------------age_category & birthdate ----------------------------------------------------

		var agecategory = args.Search.Fields[6]
		if agecategory != ""{

			//affectation des différentes catégories d'âge dans un tableau de string
			var dataSlice_agecategory []string = strings.Split(agecategory, "/")
			// -------- GESTION DE LA GATEGORIE 0 --------------------------------------------
					// on extrait la valeur 0 du slice si elle existe
					var index = SliceIndex(len(dataSlice_agecategory), func(i int) bool { return dataSlice_agecategory[i] == "0" })
					if  index > -1 {
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

		if (len(args.Search.Fields)>9){
			var lastchange_filter = args.Search.Fields[9]
			if lastchange_filter != ""{
	      			*bq = bq.Must(elastic.NewRangeQuery("lastchange").Gte(lastchange_filter))
			}
		}
		//--------------------------------------EMAIL FILTER --------------------------------------------
		if (len(args.Search.Fields)>10){
			var email_filter = args.Search.Fields[10]
			if email_filter != ""{
				var dataSlice_email []string = strings.Split(email_filter, "/")
				if (len(dataSlice_email)==1){
					if (dataSlice_email[0]=="SET"){
						*bq = bq.MustNot(elastic.NewFilteredQuery(elastic.NewMatchAllQuery()).Filter(elastic.NewMissingFilter("mail")))
					}else{
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
							interfaceSlice_form[index]=argument

						case 1,3:
							temp,err := strconv.Atoi(argument)
							if err != nil {
								logs.Error(err)
								err = errors.New("Contactez le support.(bad arguments in the filtering of forms)-1")
								return err
							}
							interfaceSlice_form[index]=temp

						case 2:
							temp,err := strconv.ParseBool(argument)
							if err != nil {
								logs.Error(err)
								err = errors.New("Contactez le support.(bad arguments in the filtering of forms)-2")
								return err
							}
							interfaceSlice_form[index]=temp

						case 4,5:
							if dataSlice_form[0]=="TEXT"{
								interfaceSlice_form[index]=argument
							}

							if dataSlice_form[0]=="DATE"{
								temp,err := time.Parse(time.RFC3339, argument)
								if err != nil {
									logs.Error(err)
									err = errors.New("Contactez le support.(bad arguments in the filtering of forms)-3")
									return err
								}
								temp2 := int(temp.Unix())*1000
								interfaceSlice_form[index]=temp2
							}

							if dataSlice_form[0]=="RANGE"{
								temp,err := strconv.Atoi(argument)
								if err != nil {
									logs.Error(err)
									err = errors.New("Contactez le support.(bad arguments in the filtering of forms)-4")
									return err
								}
								interfaceSlice_form[index]=temp
							}

						case 8:
							if dataSlice_form[0]=="TEXT"{
								interfaceSlice_form[index]=argument
							}
							if dataSlice_form[0]=="DATE"{
								temp,err := strconv.Atoi(argument)
								if err != nil {
									logs.Error(err)
									err = errors.New("Contactez le support.(bad arguments in the filtering of forms)-5")
									return err
								}
								interfaceSlice_form[index]=temp
							}
							if dataSlice_form[0]=="RANGE"{
								temp,err := strconv.Atoi(argument)
								if err != nil {
									logs.Error(err)
									err = errors.New("Contactez le support.(bad arguments in the filtering of forms)-6")
									return err
								}
								interfaceSlice_form[index]=temp
							}

						default:
							interfaceSlice_form[index]=argument
						}
					}
					// si la requête pour Form ne contient que trois éléménts, alors cela est soit une reqûete de présence ou d'absence de formdata (répondu, pas répondu)

					if len(interfaceSlice_form)==3{

						var bq_child1 elastic.BoolQuery = elastic.NewBoolQuery()
						var bq_child2 elastic.BoolQuery = elastic.NewBoolQuery()

						if (interfaceSlice_form[2].(bool)){

							*bq = bq.Must(elastic.NewTermsQuery("formdatas.form_id", interfaceSlice_form[1]))
						}else{

							bq_child1 = bq_child1.Should(elastic.NewFilteredQuery(elastic.NewMatchAllQuery()).Filter(elastic.NewMissingFilter("formdatas.form_ref_id")))
							bq_child2 = bq_child2.MustNot(elastic.NewTermQuery("formdatas.form_id", interfaceSlice_form[1]))
							bq_child1 = bq_child1.Should(bq_child2)
							bq_child1 = bq_child1.MinimumShouldMatch("1")
							*bq = bq.Must(bq_child1)
							//*bq = bq.Must(elastic.NewFilteredQuery(elastic.NewMatchAllQuery()).Filter(elastic.NewMissingFilter("formdatas.form_ref_id")))
						}
					}
					// si la requête pour Form contient quatre éléménts, alors cela est soit une reqûete pour radio ou checkbox pour vérifier que le form_ref_id correspondant à une valeur est dans le formdata
					if len(interfaceSlice_form)==4{

						var bq_child1 elastic.BoolQuery = elastic.NewBoolQuery()
						var bq_child2 elastic.BoolQuery = elastic.NewBoolQuery()
						if (interfaceSlice_form[2].(bool)){

							*bq = bq.Must(elastic.NewTermsQuery("formdatas.form_ref_id", interfaceSlice_form[3]))
						}else{

							bq_child1 = bq_child1.Should(elastic.NewFilteredQuery(elastic.NewMatchAllQuery()).Filter(elastic.NewMissingFilter("formdatas.form_ref_id")))
							bq_child2 = bq_child2.MustNot(elastic.NewTermQuery("formdatas.form_ref_id", interfaceSlice_form[3]))
							bq_child1 = bq_child1.Should(bq_child2)
							bq_child1 = bq_child1.MinimumShouldMatch("1")
							*bq = bq.Must(bq_child1)
						}
					}
					// requête avec valeur positionnée ----------------
					if len(interfaceSlice_form)==5{
						var bq_child1 elastic.BoolQuery = elastic.NewBoolQuery()
						var bq_child2 elastic.BoolQuery = elastic.NewBoolQuery()
						var bq_child3 elastic.BoolQuery = elastic.NewBoolQuery()
						var bq_child4 elastic.BoolQuery = elastic.NewBoolQuery()
						var bq_child_nested elastic.NestedQuery


						if(dataSlice_form[0]=="DATE"){
							var interfaceTemp interface{}
							interfaceTemp = interfaceSlice_form[4].(int)+86399000
							if (interfaceSlice_form[2].(bool)){
								bq_child1 = bq_child1.Must(elastic.NewTermQuery("formdatas.form_ref_id", interfaceSlice_form[3]))
								bq_child1 = bq_child1.Must(elastic.NewRangeQuery("formdatas.data.strictdata").Gte(interfaceSlice_form[4]).Lte(interfaceTemp))
								bq_child_nested = elastic.NewNestedQuery("formdatas").Query(bq_child1)
								*bq = bq.Must(bq_child_nested)
							}else{

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
						}else if(dataSlice_form[0]=="TEXT"){
								//pour découper (espace) le query du text afin de faire plusieurs arguments
								var texts = strings.Split(dataSlice_form[4], " ")
								for index, text := range texts {
									if (text==""){
										texts = append(texts[:index], texts[index+1:]...)
									}
								}
								//création d'un array d'interface
								var interfaceSlice_dataSlice_form4 []interface{} = make([]interface{}, len(texts))
								for index, text := range texts {
									interfaceSlice_dataSlice_form4[index]=strings.ToLower(text)
								}

								if (interfaceSlice_form[2].(bool)){
										bq_child1 = bq_child1.Must(elastic.NewTermQuery("formdatas.form_ref_id", interfaceSlice_form[3]))
										bq_child1 = bq_child1.Must(elastic.NewTermsQuery("formdatas.data", interfaceSlice_dataSlice_form4...))
										bq_child_nested = elastic.NewNestedQuery("formdatas").Query(bq_child1)
										*bq = bq.Must(bq_child_nested)
								}else{
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



						}else {
							if (interfaceSlice_form[2].(bool)){
									bq_child1 = bq_child1.Must(elastic.NewTermQuery("formdatas.form_ref_id", interfaceSlice_form[3]))
									bq_child1 = bq_child1.Must(elastic.NewTermsQuery("formdatas.data.strictdata", interfaceSlice_form[4]))
									bq_child_nested = elastic.NewNestedQuery("formdatas").Query(bq_child1)
									*bq = bq.Must(bq_child_nested)
							}else{

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
					if len(interfaceSlice_form)==6{

							var bq_child1 elastic.BoolQuery = elastic.NewBoolQuery()
							var bq_child2 elastic.BoolQuery = elastic.NewBoolQuery()
							var bq_child3 elastic.BoolQuery = elastic.NewBoolQuery()

							if (interfaceSlice_form[2].(bool)){
								bq_child1 = bq_child1.Must(elastic.NewTermQuery("formdatas.form_ref_id", interfaceSlice_form[3]))
								bq_child1 = bq_child1.Must(elastic.NewRangeQuery("formdatas.data").Gte(interfaceSlice_form[4]).Lte(interfaceSlice_form[5]))
								*bq = bq.Must(bq_child1)
							}else{

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
	logs.Debug("args.Search.Query:%s", args.Search.Query)
	logs.Debug("args.Search.Fields:%s", args.Search.Fields)
	logs.Debug("args.Search.Polygon:%s", args.Search.Polygon)


	// TEMPORY PATCH FOR MOBILE COMPATIBILITY 0.1.4 (and inferior) -> delete the 4th parameters of "address" request
	logs.Debug("len(args.Search.Fields):")
	logs.Debug(len(args.Search.Fields))
	if (args.Search.Fields[1]=="address"&&len(args.Search.Fields)==4){
		logs.Debug("args.Search.Fields[3]:")
		logs.Debug(args.Search.Fields[3])
		args.Search.Fields[3]="0"
	}


	var bq elastic.BoolQuery
	err := BuildQuery(args,&bq)
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
	source = source.Include("address.latitude")
	source = source.Include("address.longitude")
	source = source.Include("gender")
	source = source.Include("birthdate")
	source = source.Include("phone")
	source = source.Include("mobile")
	source = source.Include("mail")
	source = source.Include("lastchange")
	source = source.Include("formdatas")



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

	if (len(args.Search.Fields) > 8 && args.Search.Fields[7]!=""){
		sort = args.Search.Fields[7]
		if asc2, err := strconv.ParseBool(args.Search.Fields[8]); err == nil {
			asc=asc2
		}
	} else{
		sort = "surname"
		asc = true
	}




 //-------- findcontacts classique -----------------------------------------------


	 searchService := s.Client.Search().
	 Index("contacts").
	 FetchSourceContext(source).
	 Query(&bq)

 // address aggs --------------------------------

	 if (args.Search.Fields[1]=="address"){
		 	logs.Debug("ADD AGGREGATION ADDRESS")
		 	aggreg_lattitude := elastic.NewTermsAggregation().Field("address.latitude").Size(size_requete)
		 	subaggreg_unique := elastic.NewTopHitsAggregation().Size(size_requete)
		 	aggreg_lattitude = aggreg_lattitude.SubAggregation("result_subaggreg", subaggreg_unique)
			searchService.Size(0).Aggregation("result_aggreg", aggreg_lattitude).Sort("surname", true)
	 }else{
		 	searchService.Size(size_requete).
		  From(from_requete).
		  Sort(sort, asc).
		  Pretty(true)
	 }

 //-------------manage polygon Filter--------------------------




 if len(args.Search.Polygon)>0{
	 Filter := elastic.NewGeoPolygonFilter("location")
	 var point models.Point
	 for _, point = range args.Search.Polygon {
		 geoPoint := elastic.GeoPointFromLatLon(point.Lat, point.Lon)
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
	sourceTTT := bq.Source()
	data, _ := json.Marshal(sourceTTT)
	fmt.Println("DATA", string(data))

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
	agg, found := searchResult.Aggregations.Terms("result_aggreg")
  if !found {
    logs.Debug("we sould have a terms aggregation called %q", "aggreg_lattitude")
  }else{
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
	}
	//----------------------------------------------------
	return nil
}


//-------------------------------------------------------------------------------------------------


func (s *Search) KpiContacts(args models.SearchArgs, reply *models.SearchReply) error {
	logs.Debug("SearchContacts - search.go")
	logs.Debug("args.Search.Query:%s", args.Search.Query)
	logs.Debug("args.Search.Fields:%s", args.Search.Fields)

	var bq elastic.BoolQuery

	//construction de la query - commun avec findcontacts----------
	err := BuildQuery(args,&bq)
	if err != nil {
		logs.Error(err)
		return err
	}

	var refInterval []string
	refInterval=append(refInterval,"now/d") //0
	refInterval=append(refInterval,"now-18y/d") //1
	refInterval=append(refInterval,"now-25y/d") //2
	refInterval=append(refInterval,"now-35y/d") //3
	refInterval=append(refInterval,"now-50y/d") //4
	refInterval=append(refInterval,"now-65y/d") //5
	refInterval=append(refInterval,"now-150y/d") //6

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
	aggreg_kpi_birthdate[0]=elastic.NewMissingAggregation().Field("birthdate")
	for index_agecat := 1; index_agecat < 7; index_agecat++ {
		aggreg_kpi_birthdate[index_agecat]=elastic.NewDateRangeAggregation().Field("birthdate").Between(refInterval[index_agecat],refInterval[index_agecat-1])
	}

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
	Aggregation("6_aggreg", aggreg_kpi_birthdate[6].(elastic.DateRangeAggregation))


	Filter := elastic.NewGeoPolygonFilter("location")
	if (len(args.Search.Polygon)>0){
		var point models.Point
		for _, point = range args.Search.Polygon {
			geoPoint := elastic.GeoPointFromLatLon(point.Lat, point.Lon)
			Filter = Filter.AddPoint(geoPoint)
		}
		searchService.Query(elastic.NewFilteredQuery(bq).Filter(Filter))
	}else{
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


	a0_agg, found6  := searchResult.Aggregations.Missing("0_aggreg")
	a1_agg, found7  := searchResult.Aggregations.DateRange("1_aggreg")
	a2_agg, found8  := searchResult.Aggregations.DateRange("2_aggreg")
	a3_agg, found9  := searchResult.Aggregations.DateRange("3_aggreg")
	a4_agg, found10 := searchResult.Aggregations.DateRange("4_aggreg")
	a5_agg, found11 := searchResult.Aggregations.DateRange("5_aggreg")
	a6_agg, found12 := searchResult.Aggregations.DateRange("6_aggreg")

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
		//---- gestion du missing gender --------
			var kpiAtom models.KpiReply
			kpiAtom.Key="missing"
			kpiAtom.Doc_count=gender_missing_agg.DocCount
			if (gender_missing_agg.DocCount>0){
				tab_kpiAtom.KpiReplies=append(tab_kpiAtom.KpiReplies, kpiAtom)
			}
		// ---------------------------------------
			reply.Kpi=append(reply.Kpi, tab_kpiAtom)
			tab_kpiAtom = models.KpiAggs{}

		// ---- stockage réponses pour pollingstation_aggreg -----------------------
			for _, bucket := range pollingstation_agg.Buckets {
				var kpiAtom models.KpiReply
				if (bucket.Key.(string)==""){
					kpiAtom.Key="missing"
				}else{
					kpiAtom.Key=bucket.Key.(string)
				}
				kpiAtom.Doc_count=bucket.DocCount
				tab_kpiAtom.KpiReplies=append(tab_kpiAtom.KpiReplies, kpiAtom)
			}
		//---- gestion du missing pollingstation --------
			var kpiAtom2 models.KpiReply
			kpiAtom2.Key="missing"
			kpiAtom2.Doc_count=pollingstation_missing_agg.DocCount
			if(pollingstation_missing_agg.DocCount>0){
				tab_kpiAtom.KpiReplies=append(tab_kpiAtom.KpiReplies, kpiAtom2)
			}
		// ---------------------------------------
			reply.Kpi=append(reply.Kpi, tab_kpiAtom)
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
			tab_category_Age[int(bucket.Key.(float64))]=bucket.DocCount
			sum_category_Age= sum_category_Age+bucket.DocCount
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
					kpiAtom2.Key="0"
					// calcul du nombre d'âge manquant : somme des missing Birthdate - (somme des aggreg des cat d'âge - cat âge manquant)
					kpiAtom2.Doc_count=a0_agg.DocCount-(sum_category_Age-tab_category_Age[0])
					//on ajoute dans le tableau KpiReplies, la struct de résultats
					tab_kpiAtom.KpiReplies=append(tab_kpiAtom.KpiReplies, kpiAtom2)

			case 1:
				for _, bucket := range a1_agg.Buckets {
					var kpiAtom models.KpiReply
					kpiAtom.Key="1"
					kpiAtom.Doc_count=bucket.DocCount+tab_category_Age[index_agecategory]
					tab_kpiAtom.KpiReplies=append(tab_kpiAtom.KpiReplies, kpiAtom)
				}
			case 2:
				for _, bucket := range a2_agg.Buckets {
					var kpiAtom models.KpiReply
					kpiAtom.Key="2"
					kpiAtom.Doc_count=bucket.DocCount+tab_category_Age[index_agecategory]
					tab_kpiAtom.KpiReplies=append(tab_kpiAtom.KpiReplies, kpiAtom)
				}
			case 3:
				for _, bucket := range a3_agg.Buckets {
					var kpiAtom models.KpiReply
					kpiAtom.Key="3"
					kpiAtom.Doc_count=bucket.DocCount+tab_category_Age[index_agecategory]
					tab_kpiAtom.KpiReplies=append(tab_kpiAtom.KpiReplies, kpiAtom)
				}
			case 4:
				for _, bucket := range a4_agg.Buckets {
					var kpiAtom models.KpiReply
					kpiAtom.Key="4"
					kpiAtom.Doc_count=bucket.DocCount+tab_category_Age[index_agecategory]
					tab_kpiAtom.KpiReplies=append(tab_kpiAtom.KpiReplies, kpiAtom)
				}
			case 5:
				for _, bucket := range a5_agg.Buckets {
					var kpiAtom models.KpiReply
					kpiAtom.Key="5"
					kpiAtom.Doc_count=bucket.DocCount+tab_category_Age[index_agecategory]
					tab_kpiAtom.KpiReplies=append(tab_kpiAtom.KpiReplies, kpiAtom)
				}
			case 6:
				for _, bucket := range a6_agg.Buckets {
					var kpiAtom models.KpiReply
					kpiAtom.Key="6"
					kpiAtom.Doc_count=bucket.DocCount+tab_category_Age[index_agecategory]
					tab_kpiAtom.KpiReplies=append(tab_kpiAtom.KpiReplies, kpiAtom)
				}
			default:
				logs.Critical("problem ")
				err := errors.New("wrong age_category parameter")
				return err
			}
		}
		reply.Kpi=append(reply.Kpi, tab_kpiAtom)
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
			kpiAtom.Key=*bucket.KeyAsString
			kpiAtom.Doc_count=bucket.DocCount
			tab_kpiAtom.KpiReplies=append(tab_kpiAtom.KpiReplies, kpiAtom)
		}
		reply.Kpi=append(reply.Kpi, tab_kpiAtom)
		tab_kpiAtom = models.KpiAggs{}

		// ---------------------------------------------------------------------

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
