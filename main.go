// Contact managing structure, it does all the database CRUD interactions and contains an implementation of elasticsearch engine
package main

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"runtime"
	"time"

	"github.com/codegangsta/cli"
	"github.com/jinzhu/gorm"
	"github.com/quorumsco/cmd"
	"github.com/quorumsco/contacts/controllers"
	"github.com/quorumsco/contacts/models"
	"github.com/quorumsco/databases"
	"github.com/quorumsco/logs"
	"github.com/quorumsco/settings"
	elastic "gopkg.in/olivere/elastic.v2"
)

var (
	//TIMEOUT time between each try
	TIMEOUT = 5 * time.Second
	//RETRY number of tries
	RETRY = 3
)

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU())
}

func main() {
	cmd := cmd.New()
	cmd.Name = "contacts"
	cmd.Usage = "quorums contacts backend"
	cmd.Version = "0.0.1"
	cmd.Before = serve
	cmd.Flags = append(cmd.Flags, []cli.Flag{
		cli.StringFlag{Name: "config, c", Usage: "configuration file", EnvVar: "CONFIG"},
		cli.HelpFlag,
	}...)
	cmd.RunAndExitOnError()
}

// Definition of the GORM and Elasticsearch clients and Registration of the functions to RPC with the said clients
func serve(ctx *cli.Context) error {
	var (
		config settings.Config
		err    error
	)

	if ctx.String("config") != "" {
		config, err = settings.Parse(ctx.String("config"))
		if err != nil {
			logs.Error(err)
		}
	}

	if config.Debug() {
		logs.Level(logs.DebugLevel)
	}

	dialect, args, err := config.SqlDB()
	if err != nil {
		logs.Critical(err)
		os.Exit(1)
	}
	logs.Debug("database type: %s", dialect)

	var db *gorm.DB
	if db, err = databases.InitGORM(dialect, args); err != nil {
		logs.Critical(err)
		os.Exit(1)
	}

	logs.Debug("connected to %s", args)

	if config.Migrate() {
		db.AutoMigrate(models.Models()...)
		logs.Debug("database migrated successfully")
	}

	if config.Debug() {
		db.LogMode(true)
	}

	var server settings.Server
	server, err = config.Server()
	if err != nil {
		logs.Critical(err)
		os.Exit(1)
	}

	ElasticSettings, err := config.Elasticsearch()
	var client *elastic.Client
	client, err = dialElasticRetry(ElasticSettings.String())
	if err != nil {
		logs.Critical(err)
		os.Exit(1)
	}

	checkIndex("contacts", client)

	rpc.Register(&controllers.Search{Client: client})
	rpc.Register(&controllers.Contact{DB: db})
	rpc.Register(&controllers.Note{DB: db})
	rpc.Register(&controllers.Formdata{DB: db})
	rpc.Register(&controllers.Tag{DB: db})
	rpc.HandleHTTP()

	l, e := net.Listen("tcp", server.String())
	if e != nil {
		log.Fatal("listen error:", e)
	}
	logs.Info("Listening on " + server.String())
	return http.Serve(l, nil)
}

func checkIndex(index string, client *elastic.Client) {
	// Use the IndexExists service to check if a specified index exists.
	exists, err := client.IndexExists(index).Do()
	if err != nil {
		logs.Critical(err)
		os.Exit(1)
	}
	if !exists {
		createIndex, err := client.CreateIndex(index).Do()
		if err != nil {
			logs.Critical(err)
		}
		if !createIndex.Acknowledged {
			logs.Critical("Index creation wasn't aknowledged")
		}
	}
}

// We need a retry because elasticsearch takes a bit of time to be up and running before we can connect to it
func dialElasticRetry(address string) (*elastic.Client, error) {
	var client *elastic.Client
	var err error

	var i int
retry:
	for {
		client, err = elastic.NewClient(elastic.SetSniff(false), elastic.SetURL(address))
		switch {
		case err == nil:
			break retry
		case i >= RETRY:
			return nil, err
		default:
			logs.Error(err)
			i++
		}
		time.Sleep(TIMEOUT)
	}

	return client, nil
}
