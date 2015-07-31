package main

import (
	"net/http"
	"os"
	"runtime"
	"strconv"

	"github.com/codegangsta/cli"
	"github.com/jinzhu/gorm"
	"github.com/quorumsco/application"
	"github.com/quorumsco/cmd"
	"github.com/quorumsco/contacts/controllers"
	"github.com/quorumsco/contacts/models"
	"github.com/quorumsco/databases"
	"github.com/quorumsco/jsonapi"
	"github.com/quorumsco/logs"
	"github.com/quorumsco/router"
	"github.com/quorumsco/settings"
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

	var app = application.New()
	if app.Components["DB"], err = databases.InitGORM(dialect, args); err != nil {
		logs.Critical(err)
		os.Exit(1)
	}

	logs.Debug("connected to %s", args)

	if config.Migrate() {
		if err = migrate(app.Components["DB"].(*gorm.DB)); err != nil {
			logs.Critical(err)
			os.Exit(1)
		}
		logs.Debug("database migrated successfully")
	}

	app.Components["Mux"] = router.New()

	if config.Debug() {
		app.Use(router.Logger)
	}

	app.Use(app.Apply)
	app.Use(cors)
	app.Use(getUID)

	app.Post("/contacts", controllers.CreateContact)
	app.Options("/contacts", controllers.ContactCollectionOptions) // Required for CORS
	app.Get("/contacts", controllers.RetrieveContactCollection)

	app.Get("/contacts/:id", controllers.RetrieveContact)
	app.Patch("/contacts/:id", controllers.Upcd dateContact)
	app.Options("/contacts/:id", controllers.ContactOptions) // Required for CORS
	app.Delete("/contacts/:id", controllers.DeleteContact)   // Required for CORS

	app.Post("/contacts/:id/notes", controllers.CreateNote)
	app.Get("/contacts/:id/notes", controllers.RetrieveNoteCollection)

	// app.Get("/contacts/:id/notes/:node_id", controllers.RetrieveNote)
	app.Delete("/contacts/:id/notes/:node_id", controllers.DeleteNote)

	// app.Get("/contacts/:id/tags", controllers.RetrieveTagsByContact)

	var server settings.Server
	server, err = config.Server()
	if err != nil {
		logs.Critical(err)
		os.Exit(1)
	}
	return app.Serve(server.String())
}

func cors(h http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Headers", "access-control-allow-origin,content-type")
		h.ServeHTTP(w, r)
	}
	return http.HandlerFunc(fn)
}

func getUID(h http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		var (
			res    int
			userID uint
			err    error

			query = r.URL.Query()
		)
		res, err = strconv.Atoi(query.Get("user_id"))
		if err != nil {
			logs.Debug(err)
			jsonapi.Error(w, r, err.Error(), http.StatusBadRequest)
			return
		}
		userID = uint(res)
		router.Context(r).Env["UserID"] = userID
		h.ServeHTTP(w, r)
	}
	return http.HandlerFunc(fn)
}

func migrate(db *gorm.DB) error {
	db.LogMode(true)
	db.AutoMigrate(models.Models()...)

	return nil
}
