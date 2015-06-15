package database

import (
	_ "github.com/go-sql-driver/mysql"
	"github.com/jinzhu/gorm"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"

	"github.com/Quorumsco/contact/components/settings"
)

func Init() (*sqlx.DB, error) {
	var db *sqlx.DB
	var err error

	if db, err = sqlx.Connect(settings.DB.Engine, settings.DB.Source); err != nil {
		return nil, err
	}

	db.Ping()
	db.SetMaxIdleConns(10)
	db.SetMaxOpenConns(100)

	return db, nil
}

func Migrate(models []interface{}) error {
	db, err := gorm.Open(settings.DB.Engine, settings.DB.Source)
	if err != nil {
		return err
	}

	// Then you could invoke `*sql.DB`'s functions with it
	err = db.DB().Ping()
	if err != nil {
		return err
	}
	db.DB().SetMaxIdleConns(10)
	db.DB().SetMaxOpenConns(100)
	db.LogMode(true)

	db.AutoMigrate(models...)

	return nil
}
