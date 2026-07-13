package database

import (
	"fmt"
	"os"

	"github.com/arbazshaikh150/Distributed-Daftar/internal/metadata/model"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

var DB *gorm.DB

// Connecting with the database
func ConnectToDb() error {
	dsn := fmt.Sprintf(
		"host=%s user=%s password=%s dbname=%s port=%s sslmode=disable",
		os.Getenv("DB_HOST"),
		os.Getenv("DB_USER"),
		os.Getenv("DB_PASSWORD"),
		os.Getenv("DB_NAME"),
		os.Getenv("DB_PORT"),
	)

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})

	if err != nil {
		return err
	}

	DB = db
	return nil
}

func AutoMigrate() error {
	if err := DB.Exec("CREATE EXTENSION IF NOT EXISTS pgcrypto").Error; err != nil {
		return err
	}

	return DB.AutoMigrate(
		&model.NodesData{},
		&model.Metadata{},
		&model.ReplicaData{},
		&model.OutboxEvent{},
	)
}
