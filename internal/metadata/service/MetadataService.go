package service

import (
	"errors"

	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/arbazshaikh150/Distributed-Daftar/internal/database"
	"github.com/arbazshaikh150/Distributed-Daftar/internal/metadata/dto"
	"github.com/arbazshaikh150/Distributed-Daftar/internal/metadata/model"
)

func GetFileData(fileId uuid.UUID) (model.Metadata, error) {
	var data model.Metadata
	err := database.DB.First(&data, "file_id = ?", fileId).Error
	if err != nil {
		return model.Metadata{}, err
	}

	return data, nil
}

func GetFileLocation(fileId uuid.UUID) (dto.LocationDTO, error) {
	data, err := GetFileData(fileId)
	if err != nil {
		return dto.LocationDTO{}, err
	}

	return dto.LocationDTO{
		Version:      data.Version,
		PrimaryNode:  data.PrimaryNode,
		ReplicaNodes: data.ReplicaNodes,
	}, nil
}

func UpdateFileVersion(fileId uuid.UUID, version int) (dto.LocationDTO, error) {
	if version <= 0 {
		return dto.LocationDTO{}, errors.New("version must be greater than zero")
	}

	var data model.Metadata
	err := database.DB.Transaction(func(tx *gorm.DB) error {
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			First(&data, "file_id = ?", fileId).Error; err != nil {
			return err
		}

		data.Version = version

		if err := tx.Save(&data).Error; err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return dto.LocationDTO{}, err
	}

	return dto.LocationDTO{
		Version:      data.Version,
		PrimaryNode:  data.PrimaryNode,
		ReplicaNodes: data.ReplicaNodes,
	}, nil
}
