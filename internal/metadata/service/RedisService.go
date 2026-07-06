package service

import (
	"context"
	"errors"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/arbazshaikh150/Distributed-Daftar/internal/metadata/cache"
	"github.com/arbazshaikh150/Distributed-Daftar/internal/metadata/dto"
	"github.com/arbazshaikh150/Distributed-Daftar/internal/metadata/enums"
	"github.com/arbazshaikh150/Distributed-Daftar/internal/metadata/model"
)

func nodeCacheKey(id uuid.UUID) string {
	return "node:" + id.String()
}

func nodeCacheTTL() time.Duration {
	retryCount, err := strconv.Atoi(os.Getenv("RETRY_COUNT"))
	if err != nil {
		retryCount = 3
	}

	heartbeat, err := strconv.Atoi(os.Getenv("HEARTBEAT_INTERVAL"))
	if err != nil {
		heartbeat = 5
	}

	return time.Duration(retryCount*heartbeat) * time.Second
}

func RedisHashSet(key string, fields map[string]any, ttl time.Duration) error {
	ctx := context.Background()

	if err := cache.RedisClient.HSet(ctx, key, fields).Err(); err != nil {
		return err
	}

	if ttl > 0 {
		if err := cache.RedisClient.Expire(ctx, key, ttl).Err(); err != nil {
			return err
		}
	}

	return nil
}

func RedisHashGetAll(key string) (map[string]string, error) {
	ctx := context.Background()
	return cache.RedisClient.HGetAll(ctx, key).Result()
}

func RedisKeys(pattern string) ([]string, error) {
	ctx := context.Background()
	return cache.RedisClient.Keys(ctx, pattern).Result()
}

func RedisKeyExists(key string) (bool, error) {
	ctx := context.Background()
	count, err := cache.RedisClient.Exists(ctx, key).Result()
	if err != nil {
		return false, err
	}

	return count > 0, nil
}

func SaveNodeCache(id uuid.UUID, availableSpace int64) error {
	fields := map[string]any{
		"availableSpace": availableSpace,
	}

	return RedisHashSet(nodeCacheKey(id), fields, nodeCacheTTL())
}

func NodeResponseFromCache(node model.NodesData) (dto.NodeResponse, error) {
	key := nodeCacheKey(node.NodeId)

	exists, err := RedisKeyExists(key)
	if err != nil {
		return dto.NodeResponse{}, err
	}

	status := enums.INACTIVE
	if exists {
		status = enums.Active
	}

	data, err := RedisHashGetAll(key)
	if err != nil {
		return dto.NodeResponse{}, err
	}

	availableCapacity, err := strconv.ParseInt(data["availableSpace"], 10, 64)
	if err != nil {
		availableCapacity = 0
	}

	return dto.NodeResponse{
		NodeId:            node.NodeId,
		Host:              node.Host,
		Port:              node.Port,
		Status:            status,
		TotalCapacity:     node.TotalCapacity,
		AvailableCapacity: availableCapacity,
	}, nil
}

func GetAvailableNodeWithCapExclude(reqSpace int64, reqNodes int, exclude map[uuid.UUID]bool,
) ([]uuid.UUID, error) {
	if reqSpace <= 0 {
		return nil, errors.New("required space must be greater than zero")
	}

	if reqNodes <= 0 {
		return []uuid.UUID{}, nil
	}

	keys, err := RedisKeys("node:*")
	if err != nil {
		return nil, err
	}

	availableNodes := []uuid.UUID{}

	for _, key := range keys {
		data, err := RedisHashGetAll(key)
		if err != nil {
			return nil, err
		}

		availableSpace, err := strconv.ParseInt(data["availableSpace"], 10, 64)
		if err != nil {
			continue
		}

		if availableSpace < reqSpace {
			continue
		}

		nodeIdString := strings.TrimPrefix(key, "node:")
		nodeId, err := uuid.Parse(nodeIdString)
		if err != nil {
			continue
		}

		if exclude[nodeId] {
			continue
		}

		availableNodes = append(availableNodes, nodeId)

		if len(availableNodes) == reqNodes {
			break
		}
	}

	if len(availableNodes) < reqNodes {
		return nil, errors.New("not enough extra active nodes for repair")
	}

	return availableNodes, nil
}
