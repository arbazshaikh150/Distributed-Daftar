package service

import (
	"context"
	"os"
	"strconv"
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

	status := enums.InActive
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
