package cache

import (
	"context"
	"os"
	"strconv"

	"github.com/redis/go-redis/v9"
)

var RedisClient *redis.Client

func ConnectToRedis() error {
	redisDB, err := strconv.Atoi(os.Getenv("REDIS_DB"))
	if err != nil {
		redisDB = 0
	}

	// Trying to connect to redis
	RedisClient = redis.NewClient(&redis.Options{
		Addr:     os.Getenv("REDIS_ADDR"),
		Password: os.Getenv("REDIS_PASSWORD"),
		DB:       redisDB,
	})

	context := context.Background()

	if err := RedisClient.Ping(context).Err(); err != nil {
		return err
	}
	return nil
}
