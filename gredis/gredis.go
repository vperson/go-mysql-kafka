package gredis

import (
	"github.com/go-redis/redis"
	log "github.com/sirupsen/logrus"
	"go-mysql-kafka/conf"
	"time"
)

var db *redis.Client

func Setup() {
	config := conf.Config.Redis
	db = redis.NewClient(&redis.Options{
		Addr:        config.Host,
		Password:    config.Password,
		DB:          config.DB,
		IdleTimeout: config.IdleTimeout,
		PoolSize:    config.PoolSize,
		MaxRetries:  config.MaxRetries,
	})

	pong, err := db.Ping().Result()
	if err != nil {
		log.Fatalf("redis 连接失败 err: %v", err)
	}

	log.Infof("redis 连接成功: %v", pong)

	go pingLoop()
}

func pingLoop() {
	for {
		_, err := db.Ping().Result()
		if err != nil {
			log.Fatalf("redis 连接失败 err: %v", err)
		}

		time.Sleep(30)
	}
}

func Close() {
	err := db.Close()
	if err != nil {
		log.Errorf("close redis err: %v", err)
	}
}

// 在redis插入数据
func Set(key string, value interface{}, timeout time.Duration) error {
	t := timeout * time.Second
	err := db.Set(key, value, t).Err()
	if err != nil {
		return err
	}

	return nil
}

// 查询redis key是否存在
func Exist(key string) (exist bool, err error) {
	_, err = db.Get(key).Result()
	if err == redis.Nil {
		return false, nil
	} else if err != nil {
		return false, err
	}

	return true, nil
}

// 获取key
func Get(key string) (val []byte, err error) {
	return db.Get(key).Bytes()
}

// 获取key
func GetString(key string) (val string, err error) {
	return db.Get(key).Result()
}

// 删除key
func Delete(key string) error {
	return db.Del(key).Err()
}

// 批量删除
func LikeDelete(key string) error {
	keys, err := db.Keys(key).Result()
	if err != nil {
		return err
	}

	for _, key := range keys {
		err = Delete(key)
		if err != nil {
			return err
		}
	}
	return nil
}
