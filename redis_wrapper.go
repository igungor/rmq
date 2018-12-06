package rmq

import (
	"log"
	"time"

	"github.com/go-redis/redis"
)

type RedisClusterWrapper struct {
	rawClient *redis.ClusterClient
}

func (rcw RedisClusterWrapper) Set(key string, value string, expiration time.Duration) bool {
	return checkErr(rcw.rawClient.Set(key, value, expiration).Err())
}

func (rcw RedisClusterWrapper) Del(key string) (affected int, ok bool) {
	n, err := rcw.rawClient.Del(key).Result()
	ok = checkErr(err)
	if !ok {
		return 0, false
	}
	return int(n), ok
}

func (rcw RedisClusterWrapper) TTL(key string) (ttl time.Duration, ok bool) {
	ttl, err := rcw.rawClient.TTL(key).Result()
	ok = checkErr(err)
	if !ok {
		return 0, false
	}
	return ttl, ok
}

func (rcw RedisClusterWrapper) LPush(key, value string) bool {
	return checkErr(rcw.rawClient.LPush(key, value).Err())
}

func (rcw RedisClusterWrapper) LLen(key string) (affected int, ok bool) {
	n, err := rcw.rawClient.LLen(key).Result()
	ok = checkErr(err)
	if !ok {
		return 0, false
	}
	return int(n), ok
}

func (rcw RedisClusterWrapper) LRem(key string, count int, value string) (affected int, ok bool) {
	n, err := rcw.rawClient.LRem(key, int64(count), value).Result()
	return int(n), checkErr(err)
}

func (rcw RedisClusterWrapper) LTrim(key string, start, stop int) {
	checkErr(rcw.rawClient.LTrim(key, int64(start), int64(stop)).Err())
}

func (rcw RedisClusterWrapper) RPopLPush(source, destination string) (value string, ok bool) {
	value, err := rcw.rawClient.RPopLPush(source, destination).Result()
	return value, checkErr(err)
}

func (rcw RedisClusterWrapper) SAdd(key, value string) bool {
	return checkErr(rcw.rawClient.SAdd(key, value).Err())
}

func (rcw RedisClusterWrapper) SMembers(key string) []string {
	members, err := rcw.rawClient.SMembers(key).Result()
	if ok := checkErr(err); !ok {
		return []string{}
	}
	return members
}

func (rcw RedisClusterWrapper) SRem(key, value string) (affected int, ok bool) {
	n, err := rcw.rawClient.SRem(key, value).Result()
	ok = checkErr(err)
	if !ok {
		return 0, false
	}
	return int(n), ok
}

func (rcw RedisClusterWrapper) FlushDb() {
	rcw.rawClient.FlushDb()
}

type RedisWrapper struct {
	rawClient *redis.Client
}

func (wrapper RedisWrapper) Set(key string, value string, expiration time.Duration) bool {
	return checkErr(wrapper.rawClient.Set(key, value, expiration).Err())
}

func (wrapper RedisWrapper) Del(key string) (affected int, ok bool) {
	n, err := wrapper.rawClient.Del(key).Result()
	ok = checkErr(err)
	if !ok {
		return 0, false
	}
	return int(n), ok
}

func (wrapper RedisWrapper) TTL(key string) (ttl time.Duration, ok bool) {
	ttl, err := wrapper.rawClient.TTL(key).Result()
	ok = checkErr(err)
	if !ok {
		return 0, false
	}
	return ttl, ok
}

func (wrapper RedisWrapper) LPush(key, value string) bool {
	return checkErr(wrapper.rawClient.LPush(key, value).Err())
}

func (wrapper RedisWrapper) LLen(key string) (affected int, ok bool) {
	n, err := wrapper.rawClient.LLen(key).Result()
	ok = checkErr(err)
	if !ok {
		return 0, false
	}
	return int(n), ok
}

func (wrapper RedisWrapper) LRem(key string, count int, value string) (affected int, ok bool) {
	n, err := wrapper.rawClient.LRem(key, int64(count), value).Result()
	return int(n), checkErr(err)
}

func (wrapper RedisWrapper) LTrim(key string, start, stop int) {
	checkErr(wrapper.rawClient.LTrim(key, int64(start), int64(stop)).Err())
}

func (wrapper RedisWrapper) RPopLPush(source, destination string) (value string, ok bool) {
	value, err := wrapper.rawClient.RPopLPush(source, destination).Result()
	return value, checkErr(err)
}

func (wrapper RedisWrapper) SAdd(key, value string) bool {
	return checkErr(wrapper.rawClient.SAdd(key, value).Err())
}

func (wrapper RedisWrapper) SMembers(key string) []string {
	members, err := wrapper.rawClient.SMembers(key).Result()
	if ok := checkErr(err); !ok {
		return []string{}
	}
	return members
}

func (wrapper RedisWrapper) SRem(key, value string) (affected int, ok bool) {
	n, err := wrapper.rawClient.SRem(key, value).Result()
	ok = checkErr(err)
	if !ok {
		return 0, false
	}
	return int(n), ok
}

func (wrapper RedisWrapper) FlushDb() {
	wrapper.rawClient.FlushDb()
}

func checkErr(err error) bool {
	switch err {
	case nil:
		return true
	case redis.Nil:
		return false
	default:
		log.Printf("rmq: %v", err)
		return false
	}
}
