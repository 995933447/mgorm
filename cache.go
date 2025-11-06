package mgorm

import (
	"errors"
	"math/rand"
	"sync"
	"time"

	"github.com/gomodule/redigo/redis"
	jsoniter "github.com/json-iterator/go"
)

var DefaultCache Cache = NilCache{}

type NilCache struct {
}

func (n NilCache) Get(key string, val any) (bool, error) {
	return false, nil
}

func (n NilCache) Set(key string, val any) error {
	return nil
}

func (n NilCache) Del(key string) error {
	return nil
}

var _ Cache = (*NilCache)(nil)

func NewMemoryCache(expireTime time.Duration) *MemoryCache {
	cache := &MemoryCache{
		expireTime: expireTime,
	}
	go func() {
		tk := time.NewTicker(expireTime)
		defer tk.Stop()
		for {
			<-tk.C
			cache.Range(func(k, v any) bool {
				item := v.(*MemoryCacheItem)
				if item.CreatedAt.Add(expireTime).After(time.Now()) {
					return true
				}
				cache.Delete(k)
				return true
			})
		}
	}()
	return cache
}

type MemoryCacheItem struct {
	Value     any
	CreatedAt time.Time
}

type MemoryCache struct {
	sync.Map
	expireTime time.Duration
}

func (m *MemoryCache) Get(key string, val any) (bool, error) {
	v, ok := m.Map.Load(key)
	if ok {
		item := v.(*MemoryCacheItem)
		if !item.CreatedAt.Add(m.expireTime).After(time.Now()) {
			m.Map.Delete(key)
			return false, nil
		}

		str, err := jsoniter.Marshal(item.Value)
		if err != nil {
			return false, err
		}

		err = jsoniter.Unmarshal(str, val)
		if err == nil {
			return true, nil
		}
	}
	return false, nil
}

func (m *MemoryCache) Set(key string, val any) error {
	m.Map.Store(key, &MemoryCacheItem{
		Value:     val,
		CreatedAt: time.Now(),
	})
	return nil
}

func (m *MemoryCache) Del(key string) error {
	m.Map.Delete(key)
	return nil
}

var _ Cache = (*MemoryCache)(nil)

type RedisPool interface {
	Get() redis.Conn
	Close() error
}

func NewRedisCache(redisPool RedisPool, onCmdExec func(ttl int64, err error, cost time.Duration, cmd string, key string, args ...interface{})) *RedisCache {
	return &RedisCache{
		redisPool: redisPool,
		onCmdExec: onCmdExec,
	}
}

type RedisCache struct {
	redisPool RedisPool
	onCmdExec func(ttl int64, err error, cost time.Duration, cmd string, key string, args ...interface{})
}

func (r *RedisCache) Get(key string, val any) (bool, error) {
	reply, err := redis.String(r.execCmd(0, "GET", key))
	if err != nil {
		if errors.Is(err, redis.ErrNil) {
			return false, nil
		}
		return false, err
	}
	if err = jsoniter.UnmarshalFromString(reply, val); err != nil {
		return false, err
	}
	return true, nil
}

func (r *RedisCache) Set(key string, val any) error {
	rand.New(rand.NewSource(time.Now().UnixNano()))
	j, err := jsoniter.MarshalToString(val)
	if err != nil {
		return err
	}
	_, err = r.execCmd(rand.Int63n(3600)+3600, "SET", key, j)
	if err != nil {
		return err
	}
	return nil
}

func (r *RedisCache) Del(key string) error {
	_, err := r.execCmd(0, "DEL", key)

	go func() {
		time.Sleep(time.Millisecond * 500)
		_, _ = r.execCmd(0, "DEL", key)
	}()

	if err != nil {
		return err
	}

	return nil
}

func (r *RedisCache) execCmd(ttl int64, cmd, key string, args ...interface{}) (interface{}, error) {
	var err error
	
	if r.onCmdExec != nil {
		start := time.Now()
		defer func() {
			r.onCmdExec(ttl, err, time.Since(start), cmd, key, args...)
		}()
	}

	conn := r.redisPool.Get()

	err = conn.Err()
	if err != nil {
		return 0, err
	}

	defer conn.Close()

	reply, err := conn.Do(cmd, append([]interface{}{key}, args...)...)
	if err == nil {
		if ttl > 0 {
			_, _ = conn.Do("EXPIRE", key, ttl)
		}
	}

	return reply, err
}

var _ Cache = (*RedisCache)(nil)
