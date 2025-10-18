package mgorm

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
)

const ConnNameDefault = "default"

type ConnConfig struct {
	Scheme          string
	Hosts           []string
	Query           string
	User            string
	Password        string
	MaxPoolSize     uint64
	MinPoolSize     uint64
	ConnIdleTimeSec int
}

func (c *ConnConfig) GetConnFullUri() string {
	if c.Scheme == "" {
		c.Scheme = "mongodb" // mongodb+srv
	}
	var uri string
	for i, host := range c.Hosts {
		if i == 0 {
			uri = host
		} else {
			uri += fmt.Sprintf(",%s", host)
		}
	}
	url := fmt.Sprintf("%s://%s:%s@%s", c.Scheme, c.User, c.Password, uri)
	if c.Query != "" {
		url = url + "?" + c.Query
	}
	return url
}

var clientMap sync.Map

type ApplyConnOptFunc func(connOptions *ConnOptions)

type ConnOptions struct {
	WriteConcern   *writeconcern.WriteConcern
	ReadConcern    *readconcern.ReadConcern
	ReadPreference *readpref.ReadPref
	MinPoolSize    uint64
	MaxPoolSize    uint64
}

func WithWriteConcern(wc *writeconcern.WriteConcern) ApplyConnOptFunc {
	return func(opts *ConnOptions) {
		opts.WriteConcern = wc
	}
}

func WithReadConcern(rc *readconcern.ReadConcern) ApplyConnOptFunc {
	return func(opts *ConnOptions) {
		opts.ReadConcern = rc
	}
}

func WithReadPreference(pref *readpref.ReadPref) ApplyConnOptFunc {
	return func(opts *ConnOptions) {
		opts.ReadPreference = pref
	}
}

func Connect(connName string, cfg *ConnConfig, opts ...ApplyConnOptFunc) (*mongo.Client, error) {
	var connOpts ConnOptions
	for _, opt := range opts {
		opt(&connOpts)
	}
	if connOpts.WriteConcern == nil {
		connOpts.WriteConcern = writeconcern.Majority() // 请求确认写操作传播到大多数mongo实例
	}
	if connOpts.ReadConcern == nil {
		connOpts.ReadConcern = readconcern.Local() // primary 内存中已提交的写
	}
	if connOpts.ReadPreference == nil {
		connOpts.ReadPreference = readpref.SecondaryPreferred() // 优先读从库
	}
	if connOpts.MinPoolSize == 0 {
		connOpts.MinPoolSize = cfg.MinPoolSize
	}
	if connOpts.MaxPoolSize == 0 {
		connOpts.MaxPoolSize = cfg.MaxPoolSize
	}

	cliOpts := options.Client().ApplyURI(cfg.GetConnFullUri())
	cliOpts.SetMaxPoolSize(connOpts.MaxPoolSize) // 设置最大连接数
	cliOpts.SetMinPoolSize(connOpts.MinPoolSize) // 设置最小连接数
	cliOpts.SetWriteConcern(connOpts.WriteConcern)
	cliOpts.SetReadConcern(connOpts.ReadConcern)
	cliOpts.SetReadPreference(connOpts.ReadPreference)
	cliOpts.SetMaxConnIdleTime(time.Duration(cfg.ConnIdleTimeSec) * time.Second) // 设置连接空闲时间 超过就会断开
	client, err := mongo.Connect(context.TODO(), cliOpts)
	if err != nil {
		return nil, err
	}

	clientMap.Store(connName, client)

	return client, nil
}

func ConnectDefault(cfg *ConnConfig, opts ...ApplyConnOptFunc) (*mongo.Client, error) {
	return Connect(ConnNameDefault, cfg, opts...)
}

func GetClient(connName string) (*mongo.Client, error) {
	client, ok := clientMap.Load(connName)
	if !ok {
		return nil, ErrConnectionNotInitialized
	}

	return client.(*mongo.Client), nil
}

func GetDefaultClient() (*mongo.Client, error) {
	return GetClient(ConnNameDefault)
}
