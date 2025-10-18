package main

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/995933447/mgorm"
	"github.com/995933447/mgorm/mgorm-example/user"
	"github.com/gomodule/redigo/redis"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

func TestModel(t *testing.T) {
	_, err := mgorm.ConnectDefault(&mgorm.ConnConfig{
		Scheme:          "mongodb",
		Hosts:           []string{"127.0.0.1:27017"},
		User:            "root",
		Password:        "123456",
		MinPoolSize:     10,
		MaxPoolSize:     64,
		ConnIdleTimeSec: 5,
	})
	if err != nil {
		t.Error(err)
		return
	}

	mgorm.OnQueryDone = func(orm *mgorm.Orm, method string, res any, err error, cost time.Duration, args ...any) {
		t.Log("mgorm:", method, "result", res, "error", err, "cost", cost, "args", args)
	}

	mgorm.DefaultCache = mgorm.NewRedisCache(&redis.Pool{
		MaxIdle:         2,
		MaxActive:       0, //when zero,there's no limit. https://godoc.org/github.com/garyburd/redigo/redis#Pool
		IdleTimeout:     time.Minute * 3,
		MaxConnLifetime: time.Minute * 10,
		Wait:            true,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", "127.0.0.1:6379")
			if err != nil {
				return nil, err
			}
			return c, nil
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			return nil
		},
	})

	mod := user.NewUserModel("def", 20001, 20101, "hash")
	entity, err := mod.FindOneById(context.TODO(), 1213)
	if err != nil {
		if !errors.Is(err, mongo.ErrNoDocuments) {
			t.Error(err)
			return
		}
		entity = &user.UserOrm{
			Id:       1213,
			Username: fmt.Sprintf("user%d", time.Now().Unix()),
			Email:    fmt.Sprintf("userEmal%d", time.Now().Unix()),
			Address: &user.AddressOrm{
				City: fmt.Sprintf("City%d", time.Now().Unix()),
			},
			RegisterAt: time.Now(),
		}
		if err = mod.InsertOne(context.TODO(), entity); err != nil {
			t.Error(err)
			return
		}
	}

	id := entity.ID

	entity, err = mod.FindOneByID(context.TODO(), id.Hex())
	if err != nil {
		t.Error(err)
		return
	}

	entity, err = mod.FindOneByID(context.TODO(), id.Hex())
	if err != nil {
		t.Error(err)
		return
	}

	t.Log(entity)

	examples, err := mod.FindAll(context.Background(), bson.M{})
	if err != nil {
		t.Error(err)
		return
	}

	t.Log(examples)

	time.Sleep(time.Second * 5)
}
