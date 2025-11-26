package mgorm

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

type ExampleOrm struct {
	ID          primitive.ObjectID `json:"_id,omitempty" bson:"_id"`
	ExampleName string             `bson:"example_name"`
	ExampleId   uint64             `bson:"example_id"`
	Desc        *ExampleDesc       `bson:"desc"`
	ExpiredAt   time.Time          `bson:"expired_at"`
}

type ExampleDesc struct {
	Description string `bson:"description"`
}

type ExampleModel struct {
	Model[ExampleOrm]
}

func NewExampleModel() *ExampleModel {
	orm := NewOrm(
		"",
		"example_db",
		"example",
		true,
		NewMemoryCache(time.Minute),
		[]string{"example_name"},
		[]string{"example_id"},
		[]string{"expired_at"},
	)
	orm.SetOnQueryDone(func(orm *Orm, method string, res any, err error, cost time.Duration, args ...interface{}) {
		fmt.Println("mgorm log:", method, "res:", "err", err, "cost", cost, "args", args)
	})
	return &ExampleModel{
		Model[ExampleOrm]{
			Cached: true,
			Orm:    orm,
		},
	}
}

func TestModel(t *testing.T) {
	_, err := ConnectDefault(&ConnConfig{
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

	id := primitive.NewObjectID()

	mod := NewExampleModel()
	example, err := mod.FindOneByID(context.TODO(), id.Hex())
	if err != nil {
		if !errors.Is(err, mongo.ErrNoDocuments) {
			t.Error(err)
			return
		}
		example = &ExampleOrm{
			ID:          id,
			ExampleName: "Example Name",
			ExampleId:   uint64(time.Now().Unix()),
			Desc: &ExampleDesc{
				Description: "Example Desc",
			},
			ExpiredAt: time.Now().Add(time.Minute * 10),
		}
		if _, err = mod.InsertOne(context.TODO(), example); err != nil {
			t.Error(err)
			return
		}
	}

	example, err = mod.FindOneByID(context.TODO(), id.Hex())
	if err != nil {
		t.Error(err)
		return
	}

	example, err = mod.FindOneByID(context.TODO(), id.Hex())
	if err != nil {
		t.Error(err)
		return
	}

	t.Log(example)

	examples, err := mod.FindAll(context.Background(), bson.M{})
	if err != nil {
		t.Error(err)
		return
	}

	t.Log(examples)

	time.Sleep(time.Second * 5)
}
