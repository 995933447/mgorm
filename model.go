package mgorm

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

type Model[T any] struct {
	Cached bool
	*Orm
}

func (m *Model[T]) FindOne(ctx context.Context, filter any) (*T, error) {
	var data T
	err := m.Orm.FindOne(ctx, filter, &data)
	if err != nil {
		return nil, err
	}
	return &data, nil
}

func (m *Model[T]) FindOneByID(ctx context.Context, id string) (*T, error) {
	objId, err := primitive.ObjectIDFromHex(id)
	if err != nil {
		return nil, err
	}

	var data T
	if m.Cached {
		cacheKey := fmt.Sprintf("_id.%v", id)
		err = m.Orm.FindOneByCacheKey(ctx, bson.M{"_id": objId}, cacheKey, &data)
	} else {
		err = m.Orm.FindOne(ctx, bson.M{"_id": objId}, &data)
	}
	if err != nil {
		return nil, err
	}
	return &data, nil
}

func (m *Model[T]) FindMany(ctx context.Context, filter any, sort bson.D, limit int64, selectors ...any) ([]*T, error) {
	cursor, err := m.Orm.FindMany(ctx, filter, sort, limit, selectors...)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)
	data, err := m.Decode(ctx, cursor)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (m *Model[T]) FindManyByPage(ctx context.Context, filter any, sort bson.D, page, pageSize int64, selectors ...any) ([]*T, error) {
	skip := (page - 1) * pageSize
	if skip < 0 {
		skip = 0
	}
	cursor, err := m.Orm.FindManyByPage(ctx, filter, sort, skip, pageSize, selectors...)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)
	data, err := m.Decode(ctx, cursor)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (m *Model[T]) FindAll(ctx context.Context, filter any, selectors ...any) ([]*T, error) {
	cursor, err := m.Orm.FindAll(ctx, filter, selectors...)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)
	data, err := m.Decode(ctx, cursor)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (m *Model[T]) FindAllBySort(ctx context.Context, filter any, sort bson.D, selectors ...any) ([]*T, error) {
	cursor, err := m.Orm.FindAllBySort(ctx, filter, sort, selectors...)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)
	data, err := m.Decode(ctx, cursor)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (m *Model[T]) Decode(ctx context.Context, cursor *mongo.Cursor) ([]*T, error) {
	var data []*T
	for cursor.Next(ctx) {
		var item T
		err := cursor.Decode(&item)
		if err != nil {
			return nil, err
		}
		data = append(data, &item)
	}
	return data, nil
}
