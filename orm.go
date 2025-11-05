package mgorm

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func ToBsonM(data any) (bson.M, error) {
	bsonData, err := bson.Marshal(data)
	if err != nil {
		return nil, err
	}
	var bsonM bson.M
	err = bson.Unmarshal(bsonData, &bsonM)
	if err != nil {
		return nil, err
	}
	return bsonM, nil
}

type Cache interface {
	Get(key string, val any) (bool, error)
	Set(key string, val any) error
	Del(key string) error
}

func NewOrm(conn, db, tb string, cached bool, cache Cache, indexes, uniqIndexes, expireIndexes []string) *Orm {
	if conn == "" {
		conn = ConnNameDefault
	}

	o := &Orm{
		conn:            conn,
		db:              db,
		tb:              tb,
		cached:          cached,
		cache:           cache,
		indexKeys:       indexes,
		uniqueIndexKeys: uniqIndexes,
		expireIndexKeys: expireIndexes,
	}

	return o
}

type OnQueryDoneFunc func(orm *Orm, method string, res any, err error, cost time.Duration, args map[string]interface{})

var OnQueryDone = OnQueryDoneFunc(nil)

type Orm struct {
	conn            string
	cached          bool
	cache           Cache
	indexKeys       []string
	expireIndexKeys []string
	uniqueIndexKeys []string
	db              string
	tb              string
	onQueryDone     OnQueryDoneFunc
}

func (o *Orm) SetConn(connName string) {
	o.conn = connName
}

func (o *Orm) SetDb(db string) {
	o.db = db
}

func (o *Orm) SetTb(tb string) {
	o.tb = tb
}

func (o *Orm) GetDb() string {
	return o.db
}

func (o *Orm) GetTb() string {
	return o.tb
}

func (o *Orm) GetColl() (*mongo.Collection, error) {
	client, err := GetClient(o.conn)
	if err != nil {
		return nil, err
	}
	return client.Database(o.db).Collection(o.tb), nil
}

func (o *Orm) SetExpireIndexKey(keys []string) {
	o.expireIndexKeys = keys
}

func (o *Orm) SetOnQueryDone(fn OnQueryDoneFunc) {
	o.onQueryDone = fn
}

func (o *Orm) OnQueryDone(orm *Orm, method string, res any, err error, cost time.Duration, args map[string]interface{}) {
	if o.onQueryDone != nil {
		o.onQueryDone(orm, method, res, err, cost, args)
	}
}

func (o *Orm) InsertOneIgnoreConflict(ctx context.Context, data any) (res *mongo.InsertOneResult, err error) {
	if o.onQueryDone != nil {
		start := time.Now()

		defer func() {
			o.OnQueryDone(o, "InsertOneIgnoreConflict", res, err, time.Since(start), map[string]interface{}{
				"data": data,
			})
		}()
	}

	if err = o.CreatIndexes(ctx); err != nil {
		return
	}

	client, err := GetClient(o.conn)
	if err != nil {
		return
	}

	res, err = client.Database(o.db).Collection(o.tb).InsertOne(ctx, data)
	if err != nil {
		var mgoErr mongo.WriteException
		if errors.As(err, &mgoErr) {
			if len(mgoErr.WriteErrors) > 0 {
				e := mgoErr.WriteErrors[0]
				if e.Code == 11000 || e.Code == 11001 || e.Code == 12582 {
					err = nil
					return
				}
			}
		}
	}

	return
}

func (o *Orm) InsertOne(ctx context.Context, data any) (res *mongo.InsertOneResult, err error) {
	if o.onQueryDone != nil {
		start := time.Now()

		defer func() {
			o.OnQueryDone(o, "InsertOne", res, err, time.Since(start), map[string]interface{}{
				"data": data,
			})
		}()
	}

	if err = o.CreatIndexes(ctx); err != nil {
		return
	}

	client, err := GetClient(o.conn)
	if err != nil {
		return
	}

	res, err = client.Database(o.db).Collection(o.tb).InsertOne(ctx, data)

	return
}

func (o *Orm) InsertMany(ctx context.Context, data []any) (res *mongo.InsertManyResult, err error) {
	if o.onQueryDone != nil {
		start := time.Now()

		defer func() {
			o.OnQueryDone(o, "InsertMany", res, err, time.Since(start), bson.M{
				"data": data,
			})
		}()
	}

	if err = o.CreatIndexes(ctx); err != nil {
		return
	}

	client, err := GetClient(o.conn)
	if err != nil {
		return
	}

	res, err = client.Database(o.db).Collection(o.tb).InsertMany(ctx, data)

	return
}

func (o *Orm) InsertManyIgnoreConflict(ctx context.Context, data []any) (res *mongo.InsertManyResult, err error) {
	if o.onQueryDone != nil {
		start := time.Now()

		defer func() {
			o.OnQueryDone(o, "InsertManyIgnoreConflict", res, err, time.Since(start), map[string]interface{}{
				"data": data,
			})
		}()
	}

	if err = o.CreatIndexes(ctx); err != nil {
		return
	}

	client, err := GetClient(o.conn)
	res, err = client.Database(o.db).Collection(o.tb).InsertMany(ctx, data, options.InsertMany().SetOrdered(false))
	if err != nil {
		var mongoErr mongo.BulkWriteException
		if errors.As(err, &mongoErr) {
			var hasNoDupErr bool

			for _, e := range mongoErr.WriteErrors {
				if e.Code != 11000 && e.Code != 11001 && e.Code != 12582 {
					hasNoDupErr = true
					break
				}
			}

			if hasNoDupErr {
				return
			}

			err = nil
		}
	}

	return
}

func (o *Orm) UpdateOne(ctx context.Context, filter, update any) (res *mongo.UpdateResult, err error) {
	if o.onQueryDone != nil {
		start := time.Now()

		defer func() {
			o.OnQueryDone(o, "UpdateOne", res, err, time.Since(start), map[string]interface{}{
				"update": update,
			})
		}()
	}

	if err = o.CreatIndexes(ctx); err != nil {
		return
	}

	client, err := GetClient(o.conn)
	if err != nil {
		return
	}

	res, err = client.Database(o.db).Collection(o.tb).UpdateOne(ctx, filter, update)

	return
}

func (o *Orm) UpdateMany(ctx context.Context, filter, update any) (res *mongo.UpdateResult, err error) {
	if o.onQueryDone != nil {
		start := time.Now()

		defer func() {
			o.OnQueryDone(o, "UpdateMany", res, err, time.Since(start), map[string]interface{}{
				"filter": filter,
				"update": update,
			})
		}()
	}

	if err = o.CreatIndexes(ctx); err != nil {
		return
	}

	client, err := GetClient(o.conn)
	if err != nil {
		return
	}

	res, err = client.Database(o.db).Collection(o.tb).UpdateMany(ctx, filter, update)

	return
}

func (o *Orm) Upsert(ctx context.Context, filter, update any) (res *mongo.UpdateResult, err error) {
	if o.onQueryDone != nil {
		start := time.Now()

		defer func() {
			o.OnQueryDone(o, "Upsert", res, err, time.Since(start), map[string]interface{}{
				"filter": filter,
				"update": update,
			})
		}()
	}

	if err = o.CreatIndexes(ctx); err != nil {
		return
	}

	client, err := GetClient(o.conn)
	if err != nil {
		return
	}

	opts := options.Update().SetUpsert(true)

	res, err = client.Database(o.db).Collection(o.tb).UpdateOne(ctx, filter, update, opts)

	return
}

func (o *Orm) DeleteOne(ctx context.Context, filter any) (res *mongo.DeleteResult, err error) {
	if o.onQueryDone != nil {
		start := time.Now()

		defer func() {
			o.OnQueryDone(o, "DeleteOne", res, err, time.Since(start), bson.M{
				"filter": filter,
			})
		}()
	}

	if err = o.CreatIndexes(ctx); err != nil {
		return
	}

	client, err := GetClient(o.conn)
	if err != nil {
		return
	}

	res, err = client.Database(o.db).Collection(o.tb).DeleteOne(ctx, filter)

	return
}

func (o *Orm) DeleteMany(ctx context.Context, filter any) (res *mongo.DeleteResult, err error) {
	if o.onQueryDone != nil {
		start := time.Now()

		defer func() {
			o.OnQueryDone(o, "DeleteMany", res, err, time.Since(start), map[string]interface{}{
				"filter": filter,
			})
		}()
	}

	if err = o.CreatIndexes(ctx); err != nil {
		return
	}

	client, err := GetClient(o.conn)
	if err != nil {
		return
	}

	res, err = client.Database(o.db).Collection(o.tb).DeleteMany(ctx, filter)

	return
}

func (o *Orm) FindOne(ctx context.Context, filter any, resp any) (err error) {
	if o.onQueryDone != nil {
		start := time.Now()

		defer func() {
			o.OnQueryDone(o, "FindOne", resp, err, time.Since(start), map[string]interface{}{
				"filter": filter,
			})
		}()
	}

	client, err := GetClient(o.conn)
	if err != nil {
		return
	}

	err = client.Database(o.db).Collection(o.tb).FindOne(ctx, filter).Decode(resp)

	return
}

func (o *Orm) FindOneByCacheKey(ctx context.Context, filter bson.M, cacheKey string, resp any) (err error) {
	if o.onQueryDone != nil {
		start := time.Now()

		defer func() {
			o.OnQueryDone(o, "FindOneByCacheKey", resp, err, time.Since(start), map[string]interface{}{
				"filter":   filter,
				"cacheKey": cacheKey,
				"resp":     resp,
			})
		}()
	}

	client, err := GetClient(o.conn)
	if err != nil {
		return
	}

	if !o.cached {
		err = client.Database(o.db).Collection(o.tb).FindOne(ctx, filter).Decode(resp)
		return
	}

	// 读缓存
	key := fmt.Sprintf("cache.%s.%s.%s", o.db, o.tb, cacheKey)
	if ok, _ := o.cache.Get(key, resp); ok {
		return nil
	}

	// 读库
	err = client.Database(o.db).Collection(o.tb).FindOne(ctx, filter).Decode(resp)
	if err != nil {
		return
	}

	_ = o.cache.Set(key, resp)

	return
}

func (o *Orm) FindOneByID(ctx context.Context, id string, resp any) (err error) {
	if o.onQueryDone != nil {
		start := time.Now()

		defer func() {
			o.OnQueryDone(o, "FindOneByID", resp, err, time.Since(start), map[string]interface{}{
				"id":   id,
				"resp": resp,
			})
		}()
	}

	client, err := GetClient(o.conn)
	if err != nil {
		return
	}

	objId, err := primitive.ObjectIDFromHex(id)
	if err != nil {
		return
	}

	return client.Database(o.db).Collection(o.tb).FindOne(ctx, bson.M{"_id": objId}).Decode(resp)
}

func (o *Orm) FindAll(ctx context.Context, filter any, selector ...any) (curs *mongo.Cursor, err error) {
	if o.onQueryDone != nil {
		start := time.Now()

		defer func() {
			o.OnQueryDone(o, "FindAll", curs, err, time.Since(start), bson.M{
				"filter":   filter,
				"selector": selector,
			})
		}()
	}

	client, err := GetClient(o.conn)
	if err != nil {
		return
	}

	opts := options.Find()
	if len(selector) > 0 {
		opts.SetProjection(selector[0])
	}
	if filter == nil {
		filter = bson.M{}
	}

	curs, err = client.Database(o.db).Collection(o.tb).Find(ctx, filter, opts)

	return
}

func (o *Orm) FindAllBySort(ctx context.Context, filter any, sort bson.D, selector ...any) (curs *mongo.Cursor, err error) {
	if o.onQueryDone != nil {
		start := time.Now()

		defer func() {
			o.OnQueryDone(o, "FindAllBySort", curs, err, time.Since(start), map[string]interface{}{
				"filter":   filter,
				"sort":     sort,
				"selector": selector,
			})
		}()
	}

	client, err := GetClient(o.conn)
	if err != nil {
		return
	}

	opts := options.Find()
	if sort != nil {
		opts.SetSort(sort)
	}

	if len(selector) > 0 {
		opts.SetProjection(selector[0])
	}

	if filter == nil {
		filter = bson.M{}
	}

	curs, err = client.Database(o.db).Collection(o.tb).Find(ctx, filter, opts)

	return
}

func (o *Orm) FindMany(ctx context.Context, filter any, sort bson.D, limit int64, selector ...any) (curs *mongo.Cursor, err error) {
	if o.onQueryDone != nil {
		start := time.Now()

		defer func() {
			o.OnQueryDone(o, "FindMany", curs, err, time.Since(start), map[string]interface{}{
				"filter":   filter,
				"sort":     sort,
				"limit":    limit,
				"selector": selector,
			})
		}()
	}

	client, err := GetClient(o.conn)
	if err != nil {
		return
	}

	opts := options.Find()
	if sort != nil {
		opts.SetSort(sort)
	}
	opts.SetLimit(limit)
	if len(selector) > 0 {
		opts.SetProjection(selector[0])
	}
	if filter == nil {
		filter = bson.M{}
	}

	curs, err = client.Database(o.db).Collection(o.tb).Find(ctx, filter, opts)

	return
}

func (o *Orm) FindManyByPage(ctx context.Context, filter any, sort bson.D, offset, limit int64, selector ...any) (curs *mongo.Cursor, err error) {
	if o.onQueryDone != nil {
		start := time.Now()

		defer func() {
			o.OnQueryDone(o, "FindManyByPage", curs, err, time.Since(start), map[string]interface{}{
				"filter":   filter,
				"sort":     sort,
				"offset":   offset,
				"limit":    limit,
				"selector": selector,
			})
		}()
	}

	client, err := GetClient(o.conn)
	if err != nil {
		return
	}

	opts := options.Find()
	if sort != nil {
		opts.SetSort(sort)
	}
	opts.SetSkip(offset)
	opts.SetLimit(limit)
	if len(selector) > 0 {
		opts.SetProjection(selector[0])
	}
	if filter == nil {
		filter = bson.M{}
	}

	curs, err = client.Database(o.db).Collection(o.tb).Find(ctx, filter, opts)

	return
}

func (o *Orm) FindCount(ctx context.Context, filter any) (count int64, err error) {
	if o.onQueryDone != nil {
		start := time.Now()

		defer func() {
			o.OnQueryDone(o, "FindCount", count, err, time.Since(start), bson.M{
				"filter": filter,
			})
		}()
	}

	client, err := GetClient(o.conn)
	if err != nil {
		return
	}

	count, err = client.Database(o.db).Collection(o.tb).CountDocuments(ctx, filter)

	return
}

func (o *Orm) Aggregate(ctx context.Context, pipe []bson.D, opts ...*options.AggregateOptions) (res []bson.M, err error) {
	if o.onQueryDone != nil {
		start := time.Now()

		defer func() {
			o.OnQueryDone(o, "Aggregate", res, err, time.Since(start), map[string]interface{}{
				"pipe": pipe,
				"opts": opts,
			})
		}()
	}

	client, err := GetClient(o.conn)
	if err != nil {
		return
	}

	curs, _err := client.Database(o.db).Collection(o.tb).Aggregate(ctx, pipe, opts...)
	if _err != nil {
		err = _err
		return nil, err
	}

	defer curs.Close(ctx)

	res = []bson.M{}
	err = curs.All(ctx, &res)

	return
}

func (o *Orm) Sum(ctx context.Context, match any, group any) (bson.M, error) {
	if match == nil {
		match = bson.M{}
	}
	if group == nil {
		group = bson.M{}
	}
	pipeline := mongo.Pipeline{
		{
			{Key: "$match", Value: match},
		},
		{
			{Key: "$group", Value: group},
		},
	}
	res, err := o.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, err
	}
	if len(res) == 0 {
		return bson.M{}, nil
	}
	return res[0], nil
}

func (o *Orm) Txn(ctx context.Context, fn func(mongo.SessionContext) error) (err error) {
	if o.onQueryDone != nil {
		start := time.Now()

		defer func() {
			o.OnQueryDone(o, "Txn", nil, err, time.Since(start), map[string]interface{}{})
		}()
	}

	if err = o.CreatIndexes(ctx); err != nil {
		return
	}

	client, err := GetClient(o.conn)
	if err != nil {
		return
	}

	err = client.UseSession(ctx, func(sc mongo.SessionContext) error {
		err := sc.StartTransaction()
		if err != nil {
			return err
		}
		err = fn(sc)
		if err != nil {
			err = sc.AbortTransaction(context.TODO())
			return err
		}
		return sc.CommitTransaction(context.TODO())
	})

	return
}

func (o *Orm) DelCache(keys []string) (err error) {
	if !o.cached {
		return nil
	}

	for _, k := range keys {
		ks := fmt.Sprintf("cache.%s.%s.%s", o.db, o.tb, k)
		e := o.cache.Del(ks)
		if e != nil {
			err = e
		}
	}

	return
}

func (o *Orm) ClearIndexes(ctx context.Context, retainKeys []string) (err error) {
	if o.onQueryDone != nil {
		start := time.Now()

		defer func() {
			o.OnQueryDone(o, "ClearIndexes", nil, err, time.Since(start), map[string]interface{}{
				"retainKeys": retainKeys,
			})
		}()
	}

	client, err := GetClient(o.conn)
	if err != nil {
		return err
	}

	cursor, err := client.Database(o.db).Collection(o.tb).Indexes().List(ctx)
	if err != nil {
		return err
	}

	keySet := make(map[string]struct{}, len(retainKeys))
	for _, k := range retainKeys {
		keySet[k] = struct{}{}
	}

	for cursor.Next(ctx) {
		var index *bson.D
		errs := cursor.Decode(&index)
		if errs != nil || index == nil {
			continue
		}
		var keyName string
		for _, v := range *index {
			if v.Key == "name" {
				keyName, _ = v.Value.(string)
				break
			}
		}
		if _, ok := keySet[keyName]; ok {
			continue
		}
		if keyName == "_id_" {
			continue
		}
		_, err = client.Database(o.db).Collection(o.tb).Indexes().DropOne(ctx, keyName, options.DropIndexes())
		if err != nil {
			return err
		}
	}

	return cursor.Close(ctx)
}

var (
	createTbIdxRecordSet   = make(map[string]struct{})
	createTbIdxRecordSetMu sync.RWMutex
)

const (
	IndexNamePrefix       = "mgorm_"
	UniqIndexNamePrefix   = "mgorm_uni_"
	ExpireIndexNamePrefix = "mgorm_exp_"
)

func (o *Orm) CreatIndexes(ctx context.Context) (err error) {
	if o.onQueryDone != nil {
		start := time.Now()

		defer func() {
			o.OnQueryDone(o, "CreatIndexes", nil, err, time.Since(start), map[string]interface{}{})
		}()
	}

	if len(o.indexKeys) == 0 && len(o.uniqueIndexKeys) == 0 && len(o.expireIndexKeys) == 0 {
		return nil
	}

	checkCreatedIndexKey := fmt.Sprintf("%s.%s", o.db, o.tb)
	createTbIdxRecordSetMu.RLock()
	_, ok := createTbIdxRecordSet[checkCreatedIndexKey]
	createTbIdxRecordSetMu.RUnlock()
	if ok {
		return nil
	}

	createTbIdxRecordSetMu.Lock()
	defer createTbIdxRecordSetMu.Unlock()

	if _, ok := createTbIdxRecordSet[checkCreatedIndexKey]; ok {
		return nil
	}

	var models []mongo.IndexModel
	var allKeyNames []string
	for _, in := range o.indexKeys {
		var (
			model          = mongo.IndexModel{}
			bsonD          = bson.D{}
			keys           = in
			keyName        string
			hasSepcKeyName bool
		)
		comp := strings.Split(in, ":")
		if len(comp) >= 2 {
			keyName = comp[1]
			if keyName != "" {
				hasSepcKeyName = true
			}
			keys = comp[0]
		}
		if !hasSepcKeyName {
			keyName = IndexNamePrefix
		}
		for _, k := range strings.Split(keys, ",") {
			value := 1
			if strings.HasPrefix(k, "-") {
				k = k[1:]
				value = -1
			}
			bsonD = append(bsonD, bson.E{Key: k, Value: value})
			if hasSepcKeyName {
				continue
			}
			keyName += "_" + k
		}
		model.Keys = bsonD
		model.Options = options.Index().SetName(keyName)
		models = append(models, model)
		allKeyNames = append(allKeyNames, keyName)
	}

	for _, in := range o.expireIndexKeys {
		var (
			model          = mongo.IndexModel{}
			bsonD          = bson.D{}
			keys           = in
			keyName        string
			hasSepcKeyName bool
		)
		comp := strings.Split(in, ":")
		if len(comp) >= 2 {
			keyName = comp[1]
			if keyName != "" {
				hasSepcKeyName = true
			}
			keys = comp[0]
		}
		if !hasSepcKeyName {
			keyName = ExpireIndexNamePrefix
		}
		for _, k := range strings.Split(keys, ",") {
			value := 1
			if strings.HasPrefix(k, "-") {
				k = k[1:]
				value = -1
			}
			bsonD = append(bsonD, bson.E{Key: k, Value: value})
			if hasSepcKeyName {
				continue
			}
			keyName += "_" + k
		}
		model.Keys = bsonD
		model.Options = options.Index().SetName(keyName)
		model.Options.SetExpireAfterSeconds(0)
		models = append(models, model)
		allKeyNames = append(allKeyNames, keyName)
	}

	for _, in := range o.uniqueIndexKeys {
		var (
			model          = mongo.IndexModel{}
			bsonD          = bson.D{}
			keys           = in
			keyName        string
			hasSepcKeyName bool
		)
		comp := strings.Split(in, ":")
		if len(comp) >= 2 {
			keyName = comp[1]
			if keyName != "" {
				hasSepcKeyName = true
			}
			keys = comp[0]
		}
		if !hasSepcKeyName {
			keyName = UniqIndexNamePrefix
		}
		for _, k := range strings.Split(keys, ",") {
			value := 1
			if strings.HasPrefix(k, "-") {
				k = k[1:]
				value = -1
			}
			bsonD = append(bsonD, bson.E{Key: k, Value: value})
			if hasSepcKeyName {
				continue
			}
			keyName += "_" + k
		}
		model.Keys = bsonD
		model.Options = options.Index().SetUnique(true).SetName(keyName)
		models = append(models, model)
		allKeyNames = append(allKeyNames, keyName)
	}

	//清理老索引
	if err = o.ClearIndexes(ctx, allKeyNames); err != nil {
		return err
	}

	if len(allKeyNames) == 0 {
		return nil
	}

	client, err := GetClient(o.conn)
	if err != nil {
		return err
	}

	_, err = client.Database(o.db).Collection(o.tb).Indexes().CreateMany(ctx, models)
	if err != nil {
		return err
	}

	createTbIdxRecordSet[checkCreatedIndexKey] = struct{}{}

	return nil
}

func (o *Orm) DeleteIndex(ctx context.Context, indexName string) (err error) {
	if o.onQueryDone != nil {
		start := time.Now()

		defer func() {
			o.OnQueryDone(o, "DeleteIndex", nil, err, time.Since(start), map[string]interface{}{
				"indexName": indexName,
			})
		}()
	}

	client, err := GetClient(o.conn)
	if err != nil {
		return err
	}
	_, err = client.Database(o.db).Collection(o.tb).Indexes().DropOne(ctx, indexName)
	return err
}

func (o *Orm) CreatIndexByKeys(ctx context.Context, indexKeys, uniqIndexKeys, expireIndexKeys []string) (err error) {
	if o.onQueryDone != nil {
		start := time.Now()

		defer func() {
			o.OnQueryDone(o, "CreatIndexByKeys", nil, err, time.Since(start), map[string]interface{}{
				"indexKeys":       indexKeys,
				"uniqIndexKeys":   uniqIndexKeys,
				"expireIndexKeys": expireIndexKeys,
			})
		}()
	}

	if len(indexKeys) == 0 && len(uniqIndexKeys) == 0 && len(expireIndexKeys) == 0 {
		return nil
	}

	var models []mongo.IndexModel

	for _, in := range o.indexKeys {
		var (
			model          = mongo.IndexModel{}
			bsonD          = bson.D{}
			keys           = in
			keyName        string
			hasSepcKeyName bool
		)
		comp := strings.Split(in, ":")
		if len(comp) >= 2 {
			keyName = comp[1]
			if keyName != "" {
				hasSepcKeyName = true
			}
			keys = comp[0]
		}
		if !hasSepcKeyName {
			keyName = IndexNamePrefix
		}
		for _, k := range strings.Split(keys, ",") {
			value := 1
			if strings.HasPrefix(k, "-") {
				k = k[1:]
				value = -1
			}
			bsonD = append(bsonD, bson.E{Key: k, Value: value})
			if hasSepcKeyName {
				continue
			}
			keyName += "_" + k
		}
		model.Keys = bsonD
		model.Options = options.Index().SetName(keyName)
		models = append(models, model)
	}

	for _, in := range o.expireIndexKeys {
		var (
			model          = mongo.IndexModel{}
			bsonD          = bson.D{}
			keys           = in
			keyName        string
			hasSepcKeyName bool
		)
		comp := strings.Split(in, ":")
		if len(comp) >= 2 {
			keyName = comp[1]
			if keyName != "" {
				hasSepcKeyName = true
			}
			keys = comp[0]
		}
		if !hasSepcKeyName {
			keyName = ExpireIndexNamePrefix
		}
		for _, k := range strings.Split(keys, ",") {
			value := 1
			if strings.HasPrefix(k, "-") {
				k = k[1:]
				value = -1
			}
			bsonD = append(bsonD, bson.E{Key: k, Value: value})
			if hasSepcKeyName {
				continue
			}
			keyName += "_" + k
		}
		model.Keys = bsonD
		model.Options = options.Index().SetName(keyName)
		model.Options.SetExpireAfterSeconds(0)
		models = append(models, model)
	}

	for _, in := range o.uniqueIndexKeys {
		var (
			model          = mongo.IndexModel{}
			bsonD          = bson.D{}
			keys           = in
			keyName        string
			hasSepcKeyName bool
		)
		comp := strings.Split(in, ":")
		if len(comp) >= 2 {
			keyName = comp[1]
			if keyName != "" {
				hasSepcKeyName = true
			}
			keys = comp[0]
		}
		if !hasSepcKeyName {
			keyName = UniqIndexNamePrefix
		}
		for _, k := range strings.Split(keys, ",") {
			value := 1
			if strings.HasPrefix(k, "-") {
				k = k[1:]
				value = -1
			}
			bsonD = append(bsonD, bson.E{Key: k, Value: value})
			if hasSepcKeyName {
				continue
			}
			keyName += "_" + k
		}
		model.Keys = bsonD
		model.Options = options.Index().SetUnique(true).SetName(keyName)
		models = append(models, model)
	}

	client, err := GetClient(o.conn)
	if err != nil {
		return err
	}

	_, err = client.Database(o.db).Collection(o.tb).Indexes().CreateMany(ctx, models)
	if err != nil {
		return err
	}

	return nil
}
