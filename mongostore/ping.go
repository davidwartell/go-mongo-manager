package mongostore

import (
	"context"
	"github.com/davidwartell/go-logger-facade/logger"
	"github.com/davidwartell/go-logger-facade/task"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	mongooptions "go.mongodb.org/mongo-driver/mongo/options"
	"sync"
	"time"
)

func (a *DataStore) Ping(clientCtx context.Context) error {
	defer task.HandlePanic(taskName)
	var err error
	var client *mongo.Client

	queryCtx, cancel := a.ContextTimeout(clientCtx)
	defer cancel()

	client, err = a.clientUnsafeFastWrites(clientCtx)
	if err != nil {
		task.LogErrorStruct(taskName, "error getting client for ping", logger.Error(err))
		return err
	}

	err = client.Ping(queryCtx, nil)
	if err != nil {
		err2 := errors.Wrap(err, "Mongo ping failed")
		return err2
	}

	var collection *mongo.Collection
	collection, err = a.CollectionUnsafeFastWrites(clientCtx, "ping")
	if err != nil {
		task.LogErrorStruct(taskName, "error getting collection for ping write test", logger.Error(err))
		return err
	}

	filter := bson.D{{"_id", "testWrite"}}
	update := bson.D{
		{
			"$inc",
			bson.D{{"count", uint64(1)}},
		},
	}
	updateOptions := &mongooptions.UpdateOptions{}
	updateOptions = updateOptions.SetUpsert(true)
	_, err = collection.UpdateOne(queryCtx, filter, update, updateOptions)
	if err != nil {
		return err
	}

	return nil
}

func (a *DataStore) runPing(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	task.LogInfoStruct(taskName, "ping runner started")

	a.rwMutex.RLock()
	heartbeatSeconds := a.options.pingHeartbeatSeconds
	a.rwMutex.RUnlock()

	for {
		err := a.Ping(ctx)
		if err != nil {
			task.LogErrorStruct(taskName, "mongo ping failed", logger.Error(err))
		}
		select {
		case <-time.After(time.Second * time.Duration(heartbeatSeconds)):
		case <-ctx.Done():
			task.LogInfoStruct(taskName, "ping runner stopped")
			return
		}
	}
}
