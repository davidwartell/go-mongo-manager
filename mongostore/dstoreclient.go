package mongostore

import (
	"context"
	"github.com/davidwartell/go-logger-facade/logger"
	"github.com/davidwartell/go-logger-facade/task"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"sync"
	"time"
)

type clientType int

type dsStoreClientConfig interface {
	clientOptions(*DataStore) *options.ClientOptions
	clientType() clientType
	sessionOptions(*DataStore) *options.SessionOptions
}

type dsClient struct {
	ds           *DataStore
	clientConfig dsStoreClientConfig
	rwMutex      sync.RWMutex
	mClient      *mongo.Client
}

func (c *dsClient) mongoClient(ctx context.Context) (client *mongo.Client, err error) {
	if !c.ds.Started() {
		err = ErrorServiceNotStarted
		return
	}

	c.rwMutex.RLock()
	client = c.mClient
	c.rwMutex.RUnlock()

	if c.mClient != nil {
		return
	}

	client, err = c.connect(ctx)
	return
}

func (c *dsClient) connect(clientCtx context.Context) (client *mongo.Client, err error) {
	c.rwMutex.Lock()
	defer c.rwMutex.Unlock()

	if c.mClient != nil {
		client = c.mClient
		return
	}

	ctx, cancel := context.WithTimeout(clientCtx, time.Duration(c.ds.getOptions().connectTimeoutSeconds)*time.Second)
	defer cancel()

	task.LogInfo(taskName, "connecting to mongo", logger.Stringer("clientType", c.clientConfig.clientType()))

	if c.mClient, err = mongo.Connect(ctx, c.clientConfig.clientOptions(c.ds)); err != nil {
		err = errors.Wrap(err, "error connecting to mongo")
		return
	}
	client = c.mClient

	task.LogInfo(taskName, "connected to mongo", logger.Stringer("clientType", c.clientConfig.clientType()))
	return
}

func (c *dsClient) disconnect(wg *sync.WaitGroup, timeoutSecondsShutdown uint64) {
	defer wg.Done()

	c.rwMutex.Lock()
	defer c.rwMutex.Unlock()

	if c.mClient == nil {
		return
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		time.Duration(timeoutSecondsShutdown)*time.Second,
	)
	defer cancel()
	if err := c.mClient.Disconnect(ctx); err != nil {
		task.LogError(
			taskName,
			"shutdown: error on disconnect of mongo client",
			logger.Error(err),
			logger.Stringer("clientType", c.clientConfig.clientType()),
		)
	}
	c.mClient = nil
}

func (c *dsClient) database(ctx context.Context) (db *mongo.Database, err error) {
	var client *mongo.Client
	if client, err = c.mongoClient(ctx); err != nil {
		task.LogError(
			taskName,
			"error getting mongo client",
			logger.Error(err),
			logger.Stringer("clientType", c.clientConfig.clientType()),
		)
		return
	}
	db = client.Database(c.ds.getOptions().databaseName)
	return
}

func (c *dsClient) session(ctx context.Context) (sess mongo.Session, err error) {
	var client *mongo.Client
	if client, err = c.mongoClient(ctx); err != nil {
		task.LogError(
			taskName,
			"error getting mongo client",
			logger.Error(err),
			logger.Stringer("clientType", c.clientConfig.clientType()),
		)
		return
	}
	return client.StartSession(c.clientConfig.sessionOptions(c.ds))
}

func (c *dsClient) collection(ctx context.Context, name string) (coll *mongo.Collection, err error) {
	var db *mongo.Database
	if db, err = c.database(ctx); err != nil {
		return
	}
	coll = db.Collection(name)
	return
}

func (t clientType) String() string {
	switch t {
	case clientTypeLinearWriteRead:
		return "LinearReadWrite"
	case clientTypeUnsafeFastWrites:
		return "UnsafeFastWrites"
	case clientTypeReadNearest:
		return "ReadNearest"
	case clientTypeReadSecondaryPreferred:
		return "ReadSecondaryPreferred"
	case clientTypeForWatch:
		return "ForWatch"
	default:
		return "Unknown"
	}
}
