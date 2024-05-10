package mongostore

//goland:noinspection SpellCheckingInspection
import (
	"context"
	"github.com/davidwartell/go-logger-facade/task"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/event"
	mongooptions "go.mongodb.org/mongo-driver/mongo/options"
	"sync"
)

var ErrorServiceNotStarted = errors.New("getting mongo client failed: service is not started or shutdown")

const (
	DefaultDatabaseName                         = "datastore"
	DefaultConnectTimeoutSeconds                = uint64(10)
	DefaultTimeoutSecondsShutdown               = uint64(10)
	DefaultTimeoutSecondsQuery                  = uint64(10)
	DefaultPingHeartbeatSeconds                 = uint64(10)
	DefaultMaxFailedEnsureIndexesBackoffSeconds = uint64(300)
	DefaultUsername                             = ""
	DefaultPassword                             = ""
	DefaultAuthMechanism                        = "PLAIN"
	DefaultMaxPoolSize                          = uint64(100)
	DefaultHost                                 = "localhost:27017"
	taskName                                    = "Mongo DataStore"
)

type Options struct {
	databaseName                         string
	connectTimeoutSeconds                uint64
	timeoutSecondsShutdown               uint64
	timeoutSecondsQuery                  uint64
	pingHeartbeatSeconds                 uint64
	maxFailedEnsureIndexesBackoffSeconds uint64
	hosts                                []string
	uri                                  string
	username                             string
	password                             string
	authMechanism                        string // Supported values include "SCRAM-SHA-256", "SCRAM-SHA-1", "MONGODB-CR", "PLAIN", "GSSAPI", "MONGODB-X509", and "MONGODB-AWS".
	maxPoolSize                          uint64
	monitor                              *event.CommandMonitor
}

type DataStore struct {
	rwMutex            sync.RWMutex
	started            bool
	options            *Options
	clients            map[clientType]*dsClient
	ctx                context.Context
	cancel             context.CancelFunc
	wg                 sync.WaitGroup
	managedIndexes     []indexGroup
	allIndexesByPath   map[string]Index // [managedIndexId(idx.CollectionName, idx.Id)] -> Index
	managedIndexesLock sync.RWMutex
}

// New returns a new instance of the data store. You should only create one instance in your program and re-use it.
// You must call Stop() when your program exits.
//
// The instance returned is multithreading safe.
//
//goland:noinspection GoUnusedExportedFunction
func New(opts ...DataStoreOption) *DataStore {
	return newInstance(nil, opts...)
}

//goland:noinspection GoUnusedExportedFunction
func NewWithManagedIndexes(managedIndexes []Index, opts ...DataStoreOption) *DataStore {
	return newInstance(managedIndexes, opts...)
}

func newInstance(managedIndexes []Index, opts ...DataStoreOption) *DataStore {
	store := &DataStore{
		options: &Options{
			databaseName:                         DefaultDatabaseName,
			connectTimeoutSeconds:                DefaultConnectTimeoutSeconds,
			timeoutSecondsShutdown:               DefaultTimeoutSecondsShutdown,
			timeoutSecondsQuery:                  DefaultTimeoutSecondsQuery,
			pingHeartbeatSeconds:                 DefaultPingHeartbeatSeconds,
			maxFailedEnsureIndexesBackoffSeconds: DefaultMaxFailedEnsureIndexesBackoffSeconds,
			hosts:                                []string{DefaultHost},
			uri:                                  "",
			username:                             DefaultUsername,
			password:                             DefaultPassword,
			authMechanism:                        DefaultAuthMechanism,
			maxPoolSize:                          DefaultMaxPoolSize,
		},

		allIndexesByPath: make(map[string]Index),
	}

	store.clients = map[clientType]*dsClient{
		clientTypeReadSnapshotReadSecondaryPreferred: {
			ds:           store,
			clientConfig: readSnapshotReadSecondaryPreferredClientConfig{},
		},
		clientTypeReadSnapshot: {
			ds:           store,
			clientConfig: readSnapshotClientConfig{},
		},
		clientTypeLinearWriteRead: {
			ds:           store,
			clientConfig: linearWriteReadClientConfig{},
		},
		clientTypeUnsafeFastWrites: {
			ds:           store,
			clientConfig: unsafeFastWritesConfig{},
		},
		clientTypeReadNearest: {
			ds:           store,
			clientConfig: readNearestConfig{},
		},
		clientTypeReadSecondaryPreferred: {
			ds:           store,
			clientConfig: readSecondaryPreferred{},
		},
		clientTypeForWatch: {
			ds:           store,
			clientConfig: forWatchConfig{},
		},
	}

	store.start(managedIndexes, opts...)
	return store
}

func (a *DataStore) start(managedIndexes []Index, opts ...DataStoreOption) {
	a.rwMutex.Lock()
	defer a.rwMutex.Unlock()
	task.LogInfo(taskName, "starting")
	a.ctx, a.cancel = context.WithCancel(context.Background())

	for _, opt := range opts {
		opt(a.options)
	}

	a.wg.Add(1)
	go a.runPing(a.ctx, &a.wg)

	if len(managedIndexes) > 0 {
		a.addManagedIndexes(startupIndexGroupName, managedIndexes)
		a.wg.Add(1)
		go a.runEnsureIndexes(a.ctx, &a.wg, startupIndexGroupName)
	}

	a.started = true
	task.LogInfo(taskName, "started")
}

// Stop disconnects the mongo clients and stops the background routines.  Call this once on exit of your main.go.
func (a *DataStore) Stop() {
	a.rwMutex.Lock()
	if !a.started {
		a.rwMutex.Unlock()
		return
	}
	a.rwMutex.Unlock()

	task.LogInfo(taskName, "stopping")

	a.rwMutex.Lock()
	if a.cancel != nil {
		a.cancel()
	}
	a.rwMutex.Unlock()

	// don't hold the lock while waiting - cause a deadlock
	a.wg.Wait()

	a.rwMutex.Lock()
	defer a.rwMutex.Unlock()
	if !a.started {
		return
	}

	// disconnect from mongo
	var disconnectWg sync.WaitGroup
	for _, client := range a.clients {
		disconnectWg.Add(1)
		go client.disconnect(&disconnectWg, a.options.timeoutSecondsShutdown)
	}
	disconnectWg.Wait()

	a.started = false
	task.LogInfo(taskName, "stopped")
}

func (a *DataStore) Started() bool {
	a.rwMutex.RLock()
	defer a.rwMutex.RUnlock()
	return a.started
}

func (a *DataStore) getOptions() Options {
	return *a.options
}

// standardOptions sets up standard options consistent across all clients
func (a *DataStore) standardOptions() (clientOptions *mongooptions.ClientOptions) {
	options := a.getOptions()
	if len(options.uri) > 0 {
		clientOptions = mongooptions.Client().ApplyURI(options.uri)
	} else {
		clientOptions = mongooptions.Client().SetHosts(options.hosts)
	}
	if options.username != "" || options.authMechanism != "" {
		var credentials mongooptions.Credential
		if options.authMechanism != "" {
			credentials.AuthMechanism = options.authMechanism
		}
		if options.username != "" {
			credentials.Username = options.username
			credentials.Password = options.password
		}
		clientOptions.SetAuth(credentials)
	}
	clientOptions.SetRetryWrites(true)
	clientOptions.SetRetryReads(true)
	clientOptions.SetMaxPoolSize(options.maxPoolSize)
	clientOptions.SetMinPoolSize(1)
	clientOptions.SetCompressors([]string{"snappy"})
	clientOptions.SetMonitor(options.monitor)
	return
}
