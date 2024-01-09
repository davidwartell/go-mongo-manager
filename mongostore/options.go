package mongostore

import (
	"go.mongodb.org/mongo-driver/event"
	"go.mongodb.org/mongo-driver/mongo"
)

type DataStoreOption func(o *Options)

//goland:noinspection GoUnusedExportedFunction
func WithDatabaseName(databaseName string) DataStoreOption {
	return func(o *Options) {
		o.databaseName = databaseName
	}
}

//goland:noinspection GoUnusedExportedFunction
func WithMonitor(monitor *event.CommandMonitor) DataStoreOption {
	return func(o *Options) {
		o.monitor = monitor
	}
}

//goland:noinspection GoUnusedExportedFunction
func WithTimeoutSecondsShutdown(timeoutSecondsShutdown uint64) DataStoreOption {
	return func(o *Options) {
		o.timeoutSecondsShutdown = timeoutSecondsShutdown
	}
}

//goland:noinspection GoUnusedExportedFunction
func WithTimeoutSecondsQuery(timeoutSecondsQuery uint64) DataStoreOption {
	return func(o *Options) {
		o.timeoutSecondsQuery = timeoutSecondsQuery
	}
}

//goland:noinspection GoUnusedExportedFunction
func WithPingHeartbeatSeconds(pingHeartbeatSeconds uint64) DataStoreOption {
	return func(o *Options) {
		o.pingHeartbeatSeconds = pingHeartbeatSeconds
	}
}

//goland:noinspection GoUnusedExportedFunction
func WithMaxFailedEnsureIndexesBackoffSeconds(maxFailedEnsureIndexesBackoffSeconds uint64) DataStoreOption {
	return func(o *Options) {
		o.maxFailedEnsureIndexesBackoffSeconds = maxFailedEnsureIndexesBackoffSeconds
	}
}

//goland:noinspection GoUnusedExportedFunction
func WithHosts(hosts []string) DataStoreOption {
	return func(o *Options) {
		o.hosts = hosts
	}
}

//goland:noinspection GoUnusedExportedFunction
func WithUri(uri string) DataStoreOption {
	return func(o *Options) {
		o.uri = uri
	}
}

//goland:noinspection GoUnusedExportedFunction
func WithUsername(username string) DataStoreOption {
	return func(o *Options) {
		o.username = username
	}
}

//goland:noinspection GoUnusedExportedFunction
func WithPassword(password string) DataStoreOption {
	return func(o *Options) {
		o.password = password
	}
}

//goland:noinspection GoUnusedExportedFunction
func WithAuthMechanism(authMechanism string) DataStoreOption {
	return func(o *Options) {
		o.authMechanism = authMechanism
	}
}

//goland:noinspection GoUnusedExportedFunction
func WithMaxPoolSize(maxPoolSize uint64) DataStoreOption {
	return func(o *Options) {
		o.maxPoolSize = maxPoolSize
	}
}

//goland:noinspection GoUnusedExportedFunction
func WithConnectTimeoutSeconds(connectTimeoutSeconds uint64) DataStoreOption {
	return func(o *Options) {
		o.connectTimeoutSeconds = connectTimeoutSeconds
	}
}

func IsIndexNotFoundError(err error) bool {
	if err == nil {
		return false
	} else if commandErr, ok := err.(mongo.CommandError); ok {
		return commandErr.Code == 27 // Mongo Error Code 27 IndexNotFound
	} else {
		return false
	}
}
