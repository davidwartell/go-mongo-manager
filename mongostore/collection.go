package mongostore

import (
	"context"
	"github.com/davidwartell/go-logger-facade/logger"
	"go.mongodb.org/mongo-driver/mongo"
)

func (a *DataStore) getClient(t clientType) *dsClient {
	client, found := a.clients[t]
	if !found {
		logger.Instance().Panic("data store client not found", logger.Stringer("clientType", t))
	}
	return client
}

// Collection calls CollectionLinearWriteRead()
func (a *DataStore) Collection(ctx context.Context, name string) (*mongo.Collection, error) {
	return a.CollectionLinearWriteRead(ctx, name)
}

// CollectionReadSnapshotSecondaryPreferred creates a connection with:
// - readconcern.Snapshot()
// - readpref.Secondary()
// - writeconcern.J(true)
// - writeconcern.WMajority()
//
// A query with read concern "snapshot" returns majority-committed data as it appears across shards from a specific
// single point in time in the recent past.
//
// Outside of multi-document transactions, read concern "snapshot" is available on primaries and secondaries for the
// following read operations:
// * find
// * aggregate
// * distinct (on unsharded collections)
// All other read commands prohibit "snapshot".
//
// Note: readpref.Primary() is critical for reads to consistently return results in the same go routine immediately
// after an insert.  And perhaps not well documented.
func (a *DataStore) CollectionReadSnapshotSecondaryPreferred(ctx context.Context, name string) (*mongo.Collection, error) {
	return a.getClient(clientTypeReadSnapshotReadSecondaryPreferred).collection(ctx, name)
}

// CollectionReadSnapshot creates a connection with:
// - readconcern.Snapshot()
// - readpref.Primary()
// - writeconcern.J(true)
// - writeconcern.WMajority()
//
// A query with read concern "snapshot" returns majority-committed data as it appears across shards from a specific
// single point in time in the recent past.
//
// Outside of multi-document transactions, read concern "snapshot" is available on primaries and secondaries for the
// following read operations:
// * find
// * aggregate
// * distinct (on unsharded collections)
// All other read commands prohibit "snapshot".
//
// Note: readpref.Primary() is critical for reads to consistently return results in the same go routine immediately
// after an insert.  And perhaps not well documented.
func (a *DataStore) CollectionReadSnapshot(ctx context.Context, name string) (*mongo.Collection, error) {
	return a.getClient(clientTypeReadSnapshot).collection(ctx, name)
}

// CollectionLinearWriteRead creates a connection with:
// - readconcern.Linearizable()
// - readpref.Primary()
// - writeconcern.J(true)
// - writeconcern.WMajority()
//
// This connection supplies: "Casual Consistency" in a sharded cluster _inside a single client thread_.
// https://www.mongodb.com/docs/manual/core/read-isolation-consistency-recency/#std-label-sessions
//
// Reads may miss matching documents that are updated during the course of the read operation by other clients.
// Queries that use unique indexes can, in some cases, return duplicate values if indexes are modified by other clients.
// MongoDB cursors can return the same document more than once in some situations if indexes are modified by other
// clients.
//
// Note: readpref.Primary() is critical for reads to consistently return results in the same go routine immediately
// after an insert.  And perhaps not well documented.
func (a *DataStore) CollectionLinearWriteRead(ctx context.Context, name string) (*mongo.Collection, error) {
	return a.getClient(clientTypeLinearWriteRead).collection(ctx, name)
}

// CollectionUnsafeFastWrites creates a connection with:
// - readconcern.Local()
// - readpref.Primary()
// - writeconcern.J(false)
// - writeconcern.W(1)
func (a *DataStore) CollectionUnsafeFastWrites(ctx context.Context, name string) (*mongo.Collection, error) {
	return a.getClient(clientTypeUnsafeFastWrites).collection(ctx, name)
}

// CollectionReadNearest creates a connection with:
//
// Reads may miss matching documents that are updated during the course of the read operation by other clients.
// Queries that use unique indexes can, in some cases, return duplicate values if indexes are modified by other clients.
// MongoDB cursors can return the same document more than once in some situations if indexes are modified by other
// clients.
//
// Reads may miss matching documents that are updated during the course of the read operation by other clients.
// Queries that use unique indexes can, in some cases, return duplicate values if indexes are modified by other clients.
// MongoDB cursors can return the same document more than once in some situations if indexes are modified by other
// clients.
//
// - readconcern.Majority()
// - readpref.Nearest()
// - writeconcern.J(true)
// - writeconcern.WMajority()
func (a *DataStore) CollectionReadNearest(ctx context.Context, name string) (*mongo.Collection, error) {
	return a.getClient(clientTypeReadNearest).collection(ctx, name)
}

// CollectionReadSecondaryPreferred creates a connection with:
//
// Reads may miss matching documents that are updated during the course of the read operation by other clients.
// Queries that use unique indexes can, in some cases, return duplicate values if indexes are modified by other clients.
// MongoDB cursors can return the same document more than once in some situations if indexes are modified by other
// clients.
//
// - readconcern.Majority()
// - readpref.SecondaryPreferred()
// - writeconcern.J(true)
// - writeconcern.WMajority()
func (a *DataStore) CollectionReadSecondaryPreferred(ctx context.Context, name string) (*mongo.Collection, error) {
	return a.getClient(clientTypeReadSecondaryPreferred).collection(ctx, name)
}

// CollectionForWatch creates a connection with:
// - readconcern.Majority()
// - readpref.SecondaryPreferred()
// - writeconcern.J(true)
// - writeconcern.WMajority()
//
// This is recommended for use with Change Streams (Watch()).  The write concerns are just in case you use it for writes by accident.
func (a *DataStore) CollectionForWatch(ctx context.Context, name string) (*mongo.Collection, error) {
	return a.getClient(clientTypeForWatch).collection(ctx, name)
}
