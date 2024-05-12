package mongostore

import (
	"context"
	"go.mongodb.org/mongo-driver/mongo"
)

func (a *DataStore) StartSessionReadSnapshotReadSecondaryPreferred(ctx context.Context) (mongo.Session, error) {
	return a.getClient(clientTypeReadSnapshotReadSecondaryPreferred).session(ctx)
}

func (a *DataStore) StartSessionReadSnapshot(ctx context.Context) (mongo.Session, error) {
	return a.getClient(clientTypeReadSnapshot).session(ctx)
}

func (a *DataStore) StartSessionLinearWriteRead(ctx context.Context) (mongo.Session, error) {
	return a.getClient(clientTypeLinearWriteRead).session(ctx)
}

func (a *DataStore) StartSessionLinearUnsafeFastWrites(ctx context.Context) (mongo.Session, error) {
	return a.getClient(clientTypeUnsafeFastWrites).session(ctx)
}

func (a *DataStore) StartSessionReadNearest(ctx context.Context) (mongo.Session, error) {
	return a.getClient(clientTypeReadNearest).session(ctx)
}

func (a *DataStore) StartSessionReadSecondaryPreferred(ctx context.Context) (mongo.Session, error) {
	return a.getClient(clientTypeReadSecondaryPreferred).session(ctx)
}

func (a *DataStore) StartSessionForWatch(ctx context.Context) (mongo.Session, error) {
	return a.getClient(clientTypeForWatch).session(ctx)
}
