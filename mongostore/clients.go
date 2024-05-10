package mongostore

import (
	"context"
	"go.mongodb.org/mongo-driver/mongo"
)

func (a *DataStore) ClientReadSnapshotReadSecondaryPreferred(ctx context.Context) (*mongo.Client, error) {
	return a.getClient(clientTypeReadSnapshotReadSecondaryPreferred).mongoClient(ctx)
}

func (a *DataStore) ClientReadSnapshot(ctx context.Context) (*mongo.Client, error) {
	return a.getClient(clientTypeReadSnapshot).mongoClient(ctx)
}

func (a *DataStore) ClientLinearWriteRead(ctx context.Context) (*mongo.Client, error) {
	return a.getClient(clientTypeLinearWriteRead).mongoClient(ctx)
}

func (a *DataStore) ClientLinearUnsafeFastWrites(ctx context.Context) (*mongo.Client, error) {
	return a.getClient(clientTypeUnsafeFastWrites).mongoClient(ctx)
}

func (a *DataStore) ClientReadNearest(ctx context.Context) (*mongo.Client, error) {
	return a.getClient(clientTypeReadNearest).mongoClient(ctx)
}

func (a *DataStore) ClientReadSecondaryPreferred(ctx context.Context) (*mongo.Client, error) {
	return a.getClient(clientTypeReadSecondaryPreferred).mongoClient(ctx)
}

func (a *DataStore) ClientForWatch(ctx context.Context) (*mongo.Client, error) {
	return a.getClient(clientTypeForWatch).mongoClient(ctx)
}
