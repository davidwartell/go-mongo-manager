package mongostore

import (
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
	"time"
)

const (
	clientTypeLinearWriteRead clientType = iota + 1
	clientTypeUnsafeFastWrites
	clientTypeReadNearest
	clientTypeReadSecondaryPreferred
	clientTypeForWatch
	clientTypeReadSnapshot
	clientTypeReadSnapshotReadSecondaryPreferred
)

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// clientTypeReadSnapshotReadSecondaryPreferred
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type readSnapshotReadSecondaryPreferredClientConfig struct{}

func (c readSnapshotReadSecondaryPreferredClientConfig) clientType() clientType {
	return clientTypeReadSnapshotReadSecondaryPreferred
}

func (c readSnapshotReadSecondaryPreferredClientConfig) clientOptions(ds *DataStore) *options.ClientOptions {
	cOpts := ds.standardOptions()
	cOpts.SetReadConcern(readconcern.Snapshot())
	cOpts.SetReadPreference(readpref.SecondaryPreferred())
	cOpts.SetWriteConcern(writeConcernMajorityJournaled(ds.queryTimeout()))
	return cOpts
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// clientTypeReadSnapshot
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type readSnapshotClientConfig struct{}

func (c readSnapshotClientConfig) clientType() clientType {
	return clientTypeReadSnapshot
}

func (c readSnapshotClientConfig) clientOptions(ds *DataStore) *options.ClientOptions {
	cOpts := ds.standardOptions()
	cOpts.SetReadConcern(readconcern.Snapshot())
	cOpts.SetReadPreference(readpref.Primary())
	cOpts.SetWriteConcern(writeConcernMajorityJournaled(ds.queryTimeout()))
	return cOpts
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// clientTypeLinearWriteRead
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type linearWriteReadClientConfig struct{}

func (c linearWriteReadClientConfig) clientType() clientType {
	return clientTypeLinearWriteRead
}

func (c linearWriteReadClientConfig) clientOptions(ds *DataStore) *options.ClientOptions {
	cOpts := ds.standardOptions()
	cOpts.SetReadConcern(readconcern.Linearizable())
	// connect primary for reads or linear reads in same go routine will some times fail to find documents you just
	// inserted in same routine
	cOpts.SetReadPreference(readpref.Primary())
	cOpts.SetWriteConcern(writeConcernMajorityJournaled(ds.queryTimeout()))
	return cOpts
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// clientTypeUnsafeFastWrites
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type unsafeFastWritesConfig struct{}

func (c unsafeFastWritesConfig) clientType() clientType {
	return clientTypeUnsafeFastWrites
}

func (c unsafeFastWritesConfig) clientOptions(ds *DataStore) *options.ClientOptions {
	cOpts := ds.standardOptions()
	cOpts.SetReadPreference(readpref.Primary()) // read from primary for linear reads
	isFalse := false
	cOpts.SetWriteConcern(
		&writeconcern.WriteConcern{
			Journal:  &isFalse,
			W:        1,
			WTimeout: ds.queryTimeout(),
		},
	)
	cOpts.SetReadConcern(readconcern.Local())
	return cOpts
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// clientTypeReadNearest
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type readNearestConfig struct{}

func (c readNearestConfig) clientType() clientType {
	return clientTypeReadNearest
}

func (c readNearestConfig) clientOptions(ds *DataStore) *options.ClientOptions {
	cOpts := ds.standardOptions()
	cOpts.SetReadPreference(readpref.Nearest())
	cOpts.SetWriteConcern(writeConcernMajorityJournaled(ds.queryTimeout()))
	cOpts.SetReadConcern(readconcern.Majority())
	return cOpts
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// clientTypeReadSecondaryPreferred
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type readSecondaryPreferred struct{}

func (c readSecondaryPreferred) clientType() clientType {
	return clientTypeReadSecondaryPreferred
}

func (c readSecondaryPreferred) clientOptions(ds *DataStore) *options.ClientOptions {
	cOpts := ds.standardOptions()
	cOpts.SetReadPreference(readpref.SecondaryPreferred())
	cOpts.SetWriteConcern(writeConcernMajorityJournaled(ds.queryTimeout()))
	cOpts.SetReadConcern(readconcern.Majority())
	return cOpts
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// clientTypeForWatch
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type forWatchConfig struct{}

func (c forWatchConfig) clientType() clientType {
	return clientTypeForWatch
}

func (c forWatchConfig) clientOptions(ds *DataStore) *options.ClientOptions {
	cOpts := ds.standardOptions()
	cOpts.SetReadConcern(readconcern.Majority())
	cOpts.SetReadPreference(readpref.SecondaryPreferred())
	cOpts.SetWriteConcern(writeConcernMajorityJournaled(ds.queryTimeout()))
	return cOpts
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// util
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func writeConcernMajorityJournaled(wTimeout time.Duration) *writeconcern.WriteConcern {
	isTrue := true
	return &writeconcern.WriteConcern{
		Journal:  &isTrue,
		W:        "majority",
		WTimeout: wTimeout,
	}
}
