# go-mongo-manager

Lib for managing connections, indexes, and heartbeat for MongoDB clients:
* automatically manage indexes defined in code at startup
* indexes that are found on the MongoDB cluster but not in ManagedIndexes are automatically deleted.
* indexes that do not exist on the MongoDB cluster but are found in ManagedIndexes are automatically created
* indexes found in both MongoDB cluster and ManagedIndexes are updated if they changed signature and version
* background task that keeps a periodic heartbeat to mongo and logs any failures.  Useful for troubleshooting intermittent database connectivity problems.
* error handling and retry of dirty writes

## Usage

Example
```go
var options []mongostore.DataStoreOption
options = append(options, mongostore.WithDatabaseName("demo"))
options = append(options, mongostore.WithHosts([]string{"host.docker.internal:27017", "host.docker.internal:27018", "host.docker.internal:27019"}))
options = append(options, mongostore.WithMaxPoolSize(uint64(100)))
options = append(options, mongostore.WithUsername("FIXME dont put passwords in code"))
options = append(options, mongostore.WithPassword("FIXME dont put passwords in code"))
mongostore.Instance().StartTask(managedIndexes, options...)
defer mongostore.Instance().StopTask()

// Collection returns a mongo collection setup for journaling and safe writes.  Alternatively use CollectionUnsafeFast() when data integrity is not important (eg logs).
var err error
var cancel context.CancelFunc
// put a time limit on your database request(s)
ctx, cancel = mongostore.Instance().ContextTimeout(ctx)
defer cancel()

var myCollection *mongo.Collection
myCollection, err = mongostore.Instance().Collection(ctx, "MyCollection")
if err != nil {
    return err
}

// no need return collections but you DO NEED to close mongo cursors if you use them
var cursor *mongo.Cursor
cursor, err = collection.Find(ctx, FIXME)
if err != nil {
    return err
}
defer func() {
if cursor != nil {
    _ = cursor.Close(ctx)
}
}()
```

## Contributing

Happy to accept PRs.

# Author

**davidwartell**

* <http://github.com/davidwartell>
* <http://linkedin.com/in/wartell>
