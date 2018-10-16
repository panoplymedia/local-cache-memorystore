# Local Cache MemoryStore

A local cache based on a custom memory store implementation

### Sample Usage

```go
defaultTimeout := time.Minute
memoryStoreCache, err := NewCache(defaultTimeout)
if err != nil {
  fmt.Println(err)
}

// open a connection to badger database
conn, err := memoryStoreCache.Open("") // pass in empty string since the URI is arbitrary
defer conn.Close()

// write data to cache (uses defaultTimeout)
err = conn.Write([]byte("key"), []byte("data"))

// write data to cache with custom timeout
err = conn.WriteTTL([]byte("key2"), []byte("data"), 5*time.Minute)

// read data
data, err := conn.Read([]byte("key"))

// log stats
fmt.Println(conn.Stats())
```