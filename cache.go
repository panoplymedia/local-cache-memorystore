package memorystorecache

import (
	"errors"
	"sync"
	"time"
)

// a TTL of 0 does not expire keys
type MemoryStoreCache struct {
	TTL time.Duration
}

// MemoryStoreConn is a connection to a memory store db
type MemoryStoreConn struct {
	TTL      time.Duration
	Dat      [26]map[string]cacheElement
	mu       [26]sync.RWMutex
	KeyCount uint64
}

type cacheElement struct {
	expiresAt time.Time
	dat       []byte
}

// MemoryStoreStats displays stats about the memory store
type MemoryStoreStats map[string]interface{}

// NewCache creates a new MemoryStoreCache
func NewCache(defaultTimeout time.Duration) (*MemoryStoreCache, error) {
	return &MemoryStoreCache{TTL: defaultTimeout}, nil
}

// Open opens a new connection to the memory store
func (c MemoryStoreCache) Open(name string) (*MemoryStoreConn, error) {
	var m MemoryStoreConn
	for i := 0; i < 26; i++ {
		d := map[string]cacheElement{}
		m.Dat[i] = d
	}

	m.TTL = c.TTL
	return &m, nil
}

func (c *MemoryStoreConn) Close() error {
	return nil
}

// Write writes data to the cache with the default cache TTL
func (c *MemoryStoreConn) Write(k, v []byte) error {
	return c.WriteTTL(k, v, c.TTL)
}

// WriteTTL writes data to the cache with an explicit TTL
// a TTL of 0 does not expire keys
func (c *MemoryStoreConn) WriteTTL(k, v []byte, ttl time.Duration) error {
	key := string(k)
	idx := keyToShard(key)
	var e time.Time

	if ttl == 0 {
		// for a 0 TTL, store the max value of a time struct, so it essentially never expires
		e = time.Unix(1<<63-1, 0)
	} else {
		e = time.Now().UTC().Add(ttl)
	}

	ce := cacheElement{
		expiresAt: e,
		dat:       v,
	}

	c.mu[idx].Lock()
	c.Dat[idx][key] = ce
	c.KeyCount++
	c.mu[idx].Unlock()

	return nil
}

// Read retrieves data for a key from the cache
func (c *MemoryStoreConn) Read(k []byte) ([]byte, error) {
	key := string(k)
	idx := keyToShard(key)

	c.mu[idx].RLock()
	el, exists := c.Dat[idx][key]
	c.mu[idx].RUnlock()
	if exists && time.Now().UTC().Before(el.expiresAt) {
		return el.dat, nil
	} else if exists {
		// evict key since it exists and it's expired
		c.mu[idx].Lock()
		delete(c.Dat[idx], key)
		c.KeyCount--
		c.mu[idx].Unlock()
	}
	return []byte{}, errors.New("Key not found")
}

// Stats provides stats about the Badger database
func (c *MemoryStoreConn) Stats() map[string]interface{} {
	return MemoryStoreStats{
		"KeyCount": c.KeyCount,
	}
}
