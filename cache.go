package memorystorecache

import (
	"errors"
	"sync"
	"time"
)

const numBuckets = 36

// Cache contains memory store options
// a TTL of 0 does not expire keys
type Cache struct {
	TTL        time.Duration
	gcInterval time.Duration
}

// Conn is a connection to a memory store db
type Conn struct {
	TTL    time.Duration
	Dat    [numBuckets]map[string]cacheElement
	mu     [numBuckets]sync.RWMutex
	ticker *time.Ticker
}

type cacheElement struct {
	expiresAt time.Time
	dat       []byte
}

// Stats displays stats about the memory store
type Stats map[string]interface{}

// NewCache creates a new Cache
func NewCache(defaultTimeout, gcInterval time.Duration) (*Cache, error) {
	return &Cache{TTL: defaultTimeout, gcInterval: gcInterval}, nil
}

// Open opens a new connection to the memory store
func (c Cache) Open(name string) (*Conn, error) {
	var m Conn
	for i := 0; i < numBuckets; i++ {
		d := map[string]cacheElement{}
		m.Dat[i] = d
	}

	// start the sweep ticker
	m.ticker = time.NewTicker(c.gcInterval)
	go func(cn *Conn) {
		for range m.ticker.C {
			cn.sweep()
		}
	}(&m)

	m.TTL = c.TTL
	return &m, nil
}

// Close noop (there is no connection to close)
func (c *Conn) Close() error {
	c.ticker.Stop()
	c.deallocate()
	return nil
}

func (c *Conn) deallocate() {
	for i := range c.Dat {
		for k := range c.Dat[i] {
			delete(c.Dat[i], k)
		}
	}
}

// Write writes data to the cache with the default cache TTL
func (c *Conn) Write(k, v []byte) error {
	return c.WriteTTL(k, v, c.TTL)
}

// WriteTTL writes data to the cache with an explicit TTL
// a TTL of 0 does not expire keys
func (c *Conn) WriteTTL(k, v []byte, ttl time.Duration) error {
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
	c.mu[idx].Unlock()

	return nil
}

// Read retrieves data for a key from the cache
func (c *Conn) Read(k []byte) ([]byte, error) {
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
		c.mu[idx].Unlock()
	}
	return []byte{}, errors.New("Key not found")
}

func (c *Conn) keyCount() uint64 {
	var x uint64
	for i := range c.Dat {
		x += uint64(len(c.Dat[i]))
	}
	return x
}

// Stats provides stats about the Badger database
func (c *Conn) Stats() (map[string]interface{}, error) {
	return Stats{
		"KeyCount": c.keyCount(),
	}, nil
}

func (c *Conn) sweep() {
	for i := 0; i < numBuckets; i++ {
		go c.sweepBucket(i)
	}
}

func (c *Conn) sweepBucket(idx int) {
	t := time.Now().UTC()

	c.mu[idx].RLock()
	// pre-allocate the memory
	// we will store at most the total number of keys
	keys := make([]string, 0, len(c.Dat[idx]))
	for k, v := range c.Dat[idx] {
		if t.After(v.expiresAt) {
			keys = append(keys, k)
		}
	}
	c.mu[idx].RUnlock()

	for _, key := range keys {
		c.mu[idx].Lock()
		delete(c.Dat[idx], key)
		c.mu[idx].Unlock()
	}
}
