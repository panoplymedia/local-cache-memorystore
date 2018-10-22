package memorystorecache

import (
	"errors"
	"math"
	"sync"
	"time"

	"github.com/panoplymedia/local-cache-memorystore/priorityqueue"
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
	TTL      time.Duration
	dat      [numBuckets]map[string]cacheElement
	mu       [numBuckets]sync.RWMutex
	KeyCount int64
	// garbage collection bookkeeping
	gcQueue   priorityqueue.PriorityQueue
	gcQueueMu sync.RWMutex
	gcItems   map[string]*priorityqueue.Item
	gcItemsMu sync.RWMutex
	runGC     bool
}

type cacheElement struct {
	expiresAt int64
	dat       []byte
}

// Stats displays stats about the memory store
type Stats map[string]interface{}

// NewCache creates a new Cache
func NewCache(defaultTimeout time.Duration, gcInterval time.Duration) (*Cache, error) {
	return &Cache{TTL: defaultTimeout, gcInterval: gcInterval}, nil
}

// Open opens a new connection to the memory store
func (c Cache) Open(name string) (*Conn, error) {
	var m Conn
	m.gcItems = make(map[string]*priorityqueue.Item)
	for i := 0; i < numBuckets; i++ {
		d := map[string]cacheElement{}
		m.dat[i] = d
	}

	m.TTL = c.TTL
	m.runGC = true
	go gcLoop(&m, c.gcInterval)
	return &m, nil
}

// Close and stop gc loop
func (c *Conn) Close() error {
	c.runGC = false
	return nil
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
	var e int64

	if ttl == 0 {
		// for a 0 TTL, store the max value of a time
		e = math.MaxInt64
	} else {
		e = time.Now().UTC().Add(ttl).UnixNano()
	}

	ce := cacheElement{
		expiresAt: e,
		dat:       v,
	}

	c.mu[idx].Lock()
	c.dat[idx][key] = ce
	c.mu[idx].Unlock()

	// only add keys that expire to gc bookkeeping
	if ttl > 0 {
		go updateGCBookkeeping(c, key, e)
	}

	return nil
}

// Read retrieves data for a key from the cache
func (c *Conn) Read(k []byte) ([]byte, error) {
	key := string(k)
	idx := keyToShard(key)

	c.mu[idx].RLock()
	el, exists := c.dat[idx][key]
	c.mu[idx].RUnlock()
	if exists && time.Now().UTC().UnixNano() < el.expiresAt {
		return el.dat, nil
	} else if exists {
		// evict key since it exists and it's expired
		c.mu[idx].Lock()
		delete(c.dat[idx], key)
		c.mu[idx].Unlock()
	}
	return []byte{}, errors.New("Key not found")
}

// Stats provides stats about the Badger database
func (c *Conn) Stats() (map[string]interface{}, error) {
	keyCount := 0
	for i := 0; i < numBuckets; i++ {
		keyCount += len(c.dat[i])
	}
	return Stats{
		"KeyCount": keyCount,
	}, nil
}

func gcLoop(c *Conn, gcInterval time.Duration) {
	for range time.Tick(gcInterval) {
		if !c.runGC {
			break
		}
		c.gcItemsMu.RLock()
		i := c.gcQueue.Peek()
		item, ok := i.(*priorityqueue.Item)
		c.gcItemsMu.RUnlock()
		// pop items while items are timed out
		currentTime := time.Now().UTC().UnixNano()
		for i != nil && ok && item.Priority <= currentTime {
			if !c.runGC {
				break
			}
			c.gcQueueMu.Lock()
			popped := c.gcQueue.Pop()
			c.gcQueueMu.Unlock()
			poppedItem, poppedOk := popped.(*priorityqueue.Item)
			if poppedOk {
				// delete gc bookkeeping data
				c.gcItemsMu.Lock()
				delete(c.gcItems, poppedItem.Value)
				c.gcItemsMu.Unlock()

				// delete timed out data
				idx := keyToShard(poppedItem.Value)
				c.mu[idx].Lock()
				delete(c.dat[idx], poppedItem.Value)
				c.mu[idx].Unlock()
			}

			// peek at next item
			c.gcItemsMu.RLock()
			i = c.gcQueue.Peek()
			item, ok = i.(*priorityqueue.Item)
			c.gcItemsMu.RUnlock()
		}
	}
}

func updateGCBookkeeping(c *Conn, key string, expiresAt int64) {
	c.gcItemsMu.RLock()
	item, alreadyInQueue := c.gcItems[key]
	c.gcItemsMu.RUnlock()

	if alreadyInQueue {
		// update existing item
		c.gcQueueMu.Lock()
		c.gcQueue.Update(item, key, expiresAt)
		c.gcQueueMu.Unlock()
	} else {
		// new key/item
		item := &priorityqueue.Item{
			Value:    key,
			Priority: expiresAt,
		}
		// add item to queue
		c.gcQueueMu.Lock()
		c.gcQueue.Push(item)
		c.gcQueueMu.Unlock()
		// track pointer to item for future updates
		c.gcItemsMu.Lock()
		c.gcItems[key] = item
		c.gcItemsMu.Unlock()
	}
}
