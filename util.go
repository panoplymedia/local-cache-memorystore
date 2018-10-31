package memorystorecache

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"sync"
)

// receive a gob-encoded byte stream of a shard
func (c *Conn) BackupShard(i int) ([]byte, error) {
	b := bytes.Buffer{}
	e := gob.NewEncoder(&b)
	c.mu[i].RLock()
	err := e.Encode(c.Dat[i])
	c.mu[i].RUnlock()
	if err != nil {
		return []byte{}, err
	}
	return b.Bytes(), nil
}

// decode a gob-encoded byte stream to a live shard
func (c *Conn) RestoreShard(i int, dat []byte) error {
	var temp map[string]cacheElement
	b := bytes.Buffer{}

	b.Write(dat)
	d := gob.NewDecoder(&b)
	err := d.Decode(&temp)
	if err != nil {
		return err
	}

	c.mu[i].Lock()
	c.Dat[i] = temp
	c.mu[i].Unlock()

	return nil
}

// convenience method to return a map of all shards, gob-encoded to a byte stream
func (c *Conn) BackupShards() *sync.Map {
	wg := &sync.WaitGroup{}
	wg.Add(len(c.Dat))

	var sm sync.Map

	for i, _ := range c.Dat {
		go c.backupToMap(i, &sm, wg)
	}

	wg.Wait()

	return &sm
}

func (c *Conn) backupToMap(i int, sm *sync.Map, wg *sync.WaitGroup) {
	defer wg.Done()

	// TODO: handle error (error channel or some other construct)
	dat, _ := c.BackupShard(i)

	sm.Store(shardKey(i), dat)
}

// convenience method to restore all shards
func (c *Conn) RestoreShards() *sync.Map {
	wg := &sync.WaitGroup{}
	wg.Add(len(c.Dat))

	var sm sync.Map

	for i, _ := range c.Dat {
		go c.restoreFromMap(i, &sm, wg)
	}

	wg.Wait()

	return &sm
}

func (c *Conn) restoreFromMap(i int, sm *sync.Map, wg *sync.WaitGroup) {
	defer wg.Done()

	dat, exists := sm.Load(shardKey(i))
	if exists {
		c.RestoreShard(i, dat.([]byte))
	}
}

func shardKey(i int) string {
	return fmt.Sprintf("shard-%d", i+1)
}
