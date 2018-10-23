package memorystorecache

import (
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
)

func TestNewCache(t *testing.T) {
	c, err := NewCache(time.Second, time.Second)
	assert.Nil(t, err)

	assert.Equal(t, time.Second, c.TTL)
}

func TestWrite(t *testing.T) {
	c, err := NewCache(time.Second, time.Second)
	assert.Nil(t, err)
	conn, err := c.Open("")
	assert.Nil(t, err)
	defer conn.Close()

	key := []byte("set")

	// cache miss
	b := []byte{1, 2, 3}
	err = conn.Write(key, b)
	assert.Nil(t, err)

	// cache hit
	b2, err := conn.Read(key)
	assert.Nil(t, err)
	assert.Equal(t, b, b2)

	// default ttl timeout (cache miss)
	time.Sleep(time.Second)
	_, err = conn.Read(key)
	assert.Errorf(t, err, "Key not found")
}

func TestWriteTTL(t *testing.T) {
	c, err := NewCache(time.Second, time.Second)
	assert.Nil(t, err)
	conn, err := c.Open("")
	assert.Nil(t, err)
	defer conn.Close()

	key := []byte("set")

	// cache miss
	b := []byte{1, 2, 3}
	err = conn.WriteTTL(key, b, time.Second)
	assert.Nil(t, err)

	// cache hit
	b2, err := conn.Read(key)
	assert.Nil(t, err)
	assert.Equal(t, b, b2)

	// default ttl timeout (cache miss)
	time.Sleep(time.Second)
	_, err = conn.Read(key)
	assert.Errorf(t, err, "Key not found")
}

func TestRead(t *testing.T) {
	c, err := NewCache(time.Second, time.Second)
	assert.Nil(t, err)
	conn, err := c.Open("")
	assert.Nil(t, err)
	defer conn.Close()

	// creates initial key
	key := []byte("my-key")
	// cache miss
	b, err := conn.Read(key)
	assert.Errorf(t, err, "Key not found")

	// cache hit
	v := []byte{1, 2}
	err = conn.Write(key, v)
	assert.Nil(t, err)
	b, err = conn.Read(key)
	assert.Nil(t, err)
	assert.Equal(t, v, b)
}

func TestStats(t *testing.T) {
	c, err := NewCache(time.Second, time.Second)
	assert.Nil(t, err)
	conn, err := c.Open("")
	assert.Nil(t, err)
	defer conn.Close()

	// write a key
	key := []byte("my-key")
	v := []byte{1, 2}
	err = conn.Write(key, v)
	assert.Nil(t, err)

	s, err := conn.Stats()
	assert.Nil(t, err)
	assert.Equal(t, map[string]interface{}{"KeyCount": uint64(1)}, s)
}

func writeData(c *Conn, numKeys int) {
	wg := sync.WaitGroup{}
	numWorkers := runtime.NumCPU()
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(c *Conn, keys int) {
			defer wg.Done()
			for i := 0; i < keys; i++ {
				c.Write(uuid.NewV4().Bytes(), uuid.NewV4().Bytes())
			}
		}(c, numKeys/numWorkers)
	}

	wg.Wait()
}

func BenchmarkWriteWorkers(b *testing.B) {
	c, _ := NewCache(time.Second, time.Second)
	conn, _ := c.Open("")
	defer conn.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		writeData(conn, 1000000)
	}
}

func readData(c *Conn, numKeys int) {
	wg := sync.WaitGroup{}
	numWorkers := runtime.NumCPU()
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(c *Conn, keys int) {
			defer wg.Done()
			for i := 0; i < keys; i++ {
				c.Read([]byte(uuid.NewV4().String()))
			}
		}(c, numKeys/numWorkers)
	}

	wg.Wait()
}

func BenchmarkGetWorkers(b *testing.B) {
	c, _ := NewCache(time.Second, time.Second)
	conn, _ := c.Open("")
	defer conn.Close()

	writeData(conn, 1000000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		readData(conn, 1000000)
	}
}

func BenchmarkWrite(b *testing.B) {
	c, _ := NewCache(time.Second, time.Second)
	conn, _ := c.Open("")
	defer conn.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		conn.Write([]byte(uuid.NewV4().String()), uuid.NewV4().Bytes())
	}
}

func BenchmarkRead(b *testing.B) {
	c, _ := NewCache(time.Second, time.Second)
	conn, _ := c.Open("")
	defer conn.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		conn.Read(uuid.NewV4().Bytes())
	}
}
