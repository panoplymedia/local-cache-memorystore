package memorystorecache

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io/ioutil"
	"sync"

	"cloud.google.com/go/storage"
	"golang.org/x/net/context"
)

func (c *Conn) recordCheckpoint() {
	fmt.Println("recording checkpoint . . .")
	defer fmt.Println("done")

	wg := &sync.WaitGroup{}
	wg.Add(len(c.Dat))

	for i, _ := range c.Dat {
		go c.recordShard(i, wg)
	}

	wg.Wait()
}

func (c *Conn) recordShard(i int, wg *sync.WaitGroup) error {
	defer wg.Done()

	c.mu[i].Lock()
	defer c.mu[i].Unlock()

	dat, err := marshall(c.Dat[i])
	if err != nil {
		fmt.Println(err)
		return err
	}
	err = writeBytes(dat, c.bucket, fmt.Sprintf("shard-%d", i+1))
	if err != nil {
		fmt.Println(err)
		return err
	}
	return nil
}

func (c *Conn) restoreCheckpoint() {
	fmt.Println("restoring checkpoint . . .")
	defer fmt.Println("done")

	wg := &sync.WaitGroup{}
	wg.Add(len(c.Dat))

	for i, _ := range c.Dat {
		go c.restoreShard(i, wg)
	}

	wg.Wait()
}

func (c *Conn) restoreShard(i int, wg *sync.WaitGroup) error {
	defer wg.Done()

	bytes, err := readBytes(c.bucket, fmt.Sprintf("shard-%d", i+1))
	if err != nil {
		fmt.Println(err)
		return err
	}
	dat, err := unmarshall(bytes)
	if err != nil {
		fmt.Println(err)
		return err
	}

	c.mu[i].Lock()
	c.Dat[i] = dat
	c.mu[i].Unlock()

	return nil
}

func writeBytes(dat []byte, bucket, fname string) error {
	ctx := context.Background()

	client, err := storage.NewClient(ctx)
	if err != nil {
		return err
	}
	defer client.Close()

	bkt := client.Bucket(bucket)
	obj := bkt.Object(fname)

	w := obj.NewWriter(ctx)
	defer w.Close()

	_, err = w.Write(dat)
	if err != nil {
		return err
	}

	return nil

}

func readBytes(bucket, fname string) ([]byte, error) {
	ctx := context.Background()
	empty := []byte{}

	client, err := storage.NewClient(ctx)
	if err != nil {
		return empty, err
	}
	defer client.Close()

	bkt := client.Bucket(bucket)
	obj := bkt.Object(fname)

	r, err := obj.NewReader(ctx)
	if err != nil {
		return empty, err
	}
	defer r.Close()
	bytes, err := ioutil.ReadAll(r)
	if err != nil {
		return empty, err
	}

	return bytes, nil
}

func marshall(dat map[string]cacheElement) ([]byte, error) {
	b := bytes.Buffer{}
	e := gob.NewEncoder(&b)
	err := e.Encode(dat)
	if err != nil {
		return []byte{}, err
	}
	return b.Bytes(), nil
}

func unmarshall(dat []byte) (map[string]cacheElement, error) {
	b := bytes.Buffer{}
	b.Write(dat)
	d := gob.NewDecoder(&b)
	var temp map[string]cacheElement
	err := d.Decode(&temp)
	return temp, err
}
