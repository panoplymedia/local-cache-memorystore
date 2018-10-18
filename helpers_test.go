package memorystorecache

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestKeyToShard(t *testing.T) {
	data := []struct {
		key   string
		shard int
	}{
		{"aasdf", 10},
		{"basdf", 11},
		{"zasfd", 35},
		{"1asdf", 1},
		{"0basfd", 0},
		{"9basfd", 9},
		{"'basfd", 35},
	}

	for _, item := range data {
		assert.Equal(t, item.shard, keyToShard(item.key))
	}
}
