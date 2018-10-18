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
		{"aasdf", 0},
		{"basdf", 1},
		{"zasfd", 25},
		{"1asdf", 25},
		{"0basfd", 25},
	}

	for _, item := range data {
		assert.Equal(t, item.shard, keyToShard(item.key))
	}
}
