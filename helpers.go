package memorystorecache

import (
	"strings"
)

func keyToShard(key string) int {
	i := int(strings.ToLower(key)[0])
	// handle numeric chars
	if i >= 48 && i <= 57 {
		return i - 48
	}

	// handle alpha chars
	if i >= 97 && i <= 122 {
		return i - 97 + 10 // add 10 for number of numeric keys
	}

	// if we're not in the numeric or alpha char range, we'll stick it in the z bucket
	return numBuckets - 1
}
