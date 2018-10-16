package memorystorecache

import (
	"strings"
)

func keyToShard(key string) int {
	i := int(strings.ToLower(key)[0])

	// if we're not in the char range (97-122), we'll stick it in the z bucket
	if i < 97 || i > 122 {
		i = 122
	}

	return i - 97
}
