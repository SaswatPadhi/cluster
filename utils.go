package cluster

import (
	"bytes"
	"fmt"
	"os"
)

// Generates a Version 4 (pseudo-random) UUID (Universally Unique Identifier).
func GenerateUUID() ([]byte, error) {
	dev_rand, err := os.Open("/dev/urandom")
	if err != nil {
		return nil, err
	}

	rand_bytes := make([]byte, 16)
	if _, err = dev_rand.Read(rand_bytes); err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	fmt.Fprintf(&buf, "%x-%x-4%x-b%x-%x", rand_bytes[0:4], rand_bytes[4:6],
		rand_bytes[6:8], rand_bytes[8:10], rand_bytes[10:])

	return buf.Bytes(), dev_rand.Close()
}
