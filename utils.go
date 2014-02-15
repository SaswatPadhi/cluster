package cluster

import (
	"bytes"
	"fmt"
	"log"
	"os"
)

type DBG_TYPE struct {
	priority int8
	name     string
}

var (
	DBG_INFO = DBG_TYPE{0, "[INFO]"}
	DBG_WARN = DBG_TYPE{1, "[WARN]"}
	DBG_EROR = DBG_TYPE{2, "[EROR]"}
	DBG_NONE = DBG_TYPE{3, "[NONE]"}

	DBG_FLAG = DBG_EROR
)

func (DBG_LEVL DBG_TYPE) Println(a ...interface{}) {
	if DBG_LEVL.priority >= DBG_FLAG.priority {
		log.Println(DBG_LEVL.name, a)
	}
}

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
