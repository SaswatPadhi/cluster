package cluster

import (
	"bytes"
	"fmt"
	"log"
	"os"
)

type Set struct {
	m map[interface{}]interface{}
}

func (s *Set) Clear() {
	s.m = make(map[interface{}]interface{})
}

func (s *Set) Has(k interface{}) bool {
	_, found := s.m[k]
	return found
}

func (s *Set) Insert(k interface{}) {
	if s.m == nil {
		s.Clear()
	}
	s.m[k] = nil
}

func (s *Set) Size() int {
	return len(s.m)
}

type LOG_TYPE struct {
	priority int8
	name     string
}

var (
	INFO = LOG_TYPE{0, "[INFO]"}
	WARN = LOG_TYPE{1, "[WARN]"}
	EROR = LOG_TYPE{2, "[EROR]"}
	NONE = LOG_TYPE{3, "[NONE]"}

	LOG_FLAG = EROR
)

func (LOG_LEVL LOG_TYPE) Println(a ...interface{}) {
	if LOG_LEVL.priority >= LOG_FLAG.priority {
		log.Println(LOG_LEVL.name, a)
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
