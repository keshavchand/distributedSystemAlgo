package main

import (
	"fmt"
	"sync"
)

type Storage map[string]string

var storage Storage
var storageLock sync.RWMutex

func Put(key, val string) {
	storageLock.Lock()
	defer storageLock.Unlock()

	if storage == nil {
		storage = make(Storage)
	}
	storage[key] = val
}

func Get(key string) (string, error) {
	storageLock.Lock()
	defer storageLock.Unlock()

	if storage == nil {
		return "", fmt.Errorf("Empty Map")
	}
	val, present := storage[key]
	if !present {
		return "", fmt.Errorf("Value Not Present")
	}
	return val, nil
}
