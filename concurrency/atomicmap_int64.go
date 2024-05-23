/*
Copyright 2024 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package concurrency

import (
	"sync"
	"sync/atomic"
)

type AtomicInt64 struct {
	ptr *int64
}

func (a *AtomicInt64) Load() int64 {
	return atomic.LoadInt64(a.ptr)
}

func (a AtomicInt64) Store(v int64) {
	atomic.StoreInt64(a.ptr, v)
}

func (a *AtomicInt64) Add(v int64) int64 {
	return atomic.AddInt64(a.ptr, v)
}

type AtomicInt64Map[K comparable] interface {
	Get(key K) (*AtomicInt64, bool)
	GetOrCreate(key K, v int64) *AtomicInt64
	Delete(key K)
	ForEach(fn func(key K, value *AtomicInt64))
	Clear()
}

type atomicInt64Map[K comparable] struct {
	lock  sync.RWMutex
	items map[K]*AtomicInt64
}

func NewAtomicInt64Map[K comparable]() AtomicInt64Map[K] {
	return &atomicInt64Map[K]{
		items: make(map[K]*AtomicInt64),
	}
}

func (a *atomicInt64Map[K]) Get(key K) (*AtomicInt64, bool) {
	a.lock.RLock()
	defer a.lock.RUnlock()

	item, ok := a.items[key]
	if !ok {
		return nil, false
	}
	return item, true
}

func (a *atomicInt64Map[K]) GetOrCreate(key K, createT int64) *AtomicInt64 {
	a.lock.RLock()
	item, ok := a.items[key]
	a.lock.RUnlock()
	if !ok {
		a.lock.Lock()
		// Double-check the key exists to avoid race condition
		item, ok = a.items[key]
		if !ok {
			newV := createT
			item = &AtomicInt64{ptr: &newV}
			a.items[key] = item
		}
		a.lock.Unlock()
	}
	return item
}

func (a *atomicInt64Map[K]) Delete(key K) {
	a.lock.Lock()
	delete(a.items, key)
	a.lock.Unlock()
}

func (a *atomicInt64Map[K]) ForEach(fn func(key K, value *AtomicInt64)) {
	a.lock.RLock()
	defer a.lock.RUnlock()
	for k, v := range a.items {
		fn(k, v)
	}
}

func (a *atomicInt64Map[K]) Clear() {
	a.lock.Lock()
	defer a.lock.Unlock()
	clear(a.items)
}
