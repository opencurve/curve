package main

import (
	"sync"
	"sync/atomic"
)

type managedObject struct {
	nextObjectID uint64
	objects      sync.Map
}

func newManagedObject() *managedObject {
	return &managedObject{nextObjectID: 1}
}

var bindingObjects = newManagedObject()

// AddManagedObject adds the specified Go object to the managedObject
// collection so they can be access from foreign functions.
func AddManagedObject(object interface{}) uint64 {
	oid := atomic.AddUint64(&bindingObjects.nextObjectID, 1)
	if oid == 0 {
		// takes a loooooong time to overflow
		oid = 1
	}
	bindingObjects.objects.Store(oid, object)
	return oid
}

// GetManagedObject returns the Go object specified by the oid value.
func GetManagedObject(oid uint64) (interface{}, bool) {
	return bindingObjects.objects.Load(oid)
}

// RemoveManagedObject returns the object specified by the oid value from the
// managedObject collection.
func RemoveManagedObject(oid uint64) {
	bindingObjects.objects.Delete(oid)
}

// GetManagedObjectCount returns the number of object in the managed objects
// collection.
func GetManagedObjectCount() uint64 {
	count := uint64(0)
	bindingObjects.objects.Range(func(k, v interface{}) bool {
		count++
		return true
	})
	return count
}