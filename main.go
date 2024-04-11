package main

import (
	"bytes"
	"fmt"
)

func main() {
	path := "libra.db"
	db, _ := Open(path, DefaultOptions)

	for i := 0; i < 1000; i++ {
		tx := db.WriteTx()
		name := []byte(fmt.Sprintf("test_collection_%d", i))

		collection, err := tx.CreateCollection(name)
		if err != nil {
			panic(fmt.Errorf("error: %s", err))
		}

		for j := 0; j < 200; j++ {
			key, value := []byte(fmt.Sprintf("key_%d", j)), []byte(fmt.Sprintf("value_%d", j))
			err = collection.Put(key, value)
			if err != nil {
				panic(fmt.Errorf("error: %s", err))
			}
		}

		_ = tx.Commit()

		tx = db.ReadTx()

		collection, err = tx.GetCollection(name)
		if err != nil {
			panic(fmt.Errorf("error: %v", err))
		}
		if collection == nil {
			panic(fmt.Errorf("no collection found: %s", name))
		}

		for j := 0; j < 200; j++ {
			key, value := []byte(fmt.Sprintf("key_%d", j)), []byte(fmt.Sprintf("value_%d", j))
			item, err := collection.Find(key)
			if err != nil {
				panic(fmt.Errorf("error: %v", err))
			}
			if item == nil {
				panic(fmt.Errorf("item not found for key: %s in collection: %s", key, collection.name))
			}
			cmp := bytes.Compare(item.value, value)
			if cmp != 0 {
				panic(fmt.Errorf("value should be %s, found %s", item.value, value))
			}
		}

		_ = tx.Commit()
	}
}
