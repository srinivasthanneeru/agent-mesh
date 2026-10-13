// Command race is a tiny exercise for Go's race detector.
//
//	go run -race ./examples/race     reports a DATA RACE, and the count is often wrong
//
// Fix it by guarding `count` with a sync.Mutex (or using sync/atomic), then
// run it again: no report, and the count is always 1000.
package main

import (
	"fmt"
	"sync"
)

func main() {
	count := 0
	var wg sync.WaitGroup
	for range 1000 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			count++ // read, add, write: two goroutines can interleave here
		}()
	}
	wg.Wait()
	fmt.Println("count =", count)
}
