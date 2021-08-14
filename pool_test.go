package goroutine_pool

import (
	"fmt"
	"log"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func TestPool(t *testing.T) {
	Convey("TestPool", t, func() {
		Convey("should_all_done", func() {
			tc, err := NewPool(Config{
				InitialTaskWorkerCount: 1,
				MaxTaskWorkerCount:     10,
				KeepaliveTime:          5,
				HandleTaskPanic: func(err interface{}) {
					fmt.Printf("task panic, err:%v, stack:%s", err, debug.Stack())
				},
				MonitorTimeInterval: time.Millisecond * 200,
				MonitorCallback: func(m map[uint64]TaskInfo) bool {
					deadLock := len(m) != 0
					for _, v := range m {
						if (time.Now().UnixNano() - v.StartTimestamp.UnixNano()) <= int64(time.Millisecond*200) {
							deadLock = false
							break
						}
					}
					if deadLock {
						log.Printf("warn: may be deadlock")
					}
					return deadLock
				},
			})
			if err != nil {
				log.Fatalf("error in NewPool, err:%v", err)
			}
			const allTaskCount = 1000000
			doneCount := int32(0)
			// 测试task执行
			var wg sync.WaitGroup
			for i := 1; i <= allTaskCount; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					success, err := tc.AddTask(func() {
						defer func() {
							atomic.AddInt32(&doneCount, 1)
						}()
					}, fmt.Sprintf("%v", time.Now()), true)
					if !success || err != nil {
						log.Fatalf("fatal error in tc.AddTask, success:%v, err:%v", success, err)
					}
				}()
			}

			for i := 999; i >= 0; i-- {
				newKeepaliveTime := time.Millisecond * time.Duration(i)
				if oldKeepaliveTime, err := tc.ChangeKeepaliveTime(newKeepaliveTime); err != nil {
					log.Fatalf("error in tc.ChangeKeepaliveTime, oldKeepaliveTime:%d, newKeepaliveTime:%d, err:%v", oldKeepaliveTime, newKeepaliveTime, err)
				}
				// 测试修改workerCount
				if err := tc.ChangeTaskWorkerCount(uint32(i)); err != nil {
					log.Fatalf("error in tc.ChangeTaskWorkerCount, i:%d, err:%v", i, err)
				}
				status, err := tc.GetStatus()
				if status.RunningWorkerCount > status.MaxWorkerCount || err != nil || status.KeepaliveTime != newKeepaliveTime {
					log.Fatalf("fatal error: status:%#+v, err:%v, newKeepaliveTime:%d", status, err, newKeepaliveTime)
				}
			}

			for i := 0; i <= 999; i++ {
				newKeepaliveTime := time.Millisecond * time.Duration(i)
				if oldKeepaliveTime, err := tc.ChangeKeepaliveTime(newKeepaliveTime); err != nil {
					log.Fatalf("error in tc.ChangeKeepaliveTime, oldKeepaliveTime:%d, newKeepaliveTime:%d, err:%v", oldKeepaliveTime, newKeepaliveTime, err)
				}
				// 测试修改workerCount
				if err := tc.ChangeTaskWorkerCount(uint32(i)); err != nil {
					log.Fatalf("error in tc.ChangeTaskWorkerCount, i:%d, err:%v", i, err)
				}
				status, err := tc.GetStatus()
				if status.RunningWorkerCount > status.MaxWorkerCount || err != nil || status.KeepaliveTime != newKeepaliveTime {
					log.Fatalf("fatal error: status:%#+v, err:%v, newKeepaliveTime:%d", status, err, newKeepaliveTime)
				}
			}

			wg.Wait()
			// 测试stop
			if err := tc.Stop(); err != nil {
				log.Fatalf("error in tc.Stop, err:%v", err)
			}
			log.Printf("doneCount:%d, allTaskCount:%d", doneCount, allTaskCount)
			So(doneCount, ShouldEqual, allTaskCount)
		})
	})
}
