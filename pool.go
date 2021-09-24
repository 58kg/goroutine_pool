package gpool

import (
	"fmt"
	"os"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

func NewDefaultPool(maxTaskWorkerCount int) *Pool {
	if maxTaskWorkerCount < MinTaskWorkerCount {
		maxTaskWorkerCount = MinTaskWorkerCount
	}

	if maxTaskWorkerCount > MaxTaskWorkerCount {
		maxTaskWorkerCount = MaxTaskWorkerCount
	}

	ret, err := NewPool(Config{
		MaxTaskWorkerCount: MaxTaskWorkerCount,
		KeepaliveTime:      time.Second * 10,
		HandleTaskPanic: func(err interface{}) {
			_, _ = fmt.Fprintf(os.Stderr, "task panic, err:%s, stack:\n%s", err, debug.Stack())
		},
	})
	if err != nil {
		panic(err)
	}

	return ret
}

func NewPool(c Config) (*Pool, error) {
	if err := checkConfig(c); err != nil {
		return nil, err
	}

	ret := &Pool{
		taskChan:                    make(chan *taskChanElem),
		stopOneTaskWorkerSignalChan: make(chan struct{}),
		oneTaskWorkerJustExitSignalChanForChangeTaskWorkerCount: make(chan struct{}),
		stopAllTaskWorkerSignalChan:                             make(chan struct{}),
		handleTaskPanic:                                         c.HandleTaskPanic,
		oneTaskWorkerExitSignalChanForAddTask:                   make(chan struct{}, MaxTaskWorkerCount),
	}
	ret.keepaliveTimeAndKeepaliveTimeChangeSignalChanPair.Store([2]interface{}{c.KeepaliveTime, make(chan struct{})})
	ret.maxWorkerCountChangeSignalChan.Store(make(chan struct{}))

	*ret.getStatusNumPtr() += uint64(c.MaxTaskWorkerCount) << maxCountLowBit
	*ret.getStatusNumPtr() += uint64(1) << initStatusBit
	for i := 1; i <= int(c.InitialTaskWorkerCount); i++ {
		atomic.AddUint64(ret.getStatusNumPtr(), uint64(1)<<runningCountLowBit)
		ret.execOneTaskWorker(nil)
	}

	if c.MonitorTimeInterval > 0 {
		ret.wg.Add(1)
		go func() {
			defer ret.wg.Done()
			ret.monitor(c.MonitorTimeInterval, c.MonitorCallback)
		}()
	}

	return ret, nil
}

type Config struct {
	// 初始TaskWorker的个数
	InitialTaskWorkerCount uint32

	// 并发运行的TaskWorker个数的最大值
	MaxTaskWorkerCount uint32

	// 每个TaskWorker的最大连续空闲时间长, 连续空闲时长超过此值的TaskWorker会结束退出
	KeepaliveTime time.Duration

	// 处理Task执行过程中未捕获的panic, 为nil时不会进行调用
	HandleTaskPanic func(err interface{})

	// GoroutinePool状态周期巡查的时间间隔, 为0时标识关闭Goroutine状态周期巡查
	MonitorTimeInterval time.Duration

	// 每轮GoroutinePool状态周期巡查完成后将调用MonitorCallback, 如果MonitorCallback返回true将启动Monitor Tasker
	// Monitor Tasker的作用是循环从taskChan获取TaskElem执行, 直到收到结束Monitor Tasker的信号后退出
	MonitorCallback func(map[uint64]TaskInfo) bool
}

// withBlock为true表示阻塞的将task放入p中执行, 为false表示非阻塞的放入, 非阻塞时如果p不存在空闲的TaskWorker, task将放入失败
func (p *Pool) AddTask(task func(), data interface{}, withBlock bool) (success bool, err error) {
	if !p.isInitSuccess() {
		return false, newGoroutinePoolNotInitError()
	}
	if task == nil {
		return false, newConfigIllegalError("param task is nil")
	}

	elem := &taskChanElem{
		data: data,
		task: task,
	}

	for {
		if p.stopped() {
			return false, newGoroutinePoolStoppedError()
		}

		maxWorkerCountChangeSignalChan := p.maxWorkerCountChangeSignalChan.Load().(chan struct{})

		select {
		case p.taskChan <- elem:
			return true, nil
		default:
			if res, err := p.tryRunWithNewTaskWorker(elem); err != nil {
				return false, err
			} else if res {
				return true, nil
			}

			if !withBlock {
				return false, nil
			}

			select {
			case p.taskChan <- elem:
				return true, nil
			case <-p.oneTaskWorkerExitSignalChanForAddTask:
			case <-maxWorkerCountChangeSignalChan:
			case <-p.stopAllTaskWorkerSignalChan:
				return false, newGoroutinePoolStoppedError()
			}
		}
	}
}

// 修改p允许并发执行的TaskWorker的最大个数为newMaxCount
func (p *Pool) ChangeTaskWorkerCount(newMaxCount uint32) error {
	if !p.isInitSuccess() {
		return newGoroutinePoolNotInitError()
	}
	if newMaxCount < MinTaskWorkerCount || newMaxCount > MaxTaskWorkerCount {
		return newConfigIllegalError(fmt.Sprintf("param newMaxCount not in [%v, %v]", MinTaskWorkerCount, MaxTaskWorkerCount))
	}

	if !p.tryGetModifyConfigLock() {
		return newGoroutinePoolConfigModifyingError()
	}
	defer p.releaseModifyConfigLock()

	for {
		if p.stopped() {
			return newGoroutinePoolStoppedError()
		}

		oldStatus := p.analysisStatus()
		if oldStatus.maxWorkerCount == uint64(newMaxCount) {
			return nil
		}

		newStatus := oldStatus
		newStatus.maxWorkerCount = uint64(newMaxCount)
		if atomic.CompareAndSwapUint64(p.getStatusNumPtr(), oldStatus.status, p.calcStatus(newStatus)) {
			break
		}
	}

	for {
		if p.stopped() {
			return newGoroutinePoolStoppedError()
		}

		if status := p.analysisStatus(); status.runningWorkerCount <= status.maxWorkerCount {
			break
		}

		select {
		case p.stopOneTaskWorkerSignalChan <- struct{}{}:
			atomic.AddUint64(p.getStatusNumPtr(), ^((uint64(1) << runningCountLowBit) - 1))
		case <-p.oneTaskWorkerJustExitSignalChanForChangeTaskWorkerCount:
		case <-p.stopAllTaskWorkerSignalChan:
			return newGoroutinePoolStoppedError()
		}
	}

	oldMaxWorkerCountChangeSignalChan := p.maxWorkerCountChangeSignalChan.Load().(chan struct{})
	p.maxWorkerCountChangeSignalChan.Store(make(chan struct{}))
	close(oldMaxWorkerCountChangeSignalChan)

	return nil
}

// 修改p的KeepaliveTime为newKeepaliveTime, 修改成功时返回(oldKeepaliveTime, nil)
func (p *Pool) ChangeKeepaliveTime(newKeepaliveTime time.Duration) (time.Duration, error) {
	if newKeepaliveTime < 0 {
		return 0, newConfigIllegalError("param newKeepaliveTime less than 0")
	}
	if !p.isInitSuccess() {
		return 0, newGoroutinePoolNotInitError()
	}
	if p.stopped() {
		return 0, newGoroutinePoolStoppedError()
	}

	if !p.tryGetModifyConfigLock() {
		return 0, newGoroutinePoolConfigModifyingError()
	}
	defer p.releaseModifyConfigLock()

	oldKeepalivePair := p.keepaliveTimeAndKeepaliveTimeChangeSignalChanPair.Load().([2]interface{})
	oldKeepaliveTime := oldKeepalivePair[0].(time.Duration)
	oldKeepaliveTimeChangeSignalChan := oldKeepalivePair[1].(chan struct{})

	if oldKeepaliveTime == newKeepaliveTime {
		return oldKeepaliveTime, nil
	}

	p.keepaliveTimeAndKeepaliveTimeChangeSignalChanPair.Store([2]interface{}{newKeepaliveTime, make(chan struct{})})
	close(oldKeepaliveTimeChangeSignalChan)

	return oldKeepaliveTime, nil
}

// 返回TaskWorker p的状态信息
func (p *Pool) GetStatus() (TaskWorkerStatus, error) {
	if !p.isInitSuccess() {
		return TaskWorkerStatus{}, newGoroutinePoolNotInitError()
	}
	if p.stopped() {
		return TaskWorkerStatus{}, newGoroutinePoolStoppedError()
	}

	if !p.tryGetModifyConfigLock() {
		return TaskWorkerStatus{}, newGoroutinePoolConfigModifyingError()
	}
	defer p.releaseModifyConfigLock()

	status := p.analysisStatus()
	return TaskWorkerStatus{
		RunningWorkerCount: status.runningWorkerCount,
		MaxWorkerCount:     status.maxWorkerCount,
		KeepaliveTime:      p.keepaliveTimeAndKeepaliveTimeChangeSignalChanPair.Load().([2]interface{})[0].(time.Duration),
	}, nil
}

func (p *Pool) Stop() error {
	if !p.isInitSuccess() {
		return newGoroutinePoolNotInitError()
	}

	if !p.tryGetModifyConfigLock() {
		return newGoroutinePoolConfigModifyingError()
	}
	defer p.releaseModifyConfigLock()

	if p.stopped() {
		return newGoroutinePoolStoppedError()
	}

	close(p.stopAllTaskWorkerSignalChan)
	p.wg.Wait()

	return nil
}

func checkConfig(c Config) error {
	if c.InitialTaskWorkerCount < MinTaskWorkerCount || c.InitialTaskWorkerCount > MaxTaskWorkerCount {
		return newConfigIllegalError(fmt.Sprintf("InitialTaskWorkerCount not in [%v, %v]", MinTaskWorkerCount, MaxTaskWorkerCount))
	}
	if c.MaxTaskWorkerCount < MinTaskWorkerCount || c.MaxTaskWorkerCount > MaxTaskWorkerCount {
		return newConfigIllegalError(fmt.Sprintf("MaxTaskWorkerCount not in [%v, %v]", MinTaskWorkerCount, MaxTaskWorkerCount))
	}
	if c.InitialTaskWorkerCount > c.MaxTaskWorkerCount {
		return newConfigIllegalError(fmt.Sprintf("InitialTaskWorkerCount more than MaxTaskWorkerCount"))
	}
	if c.KeepaliveTime < 0 {
		return newConfigIllegalError(fmt.Sprintf("KeepaliveTime < 0"))
	}
	if c.MonitorTimeInterval > 0 && c.MonitorCallback == nil {
		return newConfigIllegalError(fmt.Sprintf("MonitorTimeInterval less than 0, but param.MonitorCallback is nil"))
	}
	return nil
}

func (p *Pool) runTask(elem *taskChanElem, needSaveTaskInfo bool) {
	taskId := atomic.AddUint64(func() *uint64 {
		if uintptr(unsafe.Pointer(&p.status))%8 == 0 {
			return (*uint64)(unsafe.Pointer(&p.status[2]))
		}
		// 兼容32位机器环境, 保证状态字(uint64)是8字节对齐的
		return (*uint64)(unsafe.Pointer(&p.status[3]))
	}(), 1)

	defer func() {
		if needSaveTaskInfo {
			p.taskStatusMap.Delete(taskId)
		}

		if err := recover(); err != nil && p.handleTaskPanic != nil {
			p.handleTaskPanic(err)
		}
	}()

	if needSaveTaskInfo {
		p.taskStatusMap.Store(taskId, TaskInfo{
			StartTimestamp: time.Now(),
			Data:           elem.data,
		})
	}

	elem.task()
}

// 尝试新创建的1个TaskWork执行task, 成功时返回true
func (p *Pool) tryRunWithNewTaskWorker(elem *taskChanElem) (bool, error) {
	if status := p.analysisStatus(); status.runningWorkerCount < status.maxWorkerCount {
		for i, loopCnt := 1, int(status.maxWorkerCount-status.runningWorkerCount); i <= loopCnt; i++ {
			if p.stopped() {
				return false, newGoroutinePoolStoppedError()
			}

			oldStatus := p.analysisStatus()
			if oldStatus.runningWorkerCount >= oldStatus.maxWorkerCount {
				return false, nil
			}

			newStatus := oldStatus
			newStatus.runningWorkerCount++
			if atomic.CompareAndSwapUint64(p.getStatusNumPtr(), oldStatus.status, p.calcStatus(newStatus)) {
				p.execOneTaskWorker(elem)
				return true, nil
			}
		}
	}
	return false, nil
}

// 启动一个新的TaskWorker, 要求对runningTaskWorkerCount的加1操作已经在调用方完成
func (p *Pool) execOneTaskWorker(elem *taskChanElem) {
	p.wg.Add(1)
	go func() {
		defer func() {
			select {
			case p.oneTaskWorkerExitSignalChanForAddTask <- struct{}{}:
			default:
			}
			p.wg.Done()
		}()

		if elem != nil {
			p.runTask(elem, true)
		}

		for {
			for {
				if p.stopped() {
					return
				}

				oldStatus := p.analysisStatus()
				if oldStatus.runningWorkerCount <= oldStatus.maxWorkerCount {
					break
				}

				newStatus := oldStatus
				newStatus.runningWorkerCount--
				if atomic.CompareAndSwapUint64(p.getStatusNumPtr(), oldStatus.status, p.calcStatus(newStatus)) {
					select {
					case p.oneTaskWorkerJustExitSignalChanForChangeTaskWorkerCount <- struct{}{}:
					case <-p.stopAllTaskWorkerSignalChan:
					}
					return
				}
			}

			// 读取keepaliveTime和ChangeSignalChan在当前时刻的快照
			keepalivePair := p.keepaliveTimeAndKeepaliveTimeChangeSignalChanPair.Load().([2]interface{})
			keepaliveTime := keepalivePair[0].(time.Duration)
			keepaliveTimeChangeSignalChan := keepalivePair[1].(chan struct{})

			timer := time.NewTimer(keepaliveTime)

			select {
			case e := <-p.taskChan:
				timer.Stop()
				p.runTask(e, true)
			case <-p.stopOneTaskWorkerSignalChan:
				// runningTaskWorkerCount的减1操作已经在向t.stopOneSignalChan发送数据一方完成, 此处直接return即可
				timer.Stop()
				return
			case <-p.stopAllTaskWorkerSignalChan:
				timer.Stop()
				return
			case <-keepaliveTimeChangeSignalChan:
				timer.Stop()
			case <-timer.C:
				if status := p.analysisStatusHelp(atomic.AddUint64(p.getStatusNumPtr(), ^((uint64(1) << runningCountLowBit) - 1))); status.runningWorkerCount+1 > status.maxWorkerCount {
					select {
					case p.oneTaskWorkerJustExitSignalChanForChangeTaskWorkerCount <- struct{}{}:
					case <-p.stopAllTaskWorkerSignalChan:
					}
				}
				return
			}
		}
	}()
}

// 周期巡查GoroutinePool状态并通过callback的参数返回给调用方, 如果callback返回true将启动Monitor Tasker
// Monitor Tasker的作用是循环从taskChan获取TaskElem执行, 直到收到结束Monitor Tasker的信号后退出
func (p *Pool) monitor(timeInterval time.Duration, callback func(map[uint64]TaskInfo) bool) {
	ticker := time.NewTicker(timeInterval)
	defer ticker.Stop()

	var exitSignalChan chan struct{}

	// 为0表示Monitor Task正在执行, 为1表示未在运行
	var monitor int32

	for {
		select {
		case <-ticker.C:
			taskStatusMap := make(map[uint64]TaskInfo)
			p.taskStatusMap.Range(func(key interface{}, value interface{}) bool {
				taskStatusMap[key.(uint64)] = value.(TaskInfo)
				return true
			})

			if !callback(taskStatusMap) {
				if atomic.LoadInt32(&monitor) != 0 && exitSignalChan != nil {
					// 停止正在执行的Monitor Tasker
					close(exitSignalChan)
					exitSignalChan = nil
				}
				continue
			}

			// 尝试创建Monitor Tasker
			if !atomic.CompareAndSwapInt32(&monitor, 0, 1) {
				// 有Monitor Task正在运行
				continue
			}

			exitSignalChan = make(chan struct{})

			p.wg.Add(1)
			go func(exitSignalChan chan struct{}) {
				defer func() {
					atomic.StoreInt32(&monitor, 0)
					p.wg.Done()
				}()

				for {
					select {
					case e := <-p.taskChan:
						p.wg.Add(1)
						go func(task *taskChanElem) {
							defer p.wg.Done()
							p.runTask(task, false)
						}(e)
					case <-exitSignalChan:
						return
					case <-p.stopAllTaskWorkerSignalChan:
						return
					}
				}
			}(exitSignalChan)
		case <-p.stopAllTaskWorkerSignalChan:
			return
		}
	}
}

// 当前GoroutinePool的状态, 使用uint64表示当前状态, 约定最低位为第0位, 第runningCountLowBit~runningCountHighBit表示当前执行
// 中的taskWorker的个数, 第maxCountLowBit~maxCountHighBit表示当前允许并发的taskWorker的个数上限, 目前第maxCountHighBit+1~63位
// 为预留标志位, 返回的地址为8字节对齐, 注: 在TaskWorkerStatus字段的下一字节开始为TaskId字段
func (p *Pool) getStatusNumPtr() *uint64 {
	if uintptr(unsafe.Pointer(&p.status))%8 == 0 {
		return (*uint64)(unsafe.Pointer(&p.status))
	}
	// 兼容32位机器环境, 保证状态字(uint64)是8字节对齐的
	return (*uint64)(unsafe.Pointer(&p.status[1]))
}

func (p *Pool) tryGetModifyConfigLock() bool {
	return atomic.CompareAndSwapUint32(&p.modifying, 0, 1)
}

func (p *Pool) releaseModifyConfigLock() {
	atomic.StoreUint32(&p.modifying, 0)
}

func (p *Pool) analysisStatus() taskConsumerStatus {
	return p.analysisStatusHelp(atomic.LoadUint64(p.getStatusNumPtr()))
}

func (p *Pool) analysisStatusHelp(status uint64) taskConsumerStatus {
	f := func(lowBitLocal int, highBitLocal int) uint64 {
		var mask uint64
		for i := lowBitLocal; i <= highBitLocal; i++ {
			mask += uint64(1) << i
		}
		return (status & mask) >> lowBitLocal
	}
	return taskConsumerStatus{
		runningWorkerCount: f(runningCountLowBit, runningCountHighBit),
		maxWorkerCount:     f(maxCountLowBit, maxCountHighBit),
		initStatus:         f(initStatusBit, initStatusBit),
		status:             status,
	}
}

func (p *Pool) calcStatus(status taskConsumerStatus) uint64 {
	f := func(value uint64, lowBitLocal int) uint64 {
		return value << lowBitLocal
	}
	return f(status.runningWorkerCount, runningCountLowBit) + f(status.maxWorkerCount, maxCountLowBit) + f(status.initStatus, initStatusBit)
}

func (p *Pool) isInitSuccess() bool {
	return p.analysisStatus().initStatus == 1
}

func (p *Pool) stopped() bool {
	select {
	case <-p.stopAllTaskWorkerSignalChan:
		return true
	default:
		return false
	}
}

const (
	// 使用uint64表示当前状态
	// 约定: 最低位为第0位
	// 第runningCountLowBit~runningCountHighBit表示当前执行中的taskWorker的个数
	runningCountLowBit  = 0
	runningCountHighBit = 19

	// 第maxCountLowBit~maxCountHighBit表示当前允许并发的taskWorker的个数上限
	maxCountLowBit  = 20
	maxCountHighBit = 39

	// 目前第maxCountHighBit+1~62位为预留标志位
	// 第initStatusBit位表示当前GoroutinePool的初始化状态
	initStatusBit = 63

	MinTaskWorkerCount = 0
	MaxTaskWorkerCount = (1 << (maxCountHighBit - maxCountLowBit + 1)) - 1
)

// Pool支持:
//  1:动态调整执行任务的Goroutine的个数(TaskWorkerCount)
//  2:动态调节Goroutine空闲超时自动退出的时间(KeepaliveTime)
//  3:Goroutine状态周期巡查(可用于监控任务执行时间, 发现和破除协程池死锁)
type Pool struct {
	// status第1个满足8字节对齐的地址开始的前8个字节存储GoroutinePool的状态信息, 包括当前正在运行的TaskWorker的个数, 允
	// 许并发运行的最大TaskWorker的个数等, 在之后的8个字节存储下一个被执行的Task的Id
	status [5]uint32

	// 所有提交给GoroutinePool的Task均会尝试写入此chan
	taskChan chan *taskChanElem

	// TaskWorker从此chan中读出元素时表示当前TaskWorker需要立即结束
	stopOneTaskWorkerSignalChan chan struct{}

	// TaskWorker通过向此chan发送一个信号(空结构体)表示当前TaskWorker已经结束
	oneTaskWorkerJustExitSignalChanForChangeTaskWorkerCount chan struct{}

	// 每个协程结束后均会向此chan非阻塞的发送一个信号(struct{}{})
	oneTaskWorkerExitSignalChanForAddTask chan struct{}

	// 存储的实际类型为chan struct{}, 每次MaxWorkerCount被修改后当前的maxWorkerCountChangeSignalChan均会被close
	// TaskWorker从此chan中读出元素时表示当前GoroutinePool的MaxWorkerCount已经被修改
	maxWorkerCountChangeSignalChan atomic.Value

	// 存储的实际类型为[2]interface{}{time.Duration, chan struct{}}, 第1个元素为keepaliveTime, 第2个为keepaliveTimeChangeSignalChan
	// , 连续keepaliveTime时间内未执行任务的TaskWorker将结束, TaskWorker从keepaliveTimeAndKeepaliveTimeChangeSignalChanPair中读出元素时表示当
	// 前GoroutinePool的keepaliveTime已经被修改
	keepaliveTimeAndKeepaliveTimeChangeSignalChanPair atomic.Value

	// 修改TaskWorker最大并发执行个数和keepaliveTime时通过modifying实现串行
	modifying uint32

	// 从此chan中可以读出元素时表示本GoroutinePool已经Stop
	stopAllTaskWorkerSignalChan chan struct{}

	// Task运行过程如果存在未被捕获的panic且handleTaskPanic不为nil, 则将对panic的对象err调用handleTaskPanic(err)
	handleTaskPanic func(err interface{})

	// 用于实现本GoroutinePool调用Stop()时等待本GoroutinePool创建的所有Goroutine退出
	wg sync.WaitGroup

	// 存储当前运行的Task的信息, key对应TaskId, value对应TaskInfo
	taskStatusMap sync.Map
}

type TaskWorkerStatus struct {
	// 处于运行中状态的TaskWorker的个数
	RunningWorkerCount uint64

	// 允许并发执行的TaskWorker的最大个数
	MaxWorkerCount uint64

	// 每个TaskWorker的最大连续空闲时间
	KeepaliveTime time.Duration
}

type TaskInfo struct {
	// 任务开始执行的时间
	StartTimestamp time.Time

	// AddTask时携带的Data
	Data interface{}
}

type taskChanElem struct {
	// 与下面的task对应, 可用于标识Task等
	data interface{}

	// 待执行的Task
	task func()
}

type taskConsumerStatus struct {
	runningWorkerCount uint64
	maxWorkerCount     uint64
	initStatus         uint64
	status             uint64
}

// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< error定义 <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
type GoroutinePoolNotInitError struct{}

func newGoroutinePoolNotInitError() GoroutinePoolNotInitError {
	return GoroutinePoolNotInitError{}
}

func (GoroutinePoolNotInitError) Error() string {
	return "the GoroutinePool is not init"
}

type ConfigIllegalError struct {
	str string
}

func newConfigIllegalError(str string) ConfigIllegalError {
	return ConfigIllegalError{str: str}
}

func (err ConfigIllegalError) Error() string {
	return err.str
}

type GoroutinePoolStoppedError struct{}

func newGoroutinePoolStoppedError() GoroutinePoolStoppedError {
	return GoroutinePoolStoppedError{}
}

func (GoroutinePoolStoppedError) Error() string {
	return "the GoroutinePool is stopped"
}

type GoroutinePoolConfigModifyingError struct{}

func newGoroutinePoolConfigModifyingError() GoroutinePoolConfigModifyingError {
	return GoroutinePoolConfigModifyingError{}
}

func (GoroutinePoolConfigModifyingError) Error() string {
	return "the GoroutinePool config is modifying"
}
