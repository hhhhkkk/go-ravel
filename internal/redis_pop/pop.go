package redispop

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
)

// Popper 管理按队列划分的消费者 goroutine，是"派生资源 reconcile"的示范：
// 配置里的 list 变化后，光换 client 没用，必须
//   启动新增队列的 goroutine
//   停止被删除队列的 goroutine
//   保持交集不动（无感）
type Popper struct {
	mgr *RedisCoon

	mu      sync.Mutex
	running map[string]*consumer // 当前状态：队列名 -> 消费者
}

type consumer struct {
	cancel context.CancelFunc
	done   chan struct{} // goroutine 退出信号，用于同步等待停止完成
}

func Run() error {
	v, err := newConfigViper()
	if err != nil {
		return err
	}

	// 回调在 New 里注册，早于 WatchConfig 启动，
	// 因此用普通字段即可，无需原子保护。
	// 闭包捕获 p，p.mgr 在 New 返回后才赋值，
	// 但回调最早也是在 reload 时才被调用，那时 mgr 已就绪。
	p := &Popper{running: map[string]*consumer{}}
	mgr, err := New(v, func(old, new Snapshot) {
		p.reconcile(new.Cfg.List)
	})
	if err != nil {
		return err
	}
	p.mgr = mgr

	// 启动时先按初始配置对齐一次
	p.reconcile(mgr.Snapshot().Cfg.List)

	// 等待退出信号
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	fmt.Println("[pop] shutting down...")
	p.stopAll()
	return nil
}

// reconcile 是状态收敛的核心：期望状态 vs 当前状态，diff 后增删。
// 无论配置怎么变、变多少次，最终状态都会收敛到期望值。
func (p *Popper) reconcile(target []string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	want := map[string]bool{}
	for _, q := range target {
		want[q] = true
	}

	// 停掉不在期望列表里的
	for q, c := range p.running {
		if !want[q] {
			c.cancel()
			<-c.done // 同步等待退出，保证不残留
			delete(p.running, q)
			fmt.Printf("[pop] consumer stopped: %s\n", q)
		}
	}

	// 启动期望列表里新增的
	for q := range want {
		if _, ok := p.running[q]; ok {
			continue
		}
		ctx, cancel := context.WithCancel(context.Background())
		c := &consumer{cancel: cancel, done: make(chan struct{})}
		p.running[q] = c
		go p.worker(ctx, q, c.done)
		fmt.Printf("[pop] consumer started: %s\n", q)
	}
}

func (p *Popper) stopAll() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for q, c := range p.running {
		c.cancel()
		<-c.done
		delete(p.running, q)
	}
}

// worker 是消费循环。关键点：每轮循环都重新读最新快照——
// 这样 client 被换成新的后，下一轮自动用新连接；
// 旧 client 在排空期被关闭时，本轮 BRPop 报错，下一轮即切到新 client。
func (p *Popper) worker(ctx context.Context, queue string, done chan struct{}) {
	defer close(done)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		snap := p.mgr.Snapshot() // 每轮读最新快照，天然跟随重载
		if snap.Client == nil {
			time.Sleep(time.Second)
			continue
		}

		res, err := snap.Client.BRPop(ctx, 3*time.Second, queue).Result()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if errors.Is(err, redis.Nil) {
				continue // 超时无数据，正常
			}
			// 其他错误（含旧 client 被排空关闭）：短暂退避后重试，
			// 下一轮就会拿到新快照里的新 client
			fmt.Printf("[pop] %s brpop err: %v\n", queue, err)
			time.Sleep(time.Second)
			continue
		}
		if len(res) < 2 {
			continue
		}
		fmt.Printf("[pop] %s -> %s\n", queue, res[1])
	}
}
