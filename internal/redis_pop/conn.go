package redispop

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/redis/go-redis/v9"
	"github.com/spf13/viper"
)

// Config 是一份纯数据配置：只有值类型字段，保证可整体复制、整体替换。
// 永远不要把连接、goroutine 等资源塞进来。
type Config struct {
	Host string   `mapstructure:"host"`
	Port int      `mapstructure:"port"`
	Db   int      `mapstructure:"db"`
	List []string `mapstructure:"list"`
}

// Snapshot 把"配置 + 由配置派生出的资源"绑成一个不可变整体。
// 读者要么看到旧的一对，要么看到新的一对，永远不会撕裂
// （这解决了旧版 Config()/Client() 分两次加锁的撕裂读问题）。
type Snapshot struct {
	Cfg    Config
	Client *redis.Client
}

// RedisCoon 持有当前生效的快照。
// 读：无锁，atomic.Pointer.Load，任意 goroutine 随时可读。
// 写：Update 内部"先建后换"，失败整体回滚。
type RedisCoon struct {
	snap atomic.Pointer[Snapshot]

	// onReload 在新快照生效后回调，用于重建派生资源（如消费者 goroutine）。
	// 这就是"事件源无关"的钩子：无论变更来自 viper/etcd/nacos，
	// 走到这里的都是同一套重载流程。
	// 它在启动阶段通过 New 注册、之后只读，不需要原子保护。
	onReload func(old, new Snapshot)
}

func New(v *viper.Viper, onReload func(old, new Snapshot)) (*RedisCoon, error) {
	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, err
	}

	r := &RedisCoon{onReload: onReload}
	if err := r.Update(cfg); err != nil {
		return nil, err
	}

	v.WatchConfig()
	// 注意：这个回调跑在 fsnotify 的 goroutine 上，只做轻活。
	// 真正的重载逻辑全在 Update 里，与事件来源解耦。
	v.OnConfigChange(func(in fsnotify.Event) {
		var next Config
		if err := v.Unmarshal(&next); err != nil {
			fmt.Printf("[reload] parse failed, keep old: %v\n", err)
			return
		}
		// 校验/建连失败时 Update 返回 err 且不改动快照 => 失败即回滚
		if err := r.Update(next); err != nil {
			fmt.Printf("[reload] apply failed, keep old: %v\n", err)
			return
		}
		fmt.Printf("[reload] config applied: %s:%d db=%d list=%v\n",
			next.Host, next.Port, next.Db, next.List)
	})

	return r, nil
}

// Snapshot 返回当前生效快照。返回的是不可变值，调用方可放心长期持有，
// 但要注意：client 可能在排空期后被关闭，持有者需对报错做重试。
func (r *RedisCoon) Snapshot() Snapshot {
	s := r.snap.Load()
	if s == nil {
		return Snapshot{}
	}
	return *s
}

// Update 是重载的核心流程，任何事件源最终都收敛到这里：
//
//	① 校验纯数据（fail fast，不碰任何现有状态）
//	② 构建并验证新资源（失败 => 整体放弃，旧快照继续服务，即回滚）
//	③ 原子替换快照（这一行之后所有读者都看到新配置）
//	④ 通知派生层 reconcile（启动/停止 goroutine 等）
//	⑤ 排空并回收旧资源
func (r *RedisCoon) Update(cfg Config) error {
	if err := validate(cfg); err != nil {
		return err
	}

	nc, err := newClient(cfg)
	if err != nil {
		return err // 新资源没建成功 => 什么都不改，天然回滚
	}

	old := r.snap.Load()
	snap := &Snapshot{Cfg: cfg, Client: nc}
	r.snap.Store(snap) // ③ 原子切换

	if r.onReload != nil {
		var oldSnap Snapshot
		if old != nil {
			oldSnap = *old
		}
		r.onReload(oldSnap, *snap) // ④ 重建派生资源
	}

	if old != nil {
		drainClose(old.Client, 5*time.Second) // ⑤ 排空旧资源
	}
	return nil
}

// validate 只做纯数据校验，越早拒绝越好。
func validate(cfg Config) error {
	if cfg.Host == "" {
		return fmt.Errorf("host is empty")
	}
	if cfg.Port <= 0 || cfg.Port > 65535 {
		return fmt.Errorf("invalid port %d", cfg.Port)
	}
	if cfg.Db < 0 {
		return fmt.Errorf("invalid db %d", cfg.Db)
	}
	if len(cfg.List) == 0 {
		return fmt.Errorf("list is empty")
	}
	return nil
}

// newClient 构建资源并当场验证（Ping），保证存进快照的 client 一定可用。
func newClient(cfg Config) (*redis.Client, error) {
	r := redis.NewClient(&redis.Options{
		Addr: fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
		DB:   cfg.Db,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := r.Ping(ctx).Result(); err != nil {
		r.Close()
		return nil, fmt.Errorf("new redis unreachable: %w", err)
	}
	return r, nil
}

// drainClose 延迟关闭旧资源，给在途请求留出排空时间。
// 进阶做法是引用计数或在旧 client 上主动 Cancel，这里用最简单可靠的延迟排空。
func drainClose(c *redis.Client, after time.Duration) {
	go func() {
		defer func() {
			if err := recover(); err != nil {
				fmt.Printf("[reload] close old client panic: %v\n", err)
			}
		}()
		time.Sleep(after)
		_ = c.Close()
	}()
}

func newConfigViper() (*viper.Viper, error) {
	v := viper.New()

	v.AddConfigPath("./config")
	v.SetConfigType("yml")
	v.SetConfigName("redisPop")

	err := v.ReadInConfig()
	if err != nil {
		return nil, fmt.Errorf("redisPop: %w", err)
	}
	return v, nil
}
