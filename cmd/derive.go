package cmd

// 第二层：带派生资源的热重载。
//
// 最简版的问题：配置一旦派生出资源（这里是一把 Redis 连接），
// "只换配置"就不够了——新配置要建新连接，旧连接要排空回收。
//
// 多出来的只有三个动作，其余和最简版一模一样：
//
//   ① 先建后换：新资源构建+验证失败 → 不换，旧配置继续服务（天然回滚）
//   ② 原子替换：cfg 和 client 绑成一个 struct，一次 Store 整体换
//      （分两次换就会撕裂：新配置配旧连接）
//   ③ 排空回收：旧连接延迟 5 秒再关，给在途请求留时间
//
// 注意：list 变化要启停消费者 goroutine，那是第三层（reconcile），这里不做。

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/redis/go-redis/v9"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// snap 就是"配置 + 派生资源"的不可变整体。
// 换就整个换，读就整体读，永不撕裂。
type snap struct {
	cfg    MinConfig
	client *redis.Client
}

var cur atomic.Value // 始终存 snap

var deriveCmd = &cobra.Command{
	Use:   "derive",
	Short: "带派生资源的热重载：换配置 = 换连接，改 config/minimal.yml 观察",
	RunE: func(cmd *cobra.Command, args []string) error {
		v := viper.New()
		v.AddConfigPath("./config")
		v.SetConfigName("minimal")
		v.SetConfigType("yml")
		if err := v.ReadInConfig(); err != nil {
			return err
		}

		apply(v) // 首次加载

		// 防抖：编辑器保存一次往往连发多个 WRITE 事件，
		// 直接每次 apply 会重复执行。用缓冲为 1 的 channel 合并：
		//   - 已有待处理事件时新事件直接丢弃（default 分支）
		//   - 处理前先等 200ms 静默，再排空窗口内堆积的事件
		//   - apply 本身幂等（总是套用最新配置），多跑一次也无害
		events := make(chan struct{}, 1)
		go func() {
			for range events {
				time.Sleep(200 * time.Millisecond) // 等变更安静下来
				select {
				case <-events: // 丢掉窗口期内堆积的【信号】：信号不是数据，
					// apply 马上会从文件读最新内容，最后一次的状态在文件里
				default:
				}
				apply(v)
			}
		}()
		v.WatchConfig()
		v.OnConfigChange(func(e fsnotify.Event) {
			select {
			case events <- struct{}{}:
			default: // 已经排过队了，这个事件合并掉
			}
		})

		// 每 2 秒用"当前快照"里的连接 Ping 一次，模拟业务在用资源
		t := time.NewTicker(2 * time.Second)
		defer t.Stop()
		ctx := context.Background()
		for range t.C {
			s := cur.Load().(snap)           // 整体读：cfg 和 client 一定配套
			err := s.client.Ping(ctx).Err()  // 每轮重新 Load，自动跟随重载
			if err != nil {
				fmt.Printf("cfg=%s:%d ping ERR: %v\n", s.cfg.Host, s.cfg.Port, err)
				continue
			}
			fmt.Printf("cfg=%s:%d ping ok\n", s.cfg.Host, s.cfg.Port)
		}
		return nil
	},
}

// apply 是重载的全部：解析 → 校验 → 建新资源 → 原子换 → 排空旧的。
// 任何一步失败都不动 cur，旧快照继续服务——这就是"失败即回滚"。
func apply(v *viper.Viper) {
	var cfg MinConfig
	if err := v.Unmarshal(&cfg); err != nil || cfg.Host == "" || cfg.Port <= 0 {
		fmt.Println("[reload] bad config, keep old")
		return
	}

	// ① 先建新资源并当场验证（Ping），建不出来就放弃
	nc := redis.NewClient(&redis.Options{
		Addr: fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := nc.Ping(ctx).Err(); err != nil {
		nc.Close()
		fmt.Printf("[reload] new client unreachable (%v), keep old\n", err)
		return
	}

	// ② 原子换快照：这一行之后所有读者都拿到"新配置+新连接"
	old := cur.Load()
	cur.Store(snap{cfg: cfg, client: nc})
	fmt.Printf("[reload] switched to %s:%d\n", cfg.Host, cfg.Port)

	// ③ 排空回收：旧连接延迟关闭，在途请求有时间跑完
	if old != nil {
		go func() {
			time.Sleep(5 * time.Second)
			old.(snap).client.Close()
			fmt.Println("[reload] old client closed")
		}()
	}
}

func init() { rootCmd.AddCommand(deriveCmd) }
