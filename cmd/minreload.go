package cmd

// 最简化的配置热重载。核心只有一句话：
//
//   配置存进 atomic.Value，变更时整体 Store 一个新值。
//
// 没有结构体、没有接口、没有回调、没有资源管理——
// 就是"读的人永远能拿到一份完整的新配置"这一件事。
// 其余一切（RedisCoon 的 Snapshot、Update、reconcile）
// 都是在这个核上叠加"派生资源怎么跟着换"的问题。

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// MinConfig 纯数据，无任何行为。
type MinConfig struct {
	Host string   `mapstructure:"host"`
	Port int      `mapstructure:"port"`
	List []string `mapstructure:"list"`
}

// 全局唯一的状态：一个原子槽位。写：reload 时；读：任何地方。
var minCfg atomic.Value // 始终存 MinConfig

var reloadCmd = &cobra.Command{
	Use:   "reload",
	Short: "最简化热重载演示：改 config/minimal.yml 观察输出",
	RunE: func(cmd *cobra.Command, args []string) error {
		v := viper.New()
		v.AddConfigPath("./config")
		v.SetConfigName("minimal")
		v.SetConfigType("yml")
		if err := v.ReadInConfig(); err != nil {
			return err
		}

		// 首次加载
		store(v)

		// 感知变更（事件源部分，两行）
		v.WatchConfig()
		v.OnConfigChange(func(e fsnotify.Event) {
			store(v) // 重载部分，也只有一行
		})

		// 每 2 秒打印当前生效的配置，模拟"业务在读配置"
		t := time.NewTicker(2 * time.Second)
		defer t.Stop()
		for range t.C {
			cfg := minCfg.Load().(MinConfig) // 读：一行，无锁
			fmt.Printf("running with: %s:%d list=%v\n", cfg.Host, cfg.Port, cfg.List)
		}
		return nil
	},
}

// store 是"重载"的全部：解析 → 校验 → 原子替换。
// 失败就不 Store，旧配置继续生效，天然回滚。
func store(v *viper.Viper) {
	var c MinConfig
	if err := v.Unmarshal(&c); err != nil || c.Host == "" || c.Port <= 0 {
		fmt.Printf("[reload] bad config, keep old\n")
		return
	}
	minCfg.Store(c) // 这一行就是热重载
}

func init() { rootCmd.AddCommand(reloadCmd) }
