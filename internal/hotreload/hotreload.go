package hotreload

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/redis/go-redis/v9"
	"github.com/spf13/viper"
)

type Config struct {
	A  string `mapstructure:"a"`
	Db struct {
		Addr  string `mapstructure:"addr"`
		Name  string `mapstructure:"name"`
		DbNum int    `mapstructure:"db_num"`
	} `mapstructure:"db"`
}

type realResult struct {
	conn   *redis.Client
	config Config
}

type LongLive struct {
	vp atomic.Value
}

func Run(ctx context.Context) error {
	v, err := getNoticer()
	if err != nil {
		return err
	}

	s, err := new(v)
	if err != nil {
		return err
	}

	// 防抖
	ch := make(chan struct{}, 1)

	go func() {
		for {
			time.Sleep(2 * time.Second)
			select {
			case <-ch:
				s.store(v)
			default:
			}
		}
	}()

	v.WatchConfig()
	v.OnConfigChange(func(in fsnotify.Event) {
		select {
		case ch <- struct{}{}:
		default: // 已经排过队了，这个事件合并掉
		}
	})
	ticker := time.Tick(1 * time.Second)

	for {
		select {
		case <-ticker:
			r, ok := s.vp.Load().(*realResult)
			if !ok {
				return fmt.Errorf("realResult is error")
			}

			fmt.Printf("%s\n", r.config.A)
			fmt.Printf("db name: %s\n", r.config.Db.Name)
			value, err := r.conn.Get(ctx, "test").Result()
			if err != nil {
				if errors.Is(err, redis.Nil) {
					continue
				}
				return err
			}
			fmt.Println(value)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func new(v *viper.Viper) (*LongLive, error) {
	l := &LongLive{}

	err := l.store(v)
	if err != nil {
		return l, err
	}
	return l, nil
}

func (l *LongLive) store(v *viper.Viper) error {

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return err
	}

	conn := redis.NewClient(&redis.Options{
		Addr: cfg.Db.Addr,
		DB:   cfg.Db.DbNum,
	})
	// validate
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := conn.Ping(ctx).Result(); err != nil {
		conn.Close()
		return err
	}
	old, _ := l.vp.Load().(*realResult)
	l.vp.Store(&realResult{
		config: cfg,
		conn:   conn,
	})
	if old != nil {
		go func() {
			time.Sleep(5 * time.Second)
			old.conn.Close()
		}()
	}
	return nil
}

func getNoticer() (*viper.Viper, error) {
	v := viper.New()
	v.AddConfigPath("./config")
	v.SetConfigType("yml")
	v.SetConfigName("hotreload")

	if err := v.ReadInConfig(); err != nil {
		return nil, err
	}

	return v, nil
}
