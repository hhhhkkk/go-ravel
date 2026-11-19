package cmd

import (
	"github.com/hhhhkkk/goravel/internal/redis_pop"
	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(redisPopCmd)
}

var redisPopCmd = &cobra.Command{
	Use:   "redis",
	Short: "run redis pop daemon (hot-reloadable config)",
	RunE: func(cmd *cobra.Command, args []string) error {
		return redispop.Run()
	},
}
