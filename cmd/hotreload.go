package cmd

import (
	"context"

	"github.com/hhhhkkk/goravel/internal/hotreload"
	"github.com/spf13/cobra"
)

var hotReloadCmd = &cobra.Command{
	Use:   "hr",
	Short: "hr",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := context.Background()
		return hotreload.Run(ctx)
	},
}

func init() { rootCmd.AddCommand(hotReloadCmd) }
