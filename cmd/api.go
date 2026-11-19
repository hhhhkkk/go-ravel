package cmd

import (
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/spf13/cobra"
)

var serverCmd = &cobra.Command{
	Use:   "gin",
	Short: "启动 API 服务",
	RunE: func(cmd *cobra.Command, args []string) error {
		// 测试让gin直接起来
		engine := gin.Default()
		engine.GET("/", func(ctx *gin.Context) {
			ctx.JSON(http.StatusOK, gin.H{
				"msg": "Hello World",
			})
		})
		if err := engine.Run(":8080"); err != nil {
			fmt.Println(fmt.Errorf("err %v", err))
			return err
		}
		return nil
	},
}

func init() { rootCmd.AddCommand(serverCmd) }
