package ginapi

import "github.com/gin-gonic/gin"

type GinApiEngine struct {
	engine *gin.Engine
}

func Init() *GinApiEngine {
	return &GinApiEngine{
		engine: gin.Default(),
	}
}

// func (g *GinApiEngine)
