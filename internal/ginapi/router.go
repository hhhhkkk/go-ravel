package ginapi

import "github.com/gin-gonic/gin"

type IServer interface {
	RegistryRoute(gin *gin.Engine)
}

type GinApiEngine struct {
	engine     *gin.Engine
	serverList []IServer
}

func newEngine() *GinApiEngine {
	return &GinApiEngine{
		engine:     gin.Default(),
		serverList: make([]IServer, 0),
	}
}

func Run() {
	engine := newEngine()

	engine.serverList = append(engine.serverList)
}

// func (g *GinApiEngine)
