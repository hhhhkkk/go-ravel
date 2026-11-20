package admin

import (
	"github.com/gin-gonic/gin"
	"github.com/hhhhkkk/goravel/internal/ginapi/admin/service"
)

type AdminServer struct{}

func (s *AdminServer) RegistryRoute(g *gin.Engine) {
	group := g.Group("/admin")
	indexService := &service.IndexService{}
	{
		group.GET("/index", indexService.Index)
	}
}
