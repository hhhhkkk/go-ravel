package service

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

type IndexService struct{}



func (s *IndexService) Index(ctx *gin.Context) {
	ctx.JSON(http.StatusOK, gin.H{
		"msg": "Hello World",
	})
}
