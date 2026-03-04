package main

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// 1. Adım: Gelen siparişin şeklini tanımlayalım
type Order struct {
	ID        string  `json:"id"`
	ProductID string  `json:"product_id"`
	Quantity  int     `json:"quantity"`
	Price     float64 `json:"price"`
}


func main() {
	// 2. Adım: Gin motorunu başlatacaz
	r := gin.Default()

	// 3. Adım: Kapıyı (endpoint) oluşturacaz. Sadece post isteklerini kabul eder.
	r.POST("/order", func(c *gin.Context) {

		// Bos bir sipariş kutusu oluşturalım
		var newOrder Order

		// Kullanıcının gönderdiği JSON versini alıp, bizim bos kutuya (newOrder) doldurmaya çalışıyoruz.
		if err := c.ShouldBindJSON(&newOrder); err != nil {
			// Eğer veri bozuksa veya yanlış formatta gelirse hata fırlat
			c.JSON(http.StatusBadRequest, gin.H{"error": "Gecersiz veri formatı. Lütfen doğru JSON gönderin."})
			return
		}

		// Şu anlık test: Veriyi aldık, veritabanına veya Redpandaya gönderiyecez
		// Sadece vriyi doğtu adlığımızı kanıtlamak için kullanıcıya geri dönürüyoruz
		c.JSON(http.StatusOK, gin.H{
			"mesaj":          "Sipariş başarıyla alındı (CQRS - Command tarafı çalışıyor)",
			"siparis_detayi": newOrder,
		})
	})

	// 4. Adım: Sunucuyu çalıştıralım 
	// 8080 portunu Redpanda kullanıyor bu yüzden API mizi 8081 portunda başlatıyoruz

	r.Run(":8081")

}
