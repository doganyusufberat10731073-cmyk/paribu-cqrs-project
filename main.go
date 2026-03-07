package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/segmentio/kafka-go" // Postacının kütüphanesi
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// Dışarıdan gelen veriyi okumak için kullandığımız şablon
// (gorm:"primaryKey" yazısını sildik çünkü bunu artık veritabanında tablo yapmayacağız)
type Order struct {
	ID        string  `json:"id" gorm:"primaryKey"`
	ProductID string  `json:"product_id"`
	Quantity  int     `json:"quantity"`
	Price     float64 `json:"price"`
}

// Olay (event)
type Event struct {
	ID          uint `gorm:"primaryKey"`
	AggregateID string
	EventType   string
	Payload     string
	CreatedAt   time.Time
}

// Tüm projemizden veritabanına erişebilmek için globel bir DB değişkeni oluşturdum
var DB *gorm.DB
var KafkaWriter *kafka.Writer // Küresel postacı

// 1. Adım: Veritabanına bağlama fonksiyonu
func initDB() {
	// Docker da kurduğumuz PostgreSQL in kapı numarası ve şifresi (Buna DSN denir)
	dsn := "host=localhost user=root password=rootpassword dbname=paribu_db port=5432 sslmode=disable"

	var err error
	// GORM ile veritabanının kapısını çalıyoruz
	DB, err = gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		// Bağlanmazsa uygulamayı durdur
		log.Fatal("Veritabanına bağlanıılmadı! Hata:", err)
	}

	log.Println("PostgreSQL Veritabanına başarıyla bağlanddı")

	// Eğer veritabanında "orders" diye bir tablo yoksa, GORM bizim için otomatik olarak o tabloyu oluşturur
	// Bu klasik olandır biz burda bunu artık order tablosu değil event tanlosu oluşturacaz
	DB.AutoMigrate(&Event{})
}

// Postacıyı hazırlma fonksiyonu
func initKafka() {
	KafkaWriter = &kafka.Writer{
		Addr:     kafka.TCP("localhost:9092"), // Docker da açtığımız Redpanda kapısı bu
		Topic:    "order-events",              // Olayı bağıracağımız kanalın adı
		Balancer: &kafka.LeastBytes{},
	}
	log.Println("Redpanda (Kafka) Postacısı göreve hazır")
}

func main() {
	// API kapılarını açmadan önce veritabanını hazırlıyalım
	initDB()
	initKafka() // Postacıyı başlat

	// Gin baslat
	r := gin.Default()

	r.POST("/order", func(c *gin.Context) {
		var newOrder Order

		// Gelen siparişi kutuya doldur
		if err := c.ShouldBindJSON(&newOrder); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Gecersiz veri formatı"})
			return
		}

		// 2. Siparişi bir JSON metnine dönüştür (Payload içine koyabilmek için)
		// Go da structı JSON metnine çevirme işlemine "Marshal" denir.
		orderJSON, _ := json.Marshal(newOrder)

		// 3. Bir olay (event) oluştur
		newEvent := Event{
			AggregateID: newOrder.ID,
			EventType:   "OrderCreated",
			Payload:     string(orderJSON),
			CreatedAt:   time.Now(),
		}

		// Önce veritabanına kayet
		if result := DB.Create(&newEvent); result.Error != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Veritabanı hatası"})
			return
		}

		// Kayıt başarılıysa, olayı redpandaya fırlat
		// Olayı json a çeviriyoruz ki postacı taşıyabilsin
		eventBytes, _ := json.Marshal(newEvent)

		err := KafkaWriter.WriteMessages(
			context.Background(),
			kafka.Message{
				Key:   []byte(newEvent.AggregateID), // Sipariş ID
				Value: eventBytes,                   // Tüm olay detayı
			},
		)

		if err != nil {
			// Redpanda ya göndermezsek uyar
			log.Println("Redpanda ya gönderilmedi:", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Postacıya verilmedi"})
			return
		}

		// Kayıt başarıyla yapılınca kullanıcıya haber edelim
		c.JSON(http.StatusOK, gin.H{
			"mesaj": "Olay hem veritabanına yazıldı hem de Redpanda ya fırlatıldı",
			"olay":  newEvent,
		})
	})

	// Sunucuyu çalıştır
	r.Run(":8081")
}
