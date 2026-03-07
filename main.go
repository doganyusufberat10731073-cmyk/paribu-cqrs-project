package main

import (
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
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

// 2. Adım: Veritabanına bağlama fonksiyonu
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

func main() {
	// API kapılarını açmadan önce veritabanını hazırlıyalım
	initDB()

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

		// 4. Siparişi değil, olayı veritabanına kaydet
		result := DB.Create(&newEvent)

		// Eğer kaydederken hata çıkarsa (Örneğin aynı ID den ikinci kez göndermeye çalışırsak)
		if result.Error != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Olay veritabanına kaydedilmedi (Aynı ID kullanılmış olabilir)"})
			return
		}

		// Kayıt başarıyla yapılınca kullanıcıya haber edelim
		c.JSON(http.StatusOK, gin.H{
			"mesaj": "Event Sourcing başarılı 'OrderCreated olayı veritabanına yazıldı",
			"olay":  newEvent,
		})
	})

	// Sunucuyu çalıştır
	r.Run(":8081")
}
