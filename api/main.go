package main

import (
	"context"       // Kafka için gerekli
	"encoding/json" // Veriyi JSON a çevirmek için
	"fmt"           // Benzersiz Event ID si üretmek için
	"log"
	"net/http"
	"sync" // Rate limiter için eşzamanlılık kilidi
	"time"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9" // Redis kütüphanemiz
	"github.com/segmentio/kafka-go"
	"github.com/sony/gobreaker" // Sonynin sigorta kütüphanesi
	"golang.org/x/time/rate"    // Rate limiter kütüphanesi
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// Dışarıdan gelen veriyi okumak için kullandığımız şablon
// (gorm:"primaryKey" yazısını sildik çünkü bunu artık veritabanında tablo yapmayacağız)
// binding:"required" bu alan boş gelirse API kapıdan geri çevirir (validasyon)
// binding: "gt=0" miktar ve fiyat 0 dan büyük olmalı
type OrderRequest struct {
	ID        string  `json:"id"`
	UserID    string  `json:"user_id" binding:"required"`
	ProductID string  `json:"product_id" binding:"required"`
	Quantity  int     `json:"quantity" binding:"required,gt=0"`
	Price     float64 `json:"price" binding:"required,gt=0"`
}

// Rezervasyon tablosu, veritabanı hakemliği yani Partial Unique Index (Kısmi Benzersiz İndeks) eklendi
type Reservation struct {
	ID uint `gorm:"primaryKey"`
	// Partial unique index. Eğer status 'active' ise bu product_ide den saece 1 tane olabilir
	ProductID string `gorm:"uniqueIndex:idx_active_reservation,where:status='active'"`
	UserID    string
	ExpiresAt time.Time
	Status    string
}

// Ödeme isteği için şablon
type PaymentRequest struct {
	ReservationID uint    `json:"reservation_id" binding:"required"`
	UserID        string  `json:"user_id" binding:"required"`
	Price         float64 `json:"price" binding:"required,gt=0"`
}

// Olay (event)
type Event struct {
	ID          uint `gorm:"primaryKey"`
	AggregateID string
	EventType   string
	Payload     string
	CreatedAt   time.Time
}

// Kafka ya gönderilmeyi bekleyen mesajların geçici olarak tutulduğu tablo
type OutboxEvent struct {
	ID          uint `gorm:"primaryKey"`
	AggregateID string
	Payload     string // Redpandaya fırlatılacak asıl veri
	CreatedAt   time.Time
}

// Bekleme listesi tablosu
type WaitList struct {
	ID        uint   `gorm:"primaryKey"`
	ProductID string `gorm:"index"`
	UserID    string
	CreatedAt time.Time
}

// Tüm projemizden veritabanına erişebilmek için globel bir DB değişkeni oluşturdum
var DB *gorm.DB
var KafkaWriter *kafka.Writer    // Küresel postacı
var CB *gobreaker.CircuitBreaker // Global sigortamız
var RDB *redis.Client            // Global redis istemcisi
var ctx = context.Background()   // Redis işlemleri için arka plan içeriği

// Rate limiter (DDoS için)
var visitors = make(map[string]*rate.Limiter)
var mu sync.Mutex // Race condition için bir kilit mekanizmasıdır

// IP adresine göre kotayı getiren fonksiyon
func getVisitor(ip string) *rate.Limiter {
	mu.Lock()
	defer mu.Unlock()

	limiter, exists := visitors[ip]
	if !exists {
		// Saniyede 1 isteğe izin ver, anlık en fazla 3 istek hakkı tanı
		limiter = rate.NewLimiter(1, 3)
		visitors[ip] = limiter

	}
	return limiter
}

func rateLimitMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		ip := c.ClientIP()
		limiter := getVisitor(ip)

		if !limiter.Allow() {
			c.JSON(http.StatusTooManyRequests, gin.H{"error": "Çok fazla istek attınız (DDoS koruması). Lütfen yavaşlayın"})
			c.Abort() // İsteği içeri alma
			return
		}

		c.Next()
	}
}

// 1. Adım: Veritabanına bağlama fonksiyonu
func initDB() {
	// Docker da kurduğumuz PostgreSQL in kapı numarası ve şifresi (Buna DSN denir)
	dsn := "host=127.0.0.1 user=root password=rootpassword dbname=paribu_db port=5433 sslmode=disable"

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
	DB.AutoMigrate(&Event{}, &Reservation{}, &OutboxEvent{}, &WaitList{})
}

// Redise bağlanma fonksiyonu
func initRedis() {
	RDB = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379", // Dockerdaki redisin adresi
		Password: "",               // Şifre yok
		DB:       0,                // Varsayılan veritabanı

	})

	_, err := RDB.Ping(ctx).Result()
	if err != nil {
		log.Fatalf("Redise bağlanılamadı: %v", err)

	}

	log.Println("Redis Cache başarıyla bağlandı.")

}

// Arka Plan Çöpçüsü (Expired Reservation Sweeper)
// Bu fonksiyon kendi halinde asenkron çalışır yani ana API yi yavaşlatmaz
func startReservationSweeper() {
	go func() {
		for {
			// Her 5 saniyede bir uyan ve kontrol et
			time.Sleep(5 * time.Second)

			var expiredReservations []Reservation
			// Sürexi dolan ve aktif olan rezervasyonları bul ve "expiredReservations" listesine doldur.
			DB.Where("status = ? AND expires_at < ?", "active", time.Now()).Find(&expiredReservations)

			for _, res := range expiredReservations {
				// Her bir iptal işlemi için Transaction başlat
				DB.Transaction(func(tx *gorm.DB) error {

					// Bekleme listesinde bu ürünü isteyen var mı diye soracağımız kısım
					var waitListCount int64
					tx.Model(&WaitList{}).Where("product_id = ?", res.ProductID).Count(&waitListCount)

					if waitListCount == 0 {
						// Senaryo A: Kapıda bekleyen bir B kişisi yoksa süreyi 5 dakika uzat.
						yeniSure := time.Now().Add(5 * time.Minute)

						// Sadece süteyi güncelliyorum kayda dokunmuyoruz
						if err := tx.Model(&res).Update("expires_at", yeniSure).Error; err != nil {
							return err

						}

						log.Printf("[Akıllı Çöpçü] %s ürünü için bekleme listesi boş. Kullaanıcının süresi 5 dakika uzatıldı.\n", res.ProductID)
						return nil // İşlemi burada bitir aşağıya inip silme
					}

					// 1. Olayın içeriğini hazırla (Hangi ürün boşa çıktı diye)
					payloadJSON := fmt.Sprintf(`{"product_id":"%s"}`, res.ProductID)

					// Consumerların okuyabilmesi için standart Event formatına sokuyoruz
					kafkaMessage := fmt.Sprintf(`{"EventType": "ProductAvailableAgain", "Payload": %q}`, payloadJSON)

					outboxEvent := OutboxEvent{
						AggregateID: fmt.Sprintf("%s-%d", res.ProductID, time.Now().Unix()),
						Payload:     kafkaMessage,
						CreatedAt:   time.Now(),
					}

					// 2. Mesajı Outboxa bırak
					if err := tx.Create(&outboxEvent).Error; err != nil {
						return err
					}

					// 3. Rezervasyonu veritabanından kalıcı olarak sil
					if err := tx.Delete(&res).Error; err != nil {
						return err

					}

					// Gerçekten silindiği an log atalım
					log.Printf("[Çöpçü] %s rünü sepetten düştü. Outboxa bildirim mesajı bırakıldı.\n", res.ProductID)
					return nil

				})
			}

		}
	}()
}

// Veritabanındaki Outbox tablosunu dinleyip Redpanda ya güvenli taşıyan consumer
func startOutboxRelay() {
	go func() {
		for {
			// Her 2 saniyede bir tabloyu kontrol et
			time.Sleep(2 * time.Second)

			var pendingEvents []OutboxEvent
			// Gönderilmeyi bekleyen ilk 100 kaydı al
			DB.Order("id asc").Limit(100).Find(&pendingEvents)

			for _, ev := range pendingEvents {
				// 1. Kafka ya fırlatmaya çalış
				err := KafkaWriter.WriteMessages(
					context.Background(),
					kafka.Message{
						Key:   []byte(ev.AggregateID),
						Value: []byte(ev.Payload),
					},
				)

				// 2. Eğer başarıyla gittiyse, veritabanından sil (Giden kutusunu temizle)
				if err == nil {
					DB.Delete(&ev)
					log.Printf("[OUTBOX] %s ID li olay başarıyla Redpanda ya iletildi ve tablodan silindi.\n", ev.AggregateID)

				} else {
					log.Printf("[OUTBOX HATA] Redpanda ya ulaşılamadı. Mesaj tabloda bekletiliyor: %v\n", err)
					// Kafka çöktüyse döngüyü kırıp 2 saniye sonra tekrar dener, veri asla kaybolmaz
					break

				}
			}
		}
	}()
}

// Postacıyı hazırlma fonksiyonu
func initKafka() {
	KafkaWriter = &kafka.Writer{
		Addr:     kafka.TCP("localhost:9092"),
		Topic:    "order-events",
		Balancer: &kafka.LeastBytes{},
	}

	log.Println("Redpanda (Kafka) Postacısı göreve hazır")
}

func initCircuitBreaker() {
	CB = gobreaker.NewCircuitBreaker(gobreaker.Settings{
		Name:        "Veritabi-Sigortasi",
		MaxRequests: 2,                // Yarı açıkken sadece 2 test isteğine izin ver
		Timeout:     10 * time.Second, // Sigorta attığında 10 saniye boyunca tüm istekleri reddet
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			// Eğer arka arkaya 3 kere hata alırsak örneğin: veritabanı kilitlenirse, sigorta atsın
			return counts.ConsecutiveFailures >= 3
		},
		OnStateChange: func(name string, from gobreaker.State, to gobreaker.State) {
			// Sigorta attığında veya düzeldiğinde konsola büyük harflerle yazılacak mesaj:
			log.Printf("\n[SİGORTA ALARMI] '%s' durumu değişti: %v -> %v\n", name, from, to)

		},
	})
	log.Println("Circuit Breaker (Sigorta) sistemi aktif.")
}

func main() {
	// API kapılarını açmadan önce veritabanını hazırlıyalım yani altyapıyı ve çöpçüyü başlat
	initDB()
	initRedis()
	startReservationSweeper()
	initKafka()
	startOutboxRelay()
	initCircuitBreaker() // Sigortayı başlat

	// Gin baslat
	r := gin.Default()

	// Rate limiteri API nin kapısında bekletiyoruz
	r.Use(rateLimitMiddleware())

	// Rezervasyon API si
	r.POST("/reserve", func(c *gin.Context) {
		var req OrderRequest

		// Validasyon (veri eksik mi?)
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Eksik veya hatalı veri gönderdiniz"})
			return
		}

		// Mevcut rezervasyon kontrolü
		var existingReservation Reservation
		// Veritabanına sor: Bu ürün için şu an active olan ve süresi dolmamış bir kayıt var mı?
		result := DB.Where("product_id = ? AND status = ? AND expires_at > ?", req.ProductID, "active", time.Now()).First(&existingReservation)

		// Fast Reject (Hızlı Red)
		if result.Error == nil {
			// Müşteriyi bekleme listesine ekle
			newWaitList := WaitList{
				ProductID: req.ProductID,
				UserID:    req.UserID,
				CreatedAt: time.Now(),
			}
			DB.Create(&newWaitList)

			// Eğer hata yoksa (yani kayıt bulunduysa), ürün başkasında demektir
			c.JSON(http.StatusConflict, gin.H{
				"error": "Şu anda başka bir kullanıcı ödeme aşamasında.",
				"mesaj": "Ürün tekrar açılırsa sizi otomatik bilgilendireceğiz.",
			})

			return
		}

		// Yeni rezervasyon oluşturma
		// Ürün boşta, kullanıcıya 1 dakikalık süre veriyoruz
		newReservation := Reservation{
			ProductID: req.ProductID,
			UserID:    req.UserID,
			ExpiresAt: time.Now().Add(1 * time.Minute),
			Status:    "active",
		}

		// Veritabanına kaydet (Veritabanı hakemlik yapacak)
		if err := DB.Create(&newReservation).Error; err != nil {
			// Eğer veritabanı unique constraint hatası fırlatırsa bu kod devreye girecek
			c.JSON(http.StatusConflict, gin.H{"error": "Bu ürün tam şu an başka bir kullanıcı tarafından rezerve edildi."})
			return
		}

		// Başarılı dönüş
		c.JSON(http.StatusOK, gin.H{
			"mesaj":          "Ürün sizin için 1 dakikalığına rezerve edildi. Lütfen ödemeyi tamamlayın.",
			"reservation_id": newReservation.ID,
			"expires_at":     newReservation.ExpiresAt,
		})
	})

	// Ödeme ve Event Fırlatma
	r.POST("/payment", func(c *gin.Context) {
		var req PaymentRequest

		// Validasyon
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Eksik veri (Reservation ID ve User ID gerekli)"})
			return
		}

		// Tüm veritabanı işlemlerini (okkuma ve yazma) sigortanın içine alıyoruz
		result, err := CB.Execute(func() (interface{}, error) {
			var reservation Reservation

			// Veritabanında okuma işlemi
			errDB := DB.Where("id = ? AND user_id = ? AND status = ?", req.ReservationID, req.UserID, "active").First(&reservation).Error

			if errDB != nil {
				// Eğer hata "Kayıt yok" ise müşteri hatasıdır. Sigortayı artırma, dışarıya bildir
				if errDB == gorm.ErrRecordNotFound {
					return "NOT_FOUND", nil

				}
				// Eğer hata veritabanı bağlantısı kopması ise, sigortayı artır
				return nil, errDB

			}

			// Süre kontrolü
			if time.Now().After(reservation.ExpiresAt) {
				return "EXPIRED", nil

			}

			// Sipariş verilerini hazırlama
			orderData := OrderRequest{
				UserID:    reservation.UserID,
				ProductID: reservation.ProductID,
				Quantity:  1,
				Price:     req.Price,
			}
			orderJSON, _ := json.Marshal(orderData)
			aggregateID := fmt.Sprintf("%s-%d", reservation.ProductID, time.Now().Unix())

			newEvent := Event{
				AggregateID: aggregateID,
				EventType:   "OrderCreated",
				Payload:     string(orderJSON),
				CreatedAt:   time.Now(),
			}

			kafkaMessage := fmt.Sprintf(`{"EventType: "OrderCreated", "Payload": %q}`, string(orderJSON))
			OutboxEvent := OutboxEvent{
				AggregateID: aggregateID,
				Payload:     kafkaMessage,
				CreatedAt:   time.Now(),
			}

			// Veritabanına yazma işlemi (Transaction)
			errTx := DB.Transaction(func(tx *gorm.DB) error {
				reservation.Status = "completed"
				if err := tx.Save(&reservation).Error; err != nil {
					return err
				}
				if err := tx.Create(&newEvent).Error; err != nil {
					return err
				}
				if err := tx.Create(&OutboxEvent).Error; err != nil {
					return err
				}
				return nil

			})

			if errTx != nil {
				return nil, errTx // Yazma hatası da sigortayı attırır

			}

			return newEvent, nil // Her şey başarılı

		})

		// Sigortadan çıkan sonuçları kullanıcıya dönme
		// Sigorta attıysa veya DB çöktüyse
		if err == gobreaker.ErrOpenState || err == gobreaker.ErrTooManyRequests {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "Sistem şu an aşırı yoğun veya veritabanı kapalı."})
			return

		}

		// İş mantığı hataları (Kayıt yok, süre dolmmuş)
		if result == "NOT_FOUND" {
			c.JSON(http.StatusNotFound, gin.H{"error": "Geçerli bir rezervasyon bulunamadı."})
			return

		} else if result == "EXPIRED" {
			c.JSON(http.StatusGone, gin.H{"error": "Rezervasyon süreniz doldu. İşlem iptal edildi."})
			return

		}

		// Tam başarı
		c.JSON(http.StatusOK, gin.H{
			"mesaj": "Ödeme başarıyla alındı. Siparişiniz güvenli kuyruğa eklendi.",
			"event": result,
		})

	})

	// Redis Cache destekli ürün görüntüleme API si
	r.GET("/product/:id", func(c *gin.Context) {
		productID := c.Param("id")
		cacheKey := "product:" + productID

		// 1. Adım: Önce Redise sor hafızada böyle bir veri var mı diye
		val, err := RDB.Get(ctx, cacheKey).Result()

		if err == redis.Nil {
			// Cache miss: Rediste yok. PostgresSQLe gitmek zorundayız
			log.Printf("[Cache Miss] Veritabanına gidiliyor. Ürün: %s\n", productID)

			// Veritabanı gecikmesini simüle etmek için 1 saniye bilerek bekletiyorum
			time.Sleep(1 * time.Second)

			// Sahte ürün üretiyorum (Burası sonradan değiştirilecek)
			productData := gin.H{
				"id":    productID,
				"name":  "Premium " + productID,
				"price": 8500.0,
				"stock": 100,
			}

			// Veriyi bulduk, JSON a çevirip Redise yazıyoruz (TTL: 60 dakika)
			// Artık 60 dakika boyunca kim sorarsa sorsun veritabanı yorulmaycak
			jsonData, _ := json.Marshal(productData)
			RDB.Set(ctx, cacheKey, jsonData, 60*time.Minute)

			c.JSON(http.StatusOK, gin.H{
				"kaynak": "PostgresSQL (Veritabanı) - Yavaş",
				"data":   productData,
			})
			return

		} else if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Redis hatası"})
			return

		}

		// Veri Rediste varsa veritabanına hiç gitmeden hemen geri dönüyor
		var cacheData map[string]interface{}
		json.Unmarshal([]byte(val), &cacheData)

		c.JSON(http.StatusOK, gin.H{
			"kaynak": "Redis Cache - Çok Hızlı",
			"data":   cacheData,
		})
	})

	// Sunucuyu çalıştır
	r.Run(":8081")
}
