package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/segmentio/kafka-go"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// Dışarıdan gelecek olayın şablonu
type IncomingEvent struct {
	ID          uint
	AggregateID string
	EventType   string // ProductViewed, AddedToCart, CheckoutStarted, OrderCreated
	Payload     string
}

// Olayın içindeki ürün bilgisini okumak için
type EventPayload struct {
	ProductID string  `json:"product_id"`
	Price     float64 `json:"price"`
}

// Analiz tablasu (OLAP)
// Bu tablo sadece yöneticilerin göreceği grafikleri doldurur
type ProductAnalytics struct {
	ProductID string  `gorm:"primaryKey"`
	Views     int     // Görüntüleme
	CartAdds  int     // Sepete ekleme
	Checkouts int     // Ödemeye geçiş
	Sales     int     // Satın alma
	Revenue   float64 // Toplam ciro

}

var AnalyticsDB *gorm.DB

func initAnalyticsDB() {
	dsn := "host=127.0.0.1 user=root password=rootpassword dbname=paribu_db port=5433 sslmode=disable"
	var err error
	AnalyticsDB, err = gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatal("Analiz veritabanına bağlanılamadı.", err)

	}
	AnalyticsDB.AutoMigrate(&ProductAnalytics{})
	log.Println("Analiz işçisi veritabanına bağlandı.")

}

func main() {
	initAnalyticsDB()

	// Redpanda dan okuma yapacak consumer
	reader := kafka.NewReader(kafka.ReaderConfig{ // Kafka readerin ayarlarını tutan yapı
		Brokers: []string{"localhost:9092"}, // Redpanda (kafka) adresi
		Topic:   "order-events",             // Hangi kanalda tutulduğu ayarı
		GroupID: "analytics-group",          // Bu grubun adı, Kafkanın nerede kadlığımızı bilmesini sağlar

	})

	log.Println("Analytics İşçisi Redpanda yı dinlemeye başladı.")

	for {
		m, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Printf("Mesaj okuma hatası: %v", err)
			continue

		}

		var event IncomingEvent
		// Redpanda dan gelen veriyi JSON dan Go formatına çeviriyoruz
		json.Unmarshal(m.Value, &event)

		var payload EventPayload
		json.Unmarshal([]byte(event.Payload), &payload)

		productID := payload.ProductID

		// Analiz tablosunda bu ürünü bul yoksa yeni oluştur
		var stats ProductAnalytics
		AnalyticsDB.FirstOrCreate(&stats, ProductAnalytics{ProductID: productID})

		// Conversion Funnel (Dönüşüm Hunisi)
		switch event.EventType {
		case "ProductViewed":
			stats.Views++
		case "AddedToCart":
			stats.CartAdds++
		case "CheckoutStarted":
			stats.Checkouts++
		case "OrderCreated":
			stats.Sales++
			stats.Revenue += payload.Price

		}

		// Güncel veriyi kaydet
		AnalyticsDB.Save(&stats)

		// Canlı hesaplama ve ekrana yazdırma
		if event.EventType == "OrderCreated" {
			// CR (Dönüşüm Oranı) Hesaplama: (Satış / Görüntüleme) * 100
			conversionRate := 0.0
			if stats.Views > 0 {
				conversionRate = (float64(stats.Sales) / float64(stats.Views)) * 100

			}

			fmt.Printf("[Anlık Analiz  Raporu] Ürün: %s\n", productID)
			fmt.Printf("%d Bakıldı -> %d Sepet -> %d Ödeme -> %d Satış\n", stats.Views, stats.CartAdds, stats.Checkouts, stats.Sales)
			fmt.Printf("Dönüşüm Oranı (CR): %%%.2f\n", conversionRate)
			fmt.Printf("Toplam Gelir: %.2f TL\n", stats.Revenue)
		}
	}
}
