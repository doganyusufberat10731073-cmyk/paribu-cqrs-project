package main

import (
	"context"
	"encoding/json"
	"log"

	"github.com/segmentio/kafka-go"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// 1. Okuma tablosu
// Müşterinin siparişlerim sayfasına girdiğinde göreceği, okuması kolay olan tablo
type OrderSummary struct {
	OrderID     string `gorm:"primaryKey"`
	ProductID   string
	Quantity    int
	TotalPrice  float64
	OrderStatus string // Örn: "Hazırlanıyor"
}

// Dış kutu (event) şablonu
type KafkaEvent struct {
	AggregateID string `json:"AggregateID"`
	Payload     string `json:"Payload"` // Asıl siparişimiz bunun içinde gizli
}

// 2. Gelen paketi açma şablonu
// Redpanda dan gelen JSON paketinin içindekileri okuyabilmek için kullandığım şablon
type OrderEventPayload struct {
	ID        string  `json:"id"`
	ProductID string  `json:"product_id"`
	Quantity  int     `json:"quantity"`
	Price     float64 `json:"price"`
}

func main() {
	// 3. Veritabanına bağlan
	dsn := "host=localhost user=root password=rootpassword dbname=paribu_db port=5432 sslmode=disable"
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatal("Veritabanına bağlanılmadı", err)
	}

	// İşçi çalışır çalışmaz veritabanında "order_summearies" adında yeni bir tablo açacak
	db.AutoMigrate(&OrderSummary{})
	log.Println("Sipariş Özeti İşçisi: Veritabanı hazır, order_summaries tablosu oluşturuldu")

	// 4. Redpandayı dinle
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{"localhost:9092"},
		Topic:    "order-events",    // Dinlediğimiz megafonun adı
		GroupID:  "summary-workers", // Bizim işçi grubunun yaka kartı
		MinBytes: 10e3,              // 10KB
		MaxBytes: 10e6,              // 10MB
	})

	log.Println("Sipariş Özeti İşçisi: Redpanda 7/24 dinleniyor...")

	// 5. Sonsuz mesai dögüsü
	for {
		// Megagondan ses (mesaj) gelmesini bekle
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			log.Println("Mesaj okunurken hata:", err)
			break
		}

		// Önce dış kutuyu açıyoruz
		var kEvent KafkaEvent
		json.Unmarshal(m.Value, &kEvent)

		// Şimdi içindeki 'payload' bölmesini açıp siparişi alıyoruz
		var payload OrderEventPayload
		json.Unmarshal([]byte(kEvent.Payload), &payload)

		log.Printf("Bir mesaj var. Sipariş ID: %s\n", string(m.Key))

		// 6. Okuma tablosuna yazılacak veriyi hazırlayalım
		summary := OrderSummary{
			OrderID:     payload.ID,
			ProductID:   payload.ProductID,
			Quantity:    payload.Quantity,
			TotalPrice:  payload.Price * float64(payload.Quantity), // Adet x fiyat ile toplamı hesapladık
			OrderStatus: "Sipariş Alındı",
		}

		// 7. Veritabanına upsert (idempotency) ile kaydet
		// Eğer OrderID zaetn varsa hata verme, sadece bilgileri güncelle
		db.Clauses(clause.OnConflict{
			UpdateAll: true,
		}).Create(&summary)
		log.Println("İş Tamam: Sipariş, okuma tablosuna (order_summaries) başarıyla yazıldı \n---")
	}
}
