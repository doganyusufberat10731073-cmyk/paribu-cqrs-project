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

// 1. Okuma tablosu: Ürün stok / Satış durumu
type ProduceStock struct {
	ProductID string `gorm:"primaryKey"`
	TotalSold int    // Bu urunden toplam kaç adet satıldığını tutacaz
}

// Dış kutu şablonu
type KafkaEvent struct {
	Payload string `json:"Payload"`
}

// 2. Gelen pakaeti açma şablonu (İşimize yarayanları tutacaz)
type OrderEventPayload struct {
	ProductID string `json:"product_id"`
	Quantity  int    `json:"quantity"`
}

func main() {
	// 3. Veritabanına bağlan
	dsn := "host=localhost user=root password=rootpassword dbname=paribu_db port=5432 sslmode=disable"
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatal("Veritabanına bağlanılamadı", err)
	}

	db.AutoMigrate(&ProduceStock{})
	log.Println("Stok İşçisi: Veritabanı hazır, 'product_stocks' tablosu oluşturuldu")

	// 4. Redpandayı dinle
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "order-events",

		// Her işçinin yaka kartı (groupID) farklı olmalıdır.
		// Yoksa postacı mesajları iki işçiye paylaştırır, ikiside eksik veri alır.
		GroupID:  "inventory-workers",
		MinBytes: 10e3,
		MaxBytes: 10e6,
	})

	log.Println("Stok İşçisi: Redpanda dinleniyor...")

	for {

		m, err := r.ReadMessage(context.Background())
		if err != nil {
			break
		}

		var kEvent KafkaEvent
		json.Unmarshal(m.Value, &kEvent)

		var payload OrderEventPayload
		json.Unmarshal([]byte(kEvent.Payload), &payload)

		// 5. Stok hesaplama ve upsert (IDEMPOTENCY)
		// Veritabanına "bu ürünü bul, daha önce satıldıysa yeni satılan miktarı üstüne ekle" diyoruz.
		newStock := ProduceStock{
			ProductID: payload.ProductID,
			TotalSold: payload.Quantity,
		}

		db.Clauses(clause.OnConflict{
			Columns: []clause.Column{{Name: "product_id"}}, // Çakışmayı ürün ID sinden anla.
			DoUpdates: clause.Assignments(map[string]interface{}{
				// Eğer ürün zaten varsa, eski 'total_sold' değerini yeni 'Quantity' yi ekle.
				"total_sold": gorm.Expr("\"produce_stocks\".total_sold + EXCLUDED.total_sold"),
			}),
		}).Create(&newStock)

		log.Printf("Stok Güncellendi: %s Ürününden %d adet daha satıldı (Toplam güncellendi)\n---\n", payload.ProductID, payload.Quantity)

	}
}
