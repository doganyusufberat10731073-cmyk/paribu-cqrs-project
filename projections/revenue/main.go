package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// 1. Okuma tablosu: günlük kasa
type DailyRevenue struct {
	Date         string  `gorm:"primaryKey"` // Tarih
	TotalRevenue float64 // O gün kazanılan toplam para
}

// 2. Kutular
type KafkaEvent struct {
	Payload string `json:"Payload"`
}

type OrderEventPayload struct {
	Quantity int     `json:"quantity"`
	Price    float64 `json:"price"`
}

func main() {
	// 3. Veritabanına bağlan
	dsn := "host=localhost user=root password=rootpassword dbname=paribu_db port=5432 sslmode=disable"
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatal("Veritabanına bağlanılamadı", err)
	}

	db.AutoMigrate(&DailyRevenue{})
	log.Println("Ciro İşçisi: Veritabanı hazır, 'daily_revenues' tablosu oluşturuldu")

	// 4. Redpandayı dinle
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{"localhost:9092"},
		Topic:    "order-events",
		GroupID:  "revenue-works", // Yaka kartı yine farklı
		MinBytes: 10e3,
		MaxBytes: 10e6,
	})

	log.Println("Ciro İşçisi: Redpanda dinleniyor...")

	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			break
		}

		// Kutuları aç
		var kEvent KafkaEvent
		json.Unmarshal(m.Value, &kEvent)

		var payload OrderEventPayload
		json.Unmarshal([]byte(kEvent.Payload), &payload)

		// 5. Hesaplama (Adet x Fiyat)
		orderTotal := float64(payload.Quantity) * payload.Price

		// Bugünün tarihini YYYY-MM-DD formatında al
		today := time.Now().Format("2006-01-02")

		// 6. Upsert ile kasaya para ekle (atomic update)
		revenue := DailyRevenue{
			Date:         today,
			TotalRevenue: orderTotal,
		}

		db.Clauses(clause.OnConflict{
			Columns: []clause.Column{{Name: "date"}}, // Çakışmayı tarihten anla
			DoUpdates: clause.Assignments(map[string]interface{}{
				// Kasadaki eski paranın üstüne yeni parayı ekle
				"total_revenue": gorm.Expr("\"daily_revenues\".total_revenue + EXCLUDED.total_revenue"),
			}),
		}).Create(&revenue)

		log.Printf("Kasa Güncellendi: %s tarihinde kasaya %.2f TL eklendi\n---\n", today, orderTotal)
	}
}
