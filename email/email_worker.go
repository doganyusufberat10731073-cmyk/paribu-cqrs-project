package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

// Dışarıdan gelicek olayın şablonu
type IncomingEvent struct {
	EventType string
	Payload   string
}

// Olayın içindeki detaylar (Payload)
// Not: Sipariş fiyatını ana API den alıyoruz. ileride kargo modülü yazıldığında
// TrackingNo ve Carrier bilgileri de Redpanda üzerinden buraya gönderilecektir.
type EventPayload struct {
	ProductID  string  `json:"product_id"`
	UserID     string  `json:"user_id"`
	Price      float64 `json:"price"`
	TrackingNo string  `json:"tracking_no"` // Gelecekde buraları düzenliyecez
	Carrier    string  `json:"carrier"`     // Aynı şekilde burasını da düzenliyecez (Hazırlık)
}

func main() {
	// Redpanda (Kafka) bağlantısı
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "order-events",
		GroupID: "email-group", // Diğer consumerlerdan farklı bir grup
	})

	log.Println("E-posta işçisi Redpandayı dinlemeye başladı...")

	for {
		m, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Printf("Mesaj okuma hatası: %v", err)
			continue
		}

		// Redpandadaki mesajı Unmarshal ile çözümlüyoruz
		var event IncomingEvent
		json.Unmarshal(m.Value, &event)

		var payload EventPayload
		json.Unmarshal([]byte(event.Payload), &payload)

		// Sipariş numarasını Redpanda mesajını Key inden (AggregateID) alıyoruz
		orderNo := string(m.Key)

		// Tahmini teslimat tarihini bugunden 3 gün sonrası olarak otomatik hesaplıyoruz
		deliveryDate := time.Now().AddDate(0, 0, 3).Format("02.01.2006")

		// E-Posta Matrisi (SMTP Şablonlama)
		switch event.EventType {

		case "OrderCreated":
			// 1. Sipariş Onay E-postası
			fmt.Println("YENİ E-POSTA GÖNDERİLDİ")
			fmt.Printf("Kime: %s@gmail.com\n", payload.UserID)
			fmt.Println("Konu: Siparişiniz başarıyla oluşturuldu")
			fmt.Printf("Merhaba %s,\n\n", payload.UserID)
			fmt.Printf("\nSipariş No: %s\n", orderNo)
			fmt.Printf("Ürün:\n- %s\n", payload.ProductID)
			fmt.Printf("\nToplam Tutar: %.2f TL\n", payload.Price)
			fmt.Printf("Tahmini Teslimat:\n%s\n", deliveryDate)

		case "OrderShipped":
			// 2. Kargo E-postası
			// Geçici olarak statik veri basıyoruz. İleride kargo API sinden dolacak
			if payload.TrackingNo == "" {
				payload.TrackingNo = "TRX123456"
				payload.Carrier = "Trendyol Express"
			}

			fmt.Println("YENİ E-POSTA GÖNDERİLDİ")
			fmt.Printf("Kime: %s@gmail.com\n", payload.UserID)
			fmt.Println("Konu: Siparişiniz kargoya verildi")
			fmt.Printf("Siparişiniz kargoya verildi.\n\n")
			fmt.Printf("Takip No:\n%s\n", payload.TrackingNo)
			fmt.Printf("Kargo Firması: %s\n", payload.Carrier)

		case "OrderDelivered":
			// 3. Teslimat E-postası
			fmt.Println("YENİ E-POSTA GÖNDERİLDİ")
			fmt.Printf("Kime: %s@gmail.com\n", payload.UserID)
			fmt.Println("Konu: Siparişiniz teslim edildi")
			fmt.Printf("Siparişiniz teslim edildi.\n\n")
			fmt.Println("Hizmetimizden memnun kaldıysanız ürünü değerlendirebilirsiniz.")
		}
	}
}
