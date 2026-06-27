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
	EventType string
	Payload   string
}

// Olayın içindeki detayları okumak için (hem ürün ID hem de kullanıcı ID lazım)
type EventPayload struct {
	ProductID string `json:"product_id"`
	UserID    string `json:"user_id"`
}

// Bekleme listesi şablonu (Gorm un bu tabloyu okuyabilmesi için)
type WaitList struct {
	ID        uint   `gorm:"primaryKey"`
	ProductID string `gorm:"index"`
	UserID    string
}

var NotificationDB *gorm.DB

func initNotificationDB() {
	dsn := "host=127.0.0.1 user=root password=rootpassword dbname=paribu_db port=5433 sslmode=disable"
	var err error
	NotificationDB, err = gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatal("Bildirim veritabanına bağlanılamadı", err)

	}
	log.Println("Bildirim işçisi veritabanına bağlandı.")

}

func main() {
	initNotificationDB()

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "order-events",
		GroupID: "notification-group", // Analiz işçisinden farklı bir grup adı

	})

	log.Println("Bildirim işçisi Redpanda yı dinlemeye başladı...")

	for {
		m, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Printf("Mesaj okuma hatası: %v", err)
			continue

		}

		var event IncomingEvent
		json.Unmarshal(m.Value, &event)

		// Json metnini alıp payload değişkenine atadık
		var payload EventPayload
		json.Unmarshal([]byte(event.Payload), &payload)

		// Bildirim Matrisi
		switch event.EventType {

		case "OrderCreated":
			// Klasik operasyonel olay
			fmt.Printf("[Bildirim - %s] Siparişiniz başarıyla oluşturuldu.\n", payload.UserID)

		case "ProductAvailableAgain":
			// Bekleme listesindekileri uyar
			var waitingUsers []WaitList

			// Bu ürünü bekleyen herkesi veritabanına çek
			NotificationDB.Where("product_id = ?", payload.ProductID).Find(&waitingUsers)

			if len(waitingUsers) > 0 {
				fmt.Printf("\n [%s] İçin bekleyenlere stok uyarısı gidiyor.\n", payload.ProductID)

				for _, user := range waitingUsers {
					// Müşteriye bildirimi gönder
					fmt.Printf("[Push bildirim - %s] Hızlı olan kazanır! Göz attığınız %s az önce sepetten düştü. Hemen tıklayın.\n", user.UserID, payload.ProductID)

					// Mesajı aldı, artık o kullanıcıyı listeden sil ki aynı mesaj sürekli gitmesin
					NotificationDB.Delete(&user)

				}
			}

			// İleride buraya "OrderShipped" ve "ReservationExpired" gibi diğer olayları da ekleyecem.
		}
	}
}
