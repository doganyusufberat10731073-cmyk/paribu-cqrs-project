package main

import (
	"bytes"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	toplamIstek := 100000000 // Göndereceğimiz toplam sipariş sayısı
	ayniAnda := 150          // Aynı anda çalışacak robot sayısı

	var basarili int64
	var basarisiz int64

	fmt.Printf("%d Sipaiş, %d seçkin işçi (Goroutine) ile fırlatılıyor...\n", toplamIstek, ayniAnda)
	baslangic := time.Now()

	var wg sync.WaitGroup

	istekBasinaIsci := toplamIstek / ayniAnda

	// 150 tane işçi oluşturuyoruz
	for i := 0; i < ayniAnda; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			// Timeout yani sunucu cevap vermezse askıda kalıp RAM yemesin
			client := &http.Client{Timeout: 10 * time.Second}

			for j := 0; j < istekBasinaIsci; j++ {
				payload := []byte(`{"id": "stres-test","product_id":"iphone-15","quantity":1, "price": 5000}`)
				req, _ := http.NewRequest("POST", "http://localhost:8081/order", bytes.NewBuffer(payload))
				req.Header.Set("Content-Type", "application/json")

				resp, err := client.Do(req)
				if err != nil {
					atomic.AddInt64(&basarisiz, 1)
					continue
				}

				if resp.StatusCode >= 200 && resp.StatusCode < 300 {
					atomic.AddInt64(&basarili, 1)

				} else {
					atomic.AddInt64(&basarisiz, 1)

				}
				resp.Body.Close()
			}
		}()
	}

	// Mtrix ekranı için izleyici (Her 5 saniyede bir log basar)
	go func() {
		for {
			time.Sleep(5 * time.Second)
			suAnkiBasarili := atomic.LoadInt64(&basarili)
			suAnkiBasarisiz := atomic.LoadInt64(&basarisiz)
			gecenSure := time.Since(baslangic).Seconds()
			tps := float64(suAnkiBasarili+suAnkiBasarisiz) / gecenSure

			fmt.Printf("Süre: %v | Başarılı: %d | Başarısız: %d | TPS: %.0f/sn\n",
				time.Since(baslangic).Round(time.Second), suAnkiBasarili, suAnkiBasarisiz, tps)

		}
	}()

	// Tüm robotların işini bitirmesini bekle
	wg.Wait()
	gecenSure := time.Since(baslangic).Seconds()

	fmt.Println("Tüm işlemler bitti")
	fmt.Printf("%d sipariş tam %.2f saniyede işlendi\n", toplamIstek, gecenSure)
	fmt.Printf("Saniyede ortalama %d sipariş API den geçti\n", int(float64(toplamIstek)/gecenSure))
}
