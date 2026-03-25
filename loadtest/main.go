package main

import (
	"bytes"
	"fmt"
	"math/rand"
	"net/http"
	"sync"
	"time"
)

func main() {
	toplamIstek := 100000000 // Göndereceğimiz toplam sipariş sayısı
	ayniAnda := 100          // Aynı anda çalışacak robot sayısı

	fmt.Printf("%d Sipaiş fırlatılacak\n", toplamIstek)
	baslangic := time.Now()

	var wg sync.WaitGroup
	isler := make(chan int, toplamIstek)

	// Özel bir HTTP İstemcisi: Bağlantıları koparmadan art arda gönderöek için
	client := &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 100,
		},
		Timeout: 10 * time.Second,
	}

	// Robotları çalıştırıyoruz
	for i := 1; i <= ayniAnda; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range isler {
				// Her siparişe rastgele bir ID ve rastgele bir ürün atıyoruz
				urunler := []string{"iphone-15", "macbook-pro", "oyuncu-koltugu", "oyuncu-klavyesi"}
				secilenUrun := urunler[rand.Intn(len(urunler))]

				jsonVeri := fmt.Sprintf(`{"id":"stres-test-%d", "product_id":"%s", "quantity":1, "price":5000.0}`, j, secilenUrun)

				req, _ := http.NewRequest("POST", "http://localhost:8081/order", bytes.NewBuffer([]byte(jsonVeri)))
				req.Header.Set("Content-Type", "application/json")

				// İsteği gönder
				resp, err := client.Do(req)
				if err == nil {
					resp.Body.Close()

				}
			}
		}()

	}

	// Robotların önüne 100.000 adet iş koyuyoruz
	for i := 1; i <= toplamIstek; i++ {
		isler <- i
	}
	close(isler)

	// Tüm robotların işini bitirmesini bekle
	wg.Wait()
	gecenSure := time.Since(baslangic).Seconds()

	fmt.Println("Tüm işlemler bitti")
	fmt.Printf("%d sipariş tam %.2f saniyede işlendi\n", toplamIstek, gecenSure)
	fmt.Printf("Saniyede ortalama %d sipariş API den geçti\n", int(float64(toplamIstek)/gecenSure))
}
