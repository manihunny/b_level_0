package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"
	_ "github.com/lib/pq"
	"github.com/jmoiron/sqlx"
)

const dbConnStr = "user=postgres password=RootPass dbname=level_0 sslmode=disable"

type Order struct {
	OrderUID    		string      `json:"order_uid" db:"order_uid"`
	TrackNumber 		string    	`json:"track_number" db:"track_number"`
	Entry  				string      `json:"entry" db:"entry"`
	Locale    			string      `json:"locale" db:"locale"`
	InternalSignature 	string    	`json:"internal_signature" db:"internal_signature"`
	CustomerID  		string      `json:"customer_id" db:"customer_id"`
	DeliveryService    	string      `json:"delivery_service" db:"delivery_service"`
	Shardkey 			string    	`json:"shardkey" db:"shardkey"`
	SmID  				int      	`json:"sm_id" db:"sm_id"`
	DateCreated    		time.Time	`json:"date_created" db:"date_created"`
	OofShard 			string    	`json:"oof_shard" db:"oof_shard"`
	Delivery						`json:"delivery"`
	Payment							`json:"payment"`
	Items				[]Item		`json:"items"`
}

type Delivery struct {
	Name 		   string    `json:"name" db:"name"`
	Phone  		   string    `json:"phone" db:"phone"`
	Zip    		   string    `json:"zip" db:"zip"`
	City 		   string    `json:"city" db:"city"`
	Address  	   string    `json:"address" db:"address"`
	Region    	   string    `json:"region" db:"region"`
	Email 		   string    `json:"email" db:"email"`
}

type Payment struct {
	Transaction    string    `json:"transaction" db:"transaction"`
	RequestID  	   string    `json:"request_id" db:"request_id"`
	Currency       string    `json:"currency" db:"currency"`
	Provider 	   string    `json:"provider" db:"provider"`
	Amount  	   int       `json:"amount" db:"amount"`
	PaymentDt      int       `json:"payment_dt" db:"payment_dt"`
	Bank 		   string    `json:"bank" db:"bank"`
	DeliveryCost   int       `json:"delivery_cost" db:"delivery_cost"`
	GoodsTotal     int       `json:"goods_total" db:"goods_total"`
	CustomFee 	   int       `json:"custom_fee" db:"custom_fee"`
}

type Item struct {
	ChrtID 		   int 		 `json:"chrt_id" db:"chrt_id"`
	TrackNumber    string    `json:"track_number" db:"track_number"`
	Price    	   int       `json:"price" db:"price"`
	RID 		   string 	 `json:"rid" db:"rid"`
	Name  		   string    `json:"name" db:"name"`
	Sale    	   int    	 `json:"sale" db:"sale"`
	Size 		   string 	 `json:"size" db:"size"`
	TotalPrice     int   	 `json:"total_price" db:"total_price"`
	NmID    	   int    	 `json:"nm_id" db:"nm_id"`
	Brand 		   string 	 `json:"brand" db:"brand"`
	Status 		   int 		 `json:"status" db:"status"`
}

func main() {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/orders", getAllOrders)
	mux.HandleFunc("GET /api/orders/{order_uid}", getOrderByID)
	mux.HandleFunc("POST /api/orders", createOrder)
	mux.HandleFunc("PUT /api/orders/{id}", updateOrder)
	mux.HandleFunc("DELETE /api/orders/{id}", deleteOrder)

	fmt.Println("Server started on port 80")
	http.ListenAndServe("localhost:80", mux)
}

func getAllOrders(w http.ResponseWriter, r *http.Request) {
	db, err := sqlx.Open("postgres", dbConnStr)
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	defer db.Close()

	var orders []Order
	err = db.Select(&orders, "SELECT * FROM orders LEFT JOIN deliveries ON deliveries.order_uid = orders.order_uid LEFT JOIN payments ON payments.transaction = orders.order_uid")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	for i := range orders {
		err = db.Select(&orders[i].Items, "SELECT * FROM items WHERE track_number = $1", orders[i].TrackNumber)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	err = json.NewEncoder(w).Encode(orders)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func getOrderByID(w http.ResponseWriter, r *http.Request) {
	order_uid := r.PathValue("order_uid")
	
	db, err := sqlx.Open("postgres", dbConnStr)
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	defer db.Close()

	var order Order
	err = db.Get(&order, "SELECT * FROM orders LEFT JOIN deliveries ON deliveries.order_uid = orders.order_uid LEFT JOIN payments ON payments.transaction = orders.order_uid WHERE orders.order_uid = $1", order_uid)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	err = db.Select(&order.Items, "SELECT * FROM items WHERE track_number = $1", order.TrackNumber)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	err = json.NewEncoder(w).Encode(order)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func createOrder(w http.ResponseWriter, r *http.Request) {
	var orderData Order
	err := json.NewDecoder(r.Body).Decode(&orderData)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	db, err := sqlx.Open("postgres", dbConnStr)
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	defer db.Close()

	tx := db.MustBegin()
	defer tx.Rollback()

	_, err = tx.NamedExec(`INSERT INTO orders (order_uid, track_number, entry, locale, internal_signature, customer_id, delivery_service, shardkey, sm_id, date_created, oof_shard) VALUES (:order_uid, :track_number, :entry, :locale, :internal_signature, :customer_id, :delivery_service, :shardkey, :sm_id, :date_created, :oof_shard)`, orderData)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	_, err = tx.Exec(`INSERT INTO deliveries (order_uid, name, phone, zip, city, address, region, email) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`, orderData.OrderUID, orderData.Delivery.Name, orderData.Delivery.Phone, orderData.Delivery.Zip, orderData.Delivery.City, orderData.Delivery.Address, orderData.Delivery.Region, orderData.Delivery.Email)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	_, err = tx.NamedExec(`INSERT INTO payments (transaction, request_id, currency, provider, amount, payment_dt, bank, delivery_cost, goods_total, custom_fee) VALUES (:transaction, :request_id, :currency, :provider, :amount, :payment_dt, :bank, :delivery_cost, :goods_total, :custom_fee)`, orderData.Payment)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	for _, item := range orderData.Items {
		_, err = tx.NamedExec(`INSERT INTO items (chrt_id, track_number, price, rid, name, sale, size, total_price, nm_id, brand, status) VALUES (:chrt_id, :track_number, :price, :rid, :name, :sale, :size, :total_price, :nm_id, :brand, :status)`, item)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	tx.Commit()

	w.WriteHeader(http.StatusCreated)
}

func updateOrder(w http.ResponseWriter, r *http.Request) {
	var orderData Order
	err := json.NewDecoder(r.Body).Decode(&orderData)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	db, err := sqlx.Open("postgres", dbConnStr)
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	defer db.Close()

	tx := db.MustBegin()
	defer tx.Rollback()

	_, err = tx.NamedExec(`UPDATE orders SET order_uid = :order_uid, track_number = :track_number, entry = :entry, locale = :locale, internal_signature = :internal_signature, customer_id = :customer_id, delivery_service = :delivery_service, shardkey = :shardkey, sm_id = :sm_id, date_created = :date_created, oof_shard = :oof_shard WHERE order_uid = :order_uid`, orderData)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	_, err = tx.Exec(`UPDATE deliveries SET order_uid = $1, name = $2, phone = $3, zip = $4, city = $5, address = $6, region = $7, email = $8 WHERE order_uid = $1`, orderData.OrderUID, orderData.Delivery.Name, orderData.Delivery.Phone, orderData.Delivery.Zip, orderData.Delivery.City, orderData.Delivery.Address, orderData.Delivery.Region, orderData.Delivery.Email)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	_, err = tx.NamedExec(`UPDATE payments SET transaction = :transaction, request_id = :request_id, currency = :currency, provider = :provider, amount = :amount, payment_dt = :payment_dt, bank = :bank, delivery_cost = :delivery_cost, goods_total = :goods_total, custom_fee = :custom_fee WHERE transaction = :transaction`, orderData.Payment)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	for _, item := range orderData.Items {
		_, err = tx.NamedExec(`UPDATE items SET chrt_id = :chrt_id, track_number = :track_number, price = :price, rid = :rid, name = :name, sale = :sale, size = :size, total_price = :total_price, nm_id = :nm_id, brand = :brand, status = :status WHERE track_number = :track_number`, item)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	tx.Commit()

	w.WriteHeader(http.StatusCreated)
}

func deleteOrder(w http.ResponseWriter, r *http.Request) {
	orderUID := r.PathValue("id")

	db, err := sqlx.Open("postgres", dbConnStr)
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	defer db.Close()

	_, err = db.Exec(`DELETE FROM orders WHERE order_uid = $1`, orderUID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}