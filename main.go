package main

import (
	"database/sql"
	"fmt"
	"net/http"
	"strconv"

	"context"
	"encoding/json"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis/v8"
	_ "github.com/lib/pq"
)

var db *sql.DB
var rdb *redis.Client

func main() {
	// Connect to PostgreSQL
	pgConnStr := "postgres://postgres:1234@localhost/products?sslmode=disable"
	var err error
	db, err = sql.Open("postgres", pgConnStr)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// Connect to Redis Cloud
	rdb = redis.NewClient(&redis.Options{
		Addr:     "redis-14431.c74.us-east-1-4.ec2.redns.redis-cloud.com:14431",
		Password: "1234", // Your Redis Cloud password
		DB:       0,      // use default DB
	})
	defer rdb.Close()

	// Insert sample products into the database
	if err := insertSampleProducts(); err != nil {
		panic(err)
	}

	// Initialize Gin router
	router := gin.Default()

	// Define routes
	router.GET("/products/:id", getProduct)

	// Start the server
	router.Run(":8080")
}

func getProduct(c *gin.Context) {
	// Get product ID from URL
	idStr := c.Param("id")
	id, err := strconv.Atoi(idStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid product ID"})
		return
	}

	// Check Redis cache first
	product, err := getProductFromCache(id)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Internal server error"})
		return
	}

	if product != nil {
		c.JSON(http.StatusOK, product)
		return
	}

	// If not found in cache, fetch from database
	product, err = getProductFromDB(id)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Internal server error"})
		return
	}

	if product == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Product not found"})
		return
	}

	// Set product in Redis cache
	err = setProductInCache(product)
	if err != nil {
		// Handle error
		fmt.Println("Error setting product in cache:", err)
	}

	c.JSON(http.StatusOK, product)
}

func getProductFromCache(id int) (*Product, error) {
	ctx := context.Background()
	data, err := rdb.Get(ctx, strconv.Itoa(id)).Result()
	if err == redis.Nil {
		return nil, nil // Cache miss
	} else if err != nil {
		return nil, err // Redis error
	}

	var product Product
	if err := json.Unmarshal([]byte(data), &product); err != nil {
		return nil, err // JSON unmarshal error
	}

	return &product, nil
}

func getProductFromDB(id int) (*Product, error) {
	row := db.QueryRow("SELECT id, name, description, price FROM products WHERE id = $1", id)
	product := &Product{}
	err := row.Scan(&product.ID, &product.Name, &product.Description, &product.Price)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil // Product not found
		}
		return nil, err
	}
	return product, nil
}

func setProductInCache(product *Product) error {
	ctx := context.Background()
	jsonData, err := json.Marshal(product)
	if err != nil {
		return err
	}
	err = rdb.Set(ctx, strconv.Itoa(product.ID), jsonData, 24*time.Hour).Err()
	if err != nil {
		return err
	}
	return nil
}

func insertSampleProducts() error {
	// Insert sample products into the database
	_, err := db.Exec(`
        INSERT INTO products (id, name, description, price) VALUES
            (1, 'Sample Product 1', 'Description of Sample Product 1', 19.99),
            (2, 'Sample Product 2', 'Description of Sample Product 2', 29.99),
            (3, 'Sample Product 3', 'Description of Sample Product 3', 39.99);
    `)
	if err != nil {
		return err
	}
	return nil
}

// Product struct
type Product struct {
	ID          int     `json:"id"`
	Name        string  `json:"name"`
	Description string  `json:"description"`
	Price       float64 `json:"price"`
}
