package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/aegis-banking/notification-service/internal/consumer"
	"github.com/aegis-banking/notification-service/internal/mailer"
	"github.com/aegis-banking/notification-service/internal/repository"
)

type Config struct {
	DBHost       string
	DBPort       string
	DBUser       string
	DBPassword   string
	DBName       string
	MetricsPort  string
	KafkaBrokers string
	MailHost     string
	MailPort     string
}

func main() {
	if err := godotenv.Load(); err != nil {
		log.Println("No .env was found in working dir, trying parent dir")
		if err2 := godotenv.Load("../.env"); err2 != nil {
			log.Println("No .env was found in parent dir either; using defaults and environment variables")
		} else {
			log.Println("Loaded .env from parent dir")
		}
	}

	cfg := loadConfig()

	go func() {
		http.Handle("/metrics", promhttp.Handler())
		log.Printf("📊 Prometheus metrics available at :%s/metrics", cfg.MetricsPort)
		if err := http.ListenAndServe(":"+cfg.MetricsPort, nil); err != nil {
			log.Fatalf("Failed to start metrics server: %v", err)
		}
	}()

	dsn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		cfg.DBHost, cfg.DBPort, cfg.DBUser, cfg.DBPassword, cfg.DBName)

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		log.Fatalf("Failed to open DB connection: %v", err)
	}
	defer db.Close()

	if err := waitForDB(db); err != nil {
		log.Fatalf("DB not ready: %v", err)
	}
	log.Println("Connected to PostgreSQL")

	contactsRepo := repository.NewContactsRepository(db)
	notifRepo := repository.NewNotificationRepository(db)
	m := mailer.New(cfg.MailHost, cfg.MailPort)

	c := consumer.New(cfg.KafkaBrokers, contactsRepo, notifRepo, m)
	defer c.Close()

	ctx, cancel := context.WithCancel(context.Background())
	go c.Start(ctx)

	log.Println("Notification service started, consuming account_events + audit_events")

	waitForShutdown(cancel)
}

func loadConfig() Config {
	return Config{
		DBHost:       getEnv("DB_HOST", "notification-db"),
		DBPort:       getEnv("DB_PORT", "5432"),
		DBUser:       getEnv("DB_USER", "user"),
		DBPassword:   getEnv("DB_PASSWORD", "password"),
		DBName:       getEnv("DB_NAME", "notification_db"),
		MetricsPort:  getEnv("METRICS_PORT", "2114"),
		KafkaBrokers: getEnv("KAFKA_BROKERS", "kafka:9092"),
		MailHost:     getEnv("MAIL_HOST", "mailpit"),
		MailPort:     getEnv("MAIL_PORT", "1025"),
	}
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func waitForDB(db *sql.DB) error {
	for i := 0; i < 10; i++ {
		if err := db.Ping(); err == nil {
			return nil
		}
		log.Printf("Waiting for database... attempt %d/10", i+1)
		time.Sleep(2 * time.Second)
	}
	return db.Ping()
}

func waitForShutdown(cancel context.CancelFunc) {
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Shutting down notification service")
	cancel()
}
