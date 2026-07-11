package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"

	"github.com/aegis-banking/fraud-service/internal/consumer"
	"github.com/aegis-banking/fraud-service/internal/grpcclient"
	fraudpb "github.com/aegis-banking/fraud-service/internal/pb/fraud"
	"github.com/aegis-banking/fraud-service/internal/repository"
	"github.com/aegis-banking/fraud-service/internal/service"
)

type Config struct {
	DBHost         string
	DBPort         string
	DBUser         string
	DBPassword     string
	DBName         string
	GRPCPort       string
	MetricsPort    string
	KafkaBrokers   string
	LedgerGRPCAddr string
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
		log.Printf("Prometheus metrics available at :%s/metrics", cfg.MetricsPort)
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

	checksRepo := repository.NewChecksRepository(db)
	fraudSvc := service.NewFraudService(checksRepo)

	lis, err := net.Listen("tcp", ":"+cfg.GRPCPort)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	fraudpb.RegisterFraudServiceServer(grpcServer, fraudSvc)

	log.Printf("Fraud gRPC Server started on :%s", cfg.GRPCPort)
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("gRPC server failed: %v", err)
		}
	}()

	ledgerClient, err := grpcclient.NewLedgerClient(cfg.LedgerGRPCAddr)
	var consumerCtx context.Context
	var consumerCancel context.CancelFunc
	if err != nil {
		log.Printf("Failed to create ledger-core client, async re-scoring disabled: %v", err)
	} else {
		defer ledgerClient.Close()
		scoredRepo := repository.NewScoredRepository(db)
		reversalsRepo := repository.NewReversalsRepository(db)
		kafkaConsumer := consumer.New(cfg.KafkaBrokers, scoredRepo, reversalsRepo, ledgerClient)
		defer kafkaConsumer.Close()

		consumerCtx, consumerCancel = context.WithCancel(context.Background())
		go kafkaConsumer.Start(consumerCtx)
		log.Println("Async re-score consumer started (topic: audit_events)")
	}

	waitForShutdown(grpcServer, consumerCancel)
}

func loadConfig() Config {
	return Config{
		DBHost:         getEnv("DB_HOST", "fraud-db"),
		DBPort:         getEnv("DB_PORT", "5432"),
		DBUser:         getEnv("DB_USER", "user"),
		DBPassword:     getEnv("DB_PASSWORD", "password"),
		DBName:         getEnv("DB_NAME", "fraud_db"),
		GRPCPort:       getEnv("GRPC_PORT", "50053"),
		MetricsPort:    getEnv("METRICS_PORT", "2116"),
		KafkaBrokers:   getEnv("KAFKA_BROKERS", "kafka:9092"),
		LedgerGRPCAddr: getEnv("LEDGER_GRPC_ADDR", "ledger-core:50051"),
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

func waitForShutdown(grpcServer *grpc.Server, consumerCancel context.CancelFunc) {
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	<-quit
	log.Println("Shutting down gRPC server")

	if consumerCancel != nil {
		consumerCancel()
	}

	done := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(done)
	}()

	select {
	case <-done:
		log.Println("gRPC server stopped gracefully")
	case <-time.After(10 * time.Second):
		log.Println("Timeout: forcing gRPC server stop")
		grpcServer.Stop()
	}
}
