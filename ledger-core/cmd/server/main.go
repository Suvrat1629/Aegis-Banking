package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
	"google.golang.org/grpc"

	pb "github.com/aegis-banking/ledger-core/internal/pb"
	"github.com/aegis-banking/ledger-core/internal/queue"
	"github.com/aegis-banking/ledger-core/internal/repository"
	"github.com/aegis-banking/ledger-core/internal/service"
)

type Config struct {
	DBHost      string
	DBPort      string
	DBUser      string
	DBPassword  string
	DBName      string
	GRPCPort    string
	RabbitMQURL string
}

func main() {

	if err := godotenv.Load(); err != nil {
		log.Println("No .env was found")
	}

	cfg := loadConfig()

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

	producer, err := queue.NewRabbitMQProducer(cfg.RabbitMQURL)
	if err != nil {
		log.Printf("Failed to connect to RabbitMQ: %v", err)
	} else {
		log.Printf("Connected to RabbitMQ")
		defer producer.Close()
	}

	repo := repository.NewAccountRepository(db)
	ledgerSvc := service.NewLedgerService(repo, producer)

	lis, err := net.Listen("tcp", ":" + cfg.GRPCPort)
	if err != nil {
		log.Fatalf("Failed to listen :%v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterLedgerServiceServer(grpcServer, ledgerSvc)

	log.Println("Ledger gRPC Server started on :50051")
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("gRPC server failed: %v", err)
	}

	waitForShutdown(grpcServer, producer)
}

func loadConfig() Config {
	return Config{
		DBHost:     getEnv("DB_HOST", "aegis-db"),
		DBPort:     getEnv("DB_PORT", "5432"),
		DBUser:     getEnv("DB_USER", "user"),
		DBPassword: getEnv("DB_PASSWORD", "password"), // still fallback for local dev
		DBName:     getEnv("DB_NAME", "aegis_db"),
		GRPCPort:   getEnv("GRPC_PORT", "50051"),
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

func waitForShutdown(grpcServer *grpc.Server, producer *queue.RabbitMQProducer) {
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	<-quit
	log.Println("Shutting down gRPC server")

	if producer != nil{
		producer.Close()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	done := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(done)
	}()

	select {
	case <- done:
		log.Println("gRPC server stopped gracefully")
	case <-ctx.Done():
		log.Println("Timeout: forcing gRPC server stop")
		grpcServer.Stop()
	}
}