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
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"

	"github.com/aegis-banking/ledger-core/internal/observability"
	pb "github.com/aegis-banking/ledger-core/internal/pb"
	"github.com/aegis-banking/ledger-core/internal/queue"
	"github.com/aegis-banking/ledger-core/internal/repository"
	"github.com/aegis-banking/ledger-core/internal/service"
	"github.com/aegis-banking/ledger-core/internal/worker"
)

type Config struct {
	DBHost       string
	DBPort       string
	DBUser       string
	DBPassword   string
	DBName       string
	GRPCPort     string
	KafkaBrokers string
	OTELEndpoint string
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

	// Initialize OpenTelemetry Tracing
	shutdown, err := observability.InitTracer("aegis-ledger", cfg.OTELEndpoint)
	if err != nil {
		log.Printf("Warning: Failed to initialize tracer: %v", err)
	} else {
		defer shutdown()
		log.Println("✅ OpenTelemetry tracing initialized")
	}

	// Start Prometheus metrics endpoint
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		log.Println("📊 Prometheus metrics available at :2113/metrics")
		if err := http.ListenAndServe(":2113", nil); err != nil {
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

	producer, err := queue.NewKafkaProducer(cfg.KafkaBrokers)
	if err != nil {
		log.Printf("Failed to connect to Kafka: %v", err)
	} else {
		log.Printf("Connected to Kafka")
		defer producer.Close()
	}

	repo := repository.NewAccountRepository(db)
	ledgerSvc := service.NewLedgerService(repo, producer)

	lis, err := net.Listen("tcp", ":"+cfg.GRPCPort)
	if err != nil {
		log.Fatalf("Failed to listen :%v", err)
	}

	// Create gRPC server with OpenTelemetry stats handler
	grpcServer := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
	)
	pb.RegisterLedgerServiceServer(grpcServer, ledgerSvc)

	// Create a cancellable context for background workers (outbox relay)
	ctx, cancel := context.WithCancel(context.Background())

	if producer != nil {
		relay := worker.NewOutboxRelay(db, producer)
		go relay.Start(ctx)
	}

	// Periodically report DB connection pool size to Prometheus
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				observability.DatabaseConnectionPoolSize.Set(float64(db.Stats().OpenConnections))
			}
		}
	}()

	log.Printf("Ledger gRPC Server started on :%s", cfg.GRPCPort)
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("gRPC server failed: %v", err)
		}
	}()

	waitForShutdown(grpcServer, producer, cancel)
}

func loadConfig() Config {
	return Config{
		DBHost:       getEnv("DB_HOST", "aegis-db"),
		DBPort:       getEnv("DB_PORT", "5432"),
		DBUser:       getEnv("DB_USER", "user"),
		DBPassword:   getEnv("DB_PASSWORD", "password"),
		DBName:       getEnv("DB_NAME", "aegis_db"),
		GRPCPort:     getEnv("GRPC_PORT", "50051"),
		KafkaBrokers: getEnv("KAFKA_BROKERS", "kafka:9092"),
		OTELEndpoint: getEnv("OTEL_EXPORTER_OTLP_ENDPOINT", "otel-collector:4317"),
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

func waitForShutdown(grpcServer *grpc.Server, producer *queue.KafkaProducer, cancel context.CancelFunc) {
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	<-quit
	log.Println("Shutting down gRPC server")

	if cancel != nil {
		cancel()
	}

	if producer != nil {
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
	case <-done:
		log.Println("gRPC server stopped gracefully")
	case <-ctx.Done():
		log.Println("Timeout: forcing gRPC server stop")
		grpcServer.Stop()
	}
}