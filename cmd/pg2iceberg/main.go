package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/pg2iceberg/pg2iceberg/api"
	"github.com/pg2iceberg/pg2iceberg/config"
	"github.com/pg2iceberg/pg2iceberg/pipeline"
)

func main() {
	configPath := flag.String("config", "config.example.yaml", "path to config file")
	serverMode := flag.Bool("server", false, "run in multi-tenant API server mode")
	listenAddr := flag.String("listen", ":8080", "API server listen address")
	storeDSN := flag.String("store-url", "", "postgres URL for pipeline store, e.g. postgresql://user:pass@host:5432/db (server mode)")
	storeDir := flag.String("store-dir", "./pipelines", "file-based pipeline store directory (server mode, used if -store-dsn is not set)")

	// ClickHouse auto-provisioning (server mode)
	chAddr := flag.String("clickhouse-addr", "", "ClickHouse HTTP address for auto-provisioning (e.g. http://localhost:8123)")
	chCatalogURI := flag.String("clickhouse-catalog-uri", "", "Iceberg catalog URI as seen by ClickHouse (e.g. http://iceberg-rest:8181/v1)")
	chS3Endpoint := flag.String("clickhouse-s3-endpoint", "", "S3 endpoint as seen by ClickHouse (e.g. http://minio:9000)")
	chS3AccessKey := flag.String("clickhouse-s3-access-key", "", "S3 access key for ClickHouse storage")
	chS3SecretKey := flag.String("clickhouse-s3-secret-key", "", "S3 secret key for ClickHouse storage")
	chWarehouse := flag.String("clickhouse-warehouse", "s3://warehouse/", "warehouse path for ClickHouse catalog")

	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle shutdown signals
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		log.Printf("received %s, shutting down...", sig)
		cancel()
	}()

	if *serverMode {
		chOpts := clickHouseOpts{
			addr:        *chAddr,
			catalogURI:  *chCatalogURI,
			s3Endpoint:  *chS3Endpoint,
			s3AccessKey: *chS3AccessKey,
			s3SecretKey: *chS3SecretKey,
			warehouse:   *chWarehouse,
		}
		runServer(ctx, *listenAddr, *storeDSN, *storeDir, chOpts)
	} else {
		runSingle(ctx, *configPath)
	}
}

func runSingle(ctx context.Context, configPath string) {
	cfg, err := config.Load(configPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	p := pipeline.NewPipeline("default", cfg)
	if err := p.Start(ctx); err != nil {
		log.Fatalf("fatal: %v", err)
	}

	<-p.Done()
	if status, err := p.Status(); status == pipeline.StatusError && err != nil {
		log.Fatalf("fatal: %v", err)
	}
}

type clickHouseOpts struct {
	addr        string
	catalogURI  string
	s3Endpoint  string
	s3AccessKey string
	s3SecretKey string
	warehouse   string
}

func runServer(ctx context.Context, listenAddr, storeDSN, storeDir string, chOpts clickHouseOpts) {
	var store pipeline.PipelineStore

	if storeDSN != "" {
		pgStore, err := pipeline.NewPgStore(ctx, storeDSN)
		if err != nil {
			log.Fatalf("connect to pipeline store: %v", err)
		}
		defer pgStore.Close()
		store = pgStore
		log.Printf("using postgres pipeline store")
	} else {
		store = pipeline.NewFileStore(storeDir)
		log.Printf("using file-based pipeline store at %s", storeDir)
	}

	mgr := pipeline.NewManager(ctx, store)

	// Set up ClickHouse auto-provisioning if configured.
	if chOpts.addr != "" {
		ch := pipeline.NewClickHouseProvisioner(chOpts.addr, chOpts.catalogURI, chOpts.s3Endpoint, chOpts.s3AccessKey, chOpts.s3SecretKey, chOpts.warehouse)
		mgr.SetClickHouse(ch)
		log.Printf("clickhouse auto-provisioning enabled (%s)", chOpts.addr)
	}

	if err := mgr.RestoreAll(); err != nil {
		log.Fatalf("restore pipelines: %v", err)
	}

	srv := api.NewServer(mgr, listenAddr)

	go func() {
		<-ctx.Done()
		mgr.StopAll()
	}()

	if err := srv.Run(ctx); err != nil {
		log.Fatalf("server error: %v", err)
	}
}
