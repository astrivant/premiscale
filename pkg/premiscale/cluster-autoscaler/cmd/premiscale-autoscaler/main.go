// Command premiscale-autoscaler serves Kubernetes' externalgrpc cloud-provider API.
package main

import (
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/premiscale/premiscale/cluster-autoscaler/bridge"
	pb "github.com/premiscale/premiscale/cluster-autoscaler/protocol"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	address := flag.String("listen", "127.0.0.1:50051", "public provider listen address")
	backend := flag.String("backend", "", "private Python backend Unix socket target")
	timeout := flag.Duration("timeout", 30*time.Second, "maximum backend RPC duration")
	cert := flag.String("tls-cert", "", "server certificate PEM")
	key := flag.String("tls-key", "", "server private key PEM")
	ca := flag.String("tls-ca", "", "client CA PEM for mutual TLS")
	flag.Parse()
	if *backend == "" || *timeout <= 0 {
		log.Fatal("backend and a positive timeout are required")
	}
	options := []grpc.ServerOption{}
	if (*cert == "") != (*key == "") || (*ca != "" && *cert == "") {
		log.Fatal("TLS requires both certificate and key")
	}
	if *cert != "" {
		pair, err := tls.LoadX509KeyPair(*cert, *key)
		if err != nil {
			log.Fatal(err)
		}
		config := &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{pair}}
		if *ca != "" {
			pem, err := os.ReadFile(*ca)
			if err != nil {
				log.Fatal(err)
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(pem) {
				log.Fatal("client CA contains no certificates")
			}
			config.ClientCAs, config.ClientAuth = pool, tls.RequireAndVerifyClientCert
		}
		options = append(options, grpc.Creds(credentials.NewTLS(config)))
	}
	connection, err := grpc.NewClient(*backend, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal(err)
	}
	defer connection.Close()
	listener, err := net.Listen("tcp", *address)
	if err != nil {
		log.Fatal(err)
	}
	server := grpc.NewServer(options...)
	pb.RegisterCloudProviderServer(server, bridge.New(connection, *timeout))
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(stop)
	go func() {
		<-stop
		timer := time.AfterFunc(5*time.Second, server.Stop)
		server.GracefulStop()
		timer.Stop()
	}()
	// The Python supervisor reads this one-line startup handshake.
	fmt.Println(listener.Addr().String())
	if err := server.Serve(listener); err != nil {
		log.Fatal(err)
	}
}
