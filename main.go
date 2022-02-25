package main

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	log "github.com/sirupsen/logrus"
	czmq "github.com/zeromq/goczmq/v4"
)

var (
	frontendEndpoint   = "@tcp://127.0.0.1:9100" // XSub
	backendEndpoint    = "@tcp://127.0.0.1:9101" // XPub
	publisherEndpoint  = ">tcp://localhost:9100" // Pub
	subscriberEndpoint = ">tcp://localhost:9101" // Sub
)

func main() {
	ctx, cancelFunc := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}

	level, _ := log.ParseLevel("debug")
	log.SetLevel(level)
	log.WithFields(log.Fields{"context": "main"}).Debug("starting")

	wg.Add(3)

	go runProxy(ctx, wg)
	go runSubscriber(ctx, wg)
	go runPublisher(ctx, wg)

	termChan := make(chan os.Signal, 1)
	signal.Notify(termChan, syscall.SIGINT, syscall.SIGTERM)
	<-termChan

	log.WithFields(log.Fields{"context": "main"}).Debug("terminated")

	cancelFunc()
	wg.Wait()
}

func runProxy(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	done := make(chan bool)

	go func() {
		var proxy *czmq.Proxy

		if proxy = czmq.NewProxy(); proxy == nil {
			log.Panic("failed to create proxy")
		}
		defer proxy.Destroy()

		if err := proxy.SetFrontend(czmq.XSub, frontendEndpoint); err != nil {
			log.Panic(err)
		}
		if err := proxy.SetBackend(czmq.XPub, backendEndpoint); err != nil {
			log.Panic(err)
		}

		<-done
	}()

	<-ctx.Done()
	done <- true
}

func runPublisher(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	go func() {
		publisher, err := czmq.NewPub(publisherEndpoint)
		if publisher == nil {
			log.Panic("failed to create publisher")
		}
		if err != nil {
			log.Panic(err)
		}
		defer publisher.Destroy()

		for {
			log.WithFields(log.Fields{"socket": "publisher"}).Info("publishing data...")
			publisher.SendFrame([]byte("test"), czmq.FlagNone)
			time.Sleep(100 * time.Millisecond)
		}
	}()

	<-ctx.Done()
}

func runSubscriber(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	go func() {
		subscriber, err := czmq.NewSub(subscriberEndpoint, "")
		if subscriber == nil {
			log.Panic("failed to create subscriber")
		}
		if err != nil {
			log.Panic(err)
		}
		defer subscriber.Destroy()

		poller, err := czmq.NewPoller()
		if err != nil {
			log.Panic("failed to create poller")
		}
		defer poller.Destroy()

		if err = poller.Add(subscriber); err != nil {
			log.Panic("failed to add subscriber to poller")
		}

		for {
			log.WithFields(log.Fields{"socket": "subscriber"}).Info("waiting for data...")

			socket, err := poller.Wait(1000)
			if err != nil {
				break
			}

			if socket == nil {
				continue
			} else {
				data, _, err := socket.RecvFrame()
				if err != nil {
					continue
				}
				log.WithFields(log.Fields{"socket": "subscriber", "data": data}).Infof("received frame")
			}
		}
	}()

	<-ctx.Done()
}
