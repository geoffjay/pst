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

	log.WithFields(log.Fields{"context": "main"}).Debug("signal received")

	cancelFunc()
	wg.Wait()

	log.WithFields(log.Fields{"context": "main"}).Debug("exiting")
}

func runProxy(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	done := make(chan bool)
	log.WithFields(log.Fields{"context": "proxy"}).Debug("setup")

	go func() {
		proxy := czmq.NewProxy()
		if proxy == nil {
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
		log.WithFields(log.Fields{"context": "proxy"}).Debug("exiting routine")
	}()

	log.WithFields(log.Fields{"context": "proxy"}).Debug("blocking")
	<-ctx.Done()
	log.WithFields(log.Fields{"context": "proxy"}).Debug("received shutdown")
	done <- true
	log.WithFields(log.Fields{"context": "proxy"}).Debug("terminated")
}

func runPublisher(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	running := true
	log.WithFields(log.Fields{"context": "publisher"}).Debug("setup")

	publisher, err := czmq.NewPub(publisherEndpoint)
	if err != nil {
		log.Panic(err)
	}
	defer publisher.Destroy()

	go func() {
		for running {
			publisher.SendFrame([]byte("test"), czmq.FlagNone)
			time.Sleep(500 * time.Millisecond)
		}
	}()

	<-ctx.Done()
	log.WithFields(log.Fields{"context": "publisher"}).Debug("received shutdown")
	running = false
	log.WithFields(log.Fields{"context": "publisher"}).Debug("terminated")
}

func runSubscriber(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	running := true
	log.WithFields(log.Fields{"context": "subscriber"}).Debug("setup")

	subscriber, err := czmq.NewSub(subscriberEndpoint, "")
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

	go func() {
		for running {
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
	log.WithFields(log.Fields{"context": "subscriber"}).Debug("received shutdown")
	running = false
	log.WithFields(log.Fields{"context": "subscriber"}).Debug("terminated")
}
