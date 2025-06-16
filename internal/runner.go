package internal

import (
	"context"
	"log"
	"sync/atomic"
	"time"

	"jaya-transport-service/config"
	"jaya-transport-service/internal/services"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
)

type Service struct {
	ctx          context.Context
	mqttClient   *services.MqttClient
	influxClient *services.InfluxClient
	redisClient  *services.Redis
	jayaClient   *services.Jaya
	cfg          *config.Config
	messageChan chan MqttMessage
	messageCount atomic.Int64
}

func NewService(ctx context.Context, mqttClient *services.MqttClient, influxClient *services.InfluxClient, redisClient *services.Redis, jayaClient *services.Jaya, cfg *config.Config) *Service {
	return &Service{
		ctx:          ctx,
		mqttClient:   mqttClient,
		influxClient: influxClient,
		redisClient:  redisClient,
		jayaClient:   jayaClient,
		cfg:          cfg,
		messageChan:  make(chan MqttMessage, 1000),
	}
}

func (s *Service) Start() {
	s.subscribeToMQTT()
	s.addPublishHandler()
	s.startWorkerPool(10)

	go func() {
		ticker := time.NewTicker(time.Second * 15)
		for range ticker.C {
			count := s.messageCount.Swap(0)
			log.Printf("Messages processed per 15 second: %d", count)
		}
	}()
}

func (s *Service) subscribeToMQTT() {
	s.mqttClient.Client.Subscribe(context.Background(), &paho.Subscribe{
		Subscriptions: []paho.SubscribeOptions{
			{Topic: "$share/g1/JI/v2/#", QoS: 1},
			{Topic: "$share/g1/provisioning", QoS: 1},
		},
	})
}

func (s *Service) addPublishHandler() {
	s.mqttClient.Client.AddOnPublishReceived(func(pr autopaho.PublishReceived) (bool, error) {
		s.messageChan <- MqttMessage{
			Topic:   pr.Packet.Topic,
			Payload: pr.Packet.Payload,
		}
		return true, nil
	})
}

func (s *Service) startWorkerPool(n int) {
	for i := 0; i < n; i++ {
		go s.processMessages()
	}
}

func (s *Service) processMessages() {
	for msg := range s.messageChan {
		s.messageCount.Add(1)

		if msg.Topic == "provisioning" {
			s.HandleProvisioning(msg.Payload)
		} else {
			s.HandleDeviceData(msg.Topic, msg.Payload)
		}
	}
}
