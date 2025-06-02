package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"jaya-transport-service/config"
	"jaya-transport-service/internal/services"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api/write"
	"github.com/redis/go-redis/v9"
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
	s.startWorkerPool(5)

	go func() {
		ticker := time.NewTicker(time.Second)
		for range ticker.C {
			count := s.messageCount.Swap(0)
			log.Printf("Messages processed per second: %d", count)
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
			s.handleProvisioning(msg.Payload)
		} else {
			s.handleDeviceData(msg.Topic, msg.Payload)
		}
	}
}

func (s *Service) handleProvisioning(payload []byte) {
	var provisionRequest ProvisionRequest
	if err := json.Unmarshal(payload, &provisionRequest); err != nil {
		log.Printf("error unmarshaling JSON: %v\n", err)
		return
	}

	result, err := s.jayaClient.Provision(provisionRequest.SerialNumber)
	if err != nil {
		log.Printf("error provisioning device %s: %v", provisionRequest.SerialNumber, err)
		return
	}

	response := ProvisionResponse{
		Pattern: "provisioning/" + provisionRequest.SerialNumber + "/response",
		Data: ProvisionResponseData{
			Username: result.Username,
			Password: result.Password,
			Status:   result.Status,
		},
	}
	log.Printf("Received provisioning request from %s", provisionRequest.SerialNumber)
	if p, err := json.Marshal(response); err == nil {
		s.mqttClient.Client.Publish(s.ctx, &paho.Publish{
			Topic:   response.Pattern,
			QoS:     2,
			Payload: p,
		})
	} else {
		log.Printf("error building JSON: %v", err)
	}
}

func (s *Service) handleDeviceData(topic string, payload []byte) {
	t, err := extractTopic(topic)
	if err != nil {
		log.Printf("%v", err)
		return
	}

	device, err := s.getDeviceFromCacheOrService(t.deviceId)
	if err != nil {
		log.Printf("%v", err)
		return
	}

	switch t.subject {
	case "gatewayhealth", "nodehealth":
		s.handleHealthData(t, payload, device)
	default:
		s.handleNodeData(t, payload, device)
	}
}

func (s *Service) getDeviceFromCacheOrService(deviceId string) (*services.Device, error) {
	result, err := s.redisClient.Rdb.Get(s.ctx, "device/"+deviceId).Result()
	if err == redis.Nil {
		device, err := s.jayaClient.GetDevice(deviceId)
		if err != nil {
			return nil, fmt.Errorf("error getting device from service: %w", err)
		}
		if jsonDevice, err := json.Marshal(device); err == nil {
			s.redisClient.Rdb.Set(s.ctx, "device/"+deviceId, jsonDevice, 10*time.Minute)
		}
		return device, nil
	} else if err != nil {
		return nil, fmt.Errorf("error getting device from Redis: %w", err)
	}

	var device services.Device
	if err := json.Unmarshal([]byte(result), &device); err != nil {
		return nil, fmt.Errorf("error parsing device JSON: %w", err)
	}
	return &device, nil
}

func (s *Service) handleHealthData(t *eventTopic, payload []byte, device *services.Device) {
	var healthData DeviceHealth
	if err := json.Unmarshal(payload, &healthData); err != nil {
		log.Printf("error unmarshaling health data: %v", err)
		return
	}

	device.Group["device"] = t.deviceId
	device.Group["gateway"] = t.gatewayId
	fields := StructToMapReflect(healthData)
	delete(fields, "ts")

	if healthData.Modules != nil {
		for _, module := range healthData.Modules {
			fields[module.Name] = module.Status
		}
	}
	delete(fields, "modules")

	ts := parseTimestamp(healthData.Ts)

	point := influxdb2.NewPoint("deviceshealth", device.Group, fields, ts)
	s.writeToInfluxDB(device.Tenant.Name, point)

	log.Printf("Received device health data from %s time: %s", t.deviceId, ts.Format("2006-01-02 15:04:05"))
}

func (s *Service) handleNodeData(t *eventTopic, payload []byte, device *services.Device) {
	var nodeData NodeIOData
	if err := json.Unmarshal(payload, &nodeData); err != nil {
		log.Printf("Error unmarshaling node data: %v", err)
		return
	}

	device.Group["device"] = t.deviceId
	device.Group["gateway"] = t.gatewayId
	fields, err := convertToMap(nodeData.Data)
	if err != nil {
		log.Printf("Error converting node data to map: %v", err)
		return
	}

	ts := parseTimestamp(nodeData.Ts)

	point := influxdb2.NewPoint(device.Type, device.Group, fields, ts)
	s.writeToInfluxDB(device.Tenant.Name, point)

	log.Printf("Received node data from %s time: %s", t.deviceId, ts.Format("2006-01-02 15:04:05"))
}

func (s *Service) writeToInfluxDB(bucket string, point *write.Point) {
	writeApi := s.influxClient.Client.WriteAPI(s.cfg.InfluxDB.Org, bucket)
	writeApi.WritePoint(point)
}