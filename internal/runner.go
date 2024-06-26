package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"jaya-transport-service/config"
	"jaya-transport-service/internal/services"
	"log"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/redis/go-redis/v9"
)

type Service struct {
	ctx          context.Context
	mqttClient   *services.MqttClient
	influxClient *services.InfluxCLient
	redisClient  *services.Redis
	jayaClient   *services.Jaya
	cfg          *config.Config
}

func NewService(ctx context.Context, mqttClient *services.MqttClient, influxClient *services.InfluxCLient, redisClient *services.Redis, jayaClient *services.Jaya, cfg *config.Config) *Service {
	return &Service{
		ctx:          ctx,
		mqttClient:   mqttClient,
		influxClient: influxClient,
		redisClient:  redisClient,
		jayaClient:   jayaClient,
		cfg:          cfg,
	}
}

func (s *Service) Start() {
	s.mqttClient.Client.Subscribe(context.Background(), &paho.Subscribe{
		Subscriptions: []paho.SubscribeOptions{
			{Topic: "$share/g1/JI/v2/#", QoS: 1},
			{Topic: "$share/g1/provisioning", QoS: 1},
		},
	})

	s.mqttClient.Client.AddOnPublishReceived(func(pr autopaho.PublishReceived) (bool, error) {
		// Handle provision request
		if pr.Packet.Topic == "provisioning" {
			var provisionRequest ProvisionRequest
			err := json.Unmarshal(pr.Packet.Payload, &provisionRequest)
			if err != nil {
				fmt.Printf("error unmarshaling JSON: %v\n", err)
				return true, nil
			}
			result, err := s.jayaClient.Provision(provisionRequest.SerialNumber)
			if err != nil {
				if err == services.ErrDeviceNotFound {
					log.Printf("device with id: %s not found in Jaya Core.", provisionRequest.SerialNumber)
				} else {
					log.Printf("error get device %s", err)
				}
				return true, nil
			}

			response := ProvisionResponse{
				Pattern: "provisioning/" + provisionRequest.SerialNumber + "/response",
				Data: ProvisionResponseData{
					Username: result.Username,
					Password: result.Password,
					Status:   result.Status,
				},
			}

			p, err := json.Marshal(response)
			if err != nil {
				log.Printf("error build JSON: %s", err)
				return true, nil
			}

			s.mqttClient.Client.Publish(s.ctx, &paho.Publish{
				Topic:   "provisioning/" + provisionRequest.SerialNumber + "/response",
				QoS:     2,
				Payload: p,
			})
			return true, nil
		}

		topic := extractTopic(pr.Packet.Topic)

		var device *services.Device
		result, err := s.redisClient.Rdb.Get(s.ctx, "device/"+topic.gatewayId).Result()

		if err == redis.Nil {
			device, err = s.jayaClient.GetDevice(topic.deviceId)
			fmt.Println("from jaya")
			if err != nil {
				if err == services.ErrDeviceNotFound {
					log.Printf("device with id: %s not found in Jaya Core.", topic.gatewayId)
				} else {
					log.Printf("error get device %s", err)
				}
				return true, nil
			}

			jsondevice, err := json.Marshal(device)
			if err != nil {
				fmt.Printf("Error convert to map field: %v\n", err)
				return true, nil
			}
			s.redisClient.Rdb.Set(s.ctx, "device/"+topic.gatewayId, jsondevice, 0)
		} else if err != nil {
			log.Printf("error getting device from Redis: %s", err)
			return true, nil
		} else {
			err = json.Unmarshal([]byte(result), &device)
			if err != nil {
				log.Printf("error parse JSON: %s", err)
				return true, nil
			}
		}

		switch topic.subject {
		case "gatewayhealth", "nodehealth":
			var devicehealth DeviceHealth
			err := json.Unmarshal(pr.Packet.Payload, &devicehealth)
			if err != nil {
				fmt.Printf("error unmarshaling JSON: %v\n", err)
			}

			device.Group["device"] = topic.deviceId
			if topic.subject == "nodehealth" {
				device.Group["gateway"] = topic.gatewayId
			}

			fields := StructToMapReflect(devicehealth)
			delete(fields, "ts")
			t := time.Unix(int64(devicehealth.Ts), 0)

			point := influxdb2.NewPoint(topic.subject, device.Group, fields, t)
			writeApi := s.influxClient.Client.WriteAPI(s.cfg.InfluxDB.Org, device.Tenant.Name)

			writeApi.WritePoint(point)
			log.Println("Receive data device heartbeat from " + topic.gatewayId)
		default:
			var nodeIOData NodeIOData
			err := json.Unmarshal(pr.Packet.Payload, &nodeIOData)
			if err != nil {
				fmt.Printf("Error unmarshaling JSON: %v\n", err)
			}

			device.Group["device"] = topic.deviceId

			t := time.Unix(int64(nodeIOData.Ts), 0)

			fields, err := convertToMap(nodeIOData.Data)
			if err != nil {
				fmt.Printf("Error convert to map field: %v\n", err)
				return true, nil
			}

			point := influxdb2.NewPoint(device.Type, device.Group, fields, t)
			writeApi := s.influxClient.Client.WriteAPI(s.cfg.InfluxDB.Org, device.Tenant.Name)

			writeApi.WritePoint(point)
			log.Println("Receive data node data from " + topic.nodeId)
		}

		return true, nil
	})
}
