package internal

import (
	"encoding/json"
	"jaya-transport-service/internal/services"
	"log"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api/write"
)

func (s *Service) HandleDeviceData(topic string, payload []byte) {
	t, err := extractTopic(topic)
	if err != nil {
		log.Printf("%v", err)
		return
	}

	device, err := s.GetDeviceFromCacheOrService(t.deviceId)
	if err != nil {
		log.Printf("%v", err)
		return
	}

	switch t.subject {
	case "gatewayhealth", "nodehealth":
		s.HandleHealthData(t, payload, device)
	default:
		s.HandleNodeData(t, payload, device)
	}
}

func (s *Service) HandleHealthData(t *eventTopic, payload []byte, device *services.Device) {
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
	s.WriteToInfluxDB(device.Tenant.Name, point)

	// log.Printf("Received device health data from %s time: %s", t.deviceId, ts.Format("2006-01-02 15:04:05"))
}

func (s *Service) HandleNodeData(t *eventTopic, payload []byte, device *services.Device) {
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
	s.WriteToInfluxDB(device.Tenant.Name, point)

	// log.Printf("Received node data from %s time: %s", t.deviceId, ts.Format("2006-01-02 15:04:05"))
}

func (s *Service) WriteToInfluxDB(bucket string, point *write.Point) {
	writeApi := s.influxClient.Client.WriteAPI(s.cfg.InfluxDB.Org, bucket)
	writeApi.WritePoint(point)
}