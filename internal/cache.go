package internal

import (
	"encoding/json"
	"fmt"
	"jaya-transport-service/internal/services"
	"time"

	"github.com/redis/go-redis/v9"
)

func (s *Service) GetDeviceFromCacheOrService(deviceId string) (*services.Device, error) {
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