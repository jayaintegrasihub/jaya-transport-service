package config

import (
	"github.com/caarlos0/env/v11"
)

type Config struct {
	MQTT     MQTTConfig
	InfluxDB InfluxDBConfig
	JayaApi  JayaApiConfig
	Redis    RedisConfig
}

type MQTTConfig struct {
	Broker   string `env:"MQTT_BROKER,required"`
	ClientID string `env:"MQTT_CLIENT_ID,required"`
	Topic    string `env:"MQTT_TOPIC,required"`
	Username string `env:"MQTT_USERNAME,required"`
	Password string `env:"MQTT_PASSWORD,required"`
}

type InfluxDBConfig struct {
	URL    string `env:"INFLUXDB_URL,required"`
	Token  string `env:"INFLUXDB_TOKEN,required"`
	Org    string `env:"INFLUXDB_ORG,required"`
	Bucket string `env:"INFLUXDB_BUCKET,required"`
}

type JayaApiConfig struct {
	URL   string `env:"JAYA_URL,required"`
	Token string `env:"JAYA_TOKEN,required"`
}

type RedisConfig struct {
	URL      string `env:"REDIS_URL,required"`
	Password string `env:"REDIS_PASSWORD,required"`
	Username string `env:"REDIS_USERNAME,required"`
	DB       int    `env:"REDIS_DB,required"`
}

func LoadConfig() (*Config, error) {
	cfg := &Config{}
	if err := env.Parse(cfg); err != nil {
		return nil, err
	}
	
	return cfg, nil
}