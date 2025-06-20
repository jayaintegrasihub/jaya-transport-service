package config

import (
	"github.com/spf13/viper"
)

type Config struct {
	MQTT     MQTTConfig
	InfluxDB InfluxDBConfig
	JayaApi  JayaApiConfig
	Redis    RedisConfig
}

type MQTTConfig struct {
	Broker   string
	ClientID string
	Topic    string
	Username string
	Password string
}

type InfluxDBConfig struct {
	URL    string
	Token  string
	Org    string
	Bucket string
}

type JayaApiConfig struct {
	URL   string
	Token string
}

type RedisConfig struct {
	URL      string
	Password string
	Username string
	DB       int
}

func LoadConfig() *Config {
	viper.SetConfigFile(".env")
	viper.ReadInConfig()

	return &Config{
		MQTT: MQTTConfig{
			Broker:   viper.GetString("MQTT_BROKER"),
			ClientID: viper.GetString("MQTT_CLIENT_ID"),
			Topic:    viper.GetString("MQTT_TOPIC"),
			Username: viper.GetString("MQTT_USERNAME"),
			Password: viper.GetString("MQTT_PASSWORD"),
		},
		InfluxDB: InfluxDBConfig{
			URL:    viper.GetString("INFLUXDB_URL"),
			Token:  viper.GetString("INFLUXDB_TOKEN"),
			Org:    viper.GetString("INFLUXDB_ORG"),
			Bucket: viper.GetString("INFLUXDB_BUCKET"),
		},
		JayaApi: JayaApiConfig{
			URL:   viper.GetString("JAYA_URL"),
			Token: viper.GetString("JAYA_TOKEN"),
		},
		Redis: RedisConfig{
			URL:      viper.GetString("REDIS_URL"),
			Password: viper.GetString("REDIS_PASSWORD"),
			Username: viper.GetString("REDIS_USERNAME"),
			DB:       viper.GetInt("REDIS_DB"),
		},
	}
}