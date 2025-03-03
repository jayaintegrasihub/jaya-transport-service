package internal

import (
	"fmt"
	"reflect"
	"strings"
	"time"
	"unicode"
)

func extractTopic(topicString string) (*eventTopic, error) {
	topic := strings.Split(topicString, "/")

	if len(topic) == 4 {
		return &eventTopic{
			prefix:    topic[0],
			version:   topic[1],
			gatewayId: topic[2],
			subject:   topic[3],
			deviceId:  topic[2],
		}, nil
	} else if len(topic) == 5 {
		return &eventTopic{
			prefix:    topic[0],
			version:   topic[1],
			gatewayId: topic[2],
			nodeId:    topic[3],
			subject:   topic[4],
			deviceId:  topic[3],
		}, nil
	} else {
		return nil, fmt.Errorf("topic not supported to be extract : %v", topicString)
	}
}

func StructToMapReflect(data interface{}) map[string]interface{} {
	result := make(map[string]interface{})
	value := reflect.ValueOf(data)
	dataType := reflect.TypeOf(data)

	for i := 0; i < value.NumField(); i++ {
		field := value.Field(i)
		fieldName := dataType.Field(i).Name
		result[lowercaseFirstChar(fieldName)] = field.Interface()
	}
	return result
}

func lowercaseFirstChar(s string) string {
	if s == "" {
		return s
	}
	runes := []rune(s)
	runes[0] = unicode.ToLower(runes[0])
	return string(runes)
}

func parseTimestamp(ts interface{}) time.Time {
	switch v := ts.(type) {
	case int:
		return time.Unix(int64(v), 0)
	case float64:
		return time.Unix(int64(v), 0)
	case string:
		// Layout for ISO 8601 with microseconds
		iso8601Micro := "2006-01-02T15:04:05.999999"
		iso8601Nano := "2006-01-02T15:04:05.999999999"

		// Try parsing with microseconds precision
		if parsedTime, err := time.Parse(iso8601Micro, v); err == nil {
			return parsedTime
		}

		// Try parsing with nanoseconds precision (fallback)
		if parsedTime, err := time.Parse(iso8601Nano, v); err == nil {
			return parsedTime
		}

		// Try RFC3339 (compatible with ISO 8601)
		if parsedTime, err := time.Parse(time.RFC3339, v); err == nil {
			return parsedTime
		}

		fmt.Println("Error: Invalid ISO 8601 timestamp format:", v)
	default:
		fmt.Println("Error: Unsupported timestamp type:", v)
	}
	return time.Time{}
}
