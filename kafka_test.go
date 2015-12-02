package kafka

import (
	"encoding/json"
	"reflect"
	"testing"
)

var noopts = map[string]string{}

func Test_read_route_address(t *testing.T) {
	address := "broker1:9092,broker2:9092"
	brokers := readBrokers(address)
	if len(brokers) != 2 {
		t.Fatal("expected two broker addrs")
	}
	if brokers[0] != "broker1:9092" {
		t.Errorf("broker1 addr should not be %s", brokers[0])
	}
	if brokers[1] != "broker2:9092" {
		t.Errorf("broker2 addr should not be %s", brokers[1])
	}
}

func Test_read_route_address_with_a_slash_topic(t *testing.T) {
	address := "broker/hello"
	brokers := readBrokers(address)
	if len(brokers) != 1 {
		t.Fatal("expected a broker addr")
	}

	topic := readTopic(address, noopts)
	if topic != "hello" {
		t.Errorf("topic should not be %s", topic)
	}
}

func Test_read_topic_option(t *testing.T) {
	opts := map[string]string{"topic": "hello"}
	topic := readTopic("", opts)
	if topic != "hello" {
		t.Errorf("topic should not be %s", topic)
	}
}

func Test_read_route_address_with_a_slash_topic_trumps_a_topic_option(t *testing.T) {
	opts := map[string]string{"topic": "trumped"}
	topic := readTopic("broker/hello", opts)
	if topic != "hello" {
		t.Errorf("topic should not be %s", topic)
	}
}

func Test_build_JSON_log_line_with_JSON(t *testing.T) {
	template := "{\"timestamp\":\"2015-12-01 14:34:23 UTC\", \"container_name\":\"a7d7e998ede90c0b\", \"message\":{\"message\":\"Log message\", \"logger\":\"logger\", \"timestamp\":\"2015-12-01 14:34:22 UTC\"}}"
	message := "{\"message\":\"Log message\", \"logger\":\"logger\", \"timestamp\":\"2015-12-01 14:34:22 UTC\"}"
	expectedLogline := "{\"timestamp\":\"2015-12-01 14:34:22 UTC\", \"container_name\":\"a7d7e998ede90c0b\", \"message\":\"Log message\", \"logger\":\"logger\"}"

	logline, err := buildJSONLogLine([]byte(template), message)
	if err != nil {
		t.Error("expected not to fail")
	}

	var loglineJSON map[string]interface{}
	var expectedLoglineJSON map[string]interface{}
	json.Unmarshal(logline, &loglineJSON)
	json.Unmarshal([]byte(expectedLogline), &expectedLoglineJSON)

	if !reflect.DeepEqual(loglineJSON, expectedLoglineJSON) {
		t.Errorf("Expected JSON log line to be: %s \nbut got: %s", expectedLogline, string(logline))
	}
}

func Test_build_JSON_log_line_with_simple_text(t *testing.T) {
	template := "{\"timestamp\":\"2015-12-01 14:34:23 UTC\", \"container_name\":\"a7d7e998ede90c0b\", \"message\":\"Log message\"}"
	message := "Log message"
	expectedLogline := "{\"timestamp\":\"2015-12-01 14:34:23 UTC\", \"container_name\":\"a7d7e998ede90c0b\", \"message\":\"Log message\"}"

	logline, err := buildJSONLogLine([]byte(template), message)
	if err != nil {
		t.Error("expected not to fail")
	}

	var loglineJSON map[string]interface{}
	var expectedLoglineJSON map[string]interface{}
	json.Unmarshal(logline, &loglineJSON)
	json.Unmarshal([]byte(expectedLogline), &expectedLoglineJSON)

	if !reflect.DeepEqual(loglineJSON, expectedLoglineJSON) {
		t.Errorf("Expected JSON log line to be: %s \nbut got: %s", expectedLogline, string(logline))
	}
}
