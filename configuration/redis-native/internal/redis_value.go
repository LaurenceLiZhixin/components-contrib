package internal

import (
	"errors"
	"fmt"
	"strings"
)

const separator = "/"
const channelPrefix = "__keyspace@0__:"

func GetRedisValueAndVersion(redisValue string) (string, string) {
	valueAndRevision := strings.Split(redisValue, separator)
	if len(valueAndRevision) == 0 {
		return "", ""
	}
	if len(valueAndRevision) == 1 {
		return valueAndRevision[0], ""
	}
	return valueAndRevision[0], valueAndRevision[1]
}

func ParseRedisKeyFromEvent(eventChannel string) (string, error) {
	index := strings.Index(eventChannel, channelPrefix)
	if index == -1 {
		return "", errors.New(fmt.Sprintf("wrong format of event channel, it should start with '%s': eventChannel=%s", channelPrefix, eventChannel))
	}

	return eventChannel[len(channelPrefix):], nil
}
