package accounts

import (
	"github.com/status-im/status-go/eth-node/types"
	"github.com/status-im/status-go/sqlite"
)

func BoolHandler(value interface{}) (interface{}, error) {
	_, ok := value.(bool)
	if !ok {
		return value, ErrInvalidConfig
	}

	return value, nil
}

func JSONBlobHandler(value interface{}) (interface{}, error) {
	return &sqlite.JSONBlob{Data: value}, nil
}

func AddressHandler(value interface{}) (interface{}, error) {
	str, ok := value.(string)
	if ok {
		value = types.HexToAddress(str)
	} else {
		return value, ErrInvalidConfig
	}
	return value, nil
}
