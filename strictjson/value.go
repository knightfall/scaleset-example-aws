// Package strictjson reads JSON values while rejecting duplicate object members.
package strictjson

import (
	"encoding/json"
	"errors"
)

// ReadValue consumes one value from the decoder and rejects duplicate members.
func ReadValue(decoder *json.Decoder) (any, error) {
	return readJSONValue(decoder, 0)
}

func readJSONValue(decoder *json.Decoder, depth int) (any, error) {
	const maxDepth = 64
	if depth > maxDepth {
		return nil, errors.New("JSON is nested too deeply")
	}
	token, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	switch token {
	case json.Delim('{'):
		object := make(map[string]any)
		for decoder.More() {
			key, err := decoder.Token()
			if err != nil {
				return nil, err
			}
			name, ok := key.(string)
			if !ok {
				return nil, errors.New("expected a JSON member name")
			}
			if _, exists := object[name]; exists {
				return nil, errors.New("duplicate JSON member")
			}
			value, err := readJSONValue(decoder, depth+1)
			if err != nil {
				return nil, err
			}
			object[name] = value
		}
		_, err := decoder.Token()
		return object, err
	case json.Delim('['):
		array := []any{}
		for decoder.More() {
			value, err := readJSONValue(decoder, depth+1)
			if err != nil {
				return nil, err
			}
			array = append(array, value)
		}
		_, err := decoder.Token()
		return array, err
	default:
		return token, nil
	}
}
