package utils

import "encoding/json"

func CompressToJson(obj any) []byte {
	raw, _ := json.Marshal(obj)
	return raw
}
