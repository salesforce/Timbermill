package utils

import (
	"bytes"
	"encoding/gob"
)

func TruncateString(str string, num int) string {
	if len(str) > num {
		str = str[:num]
	}
	return str
}

func GetRealSizeOf(v interface{}) (int, error) {
	b := new(bytes.Buffer)
	if err := gob.NewEncoder(b).Encode(v); err != nil {
		return 0, err
	}
	return b.Len(), nil
}
