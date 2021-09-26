package utils

func TruncateString(str string, num int) string {
	if len(str) > num {
		str = str[:num]
	}
	return str
}
