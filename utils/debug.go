package utils

import (
	"fmt"
	"strings"
)

func DebugFilter(x any) bool {
	return strings.Contains(fmt.Sprint(x), "fec374648fd5e")
}
