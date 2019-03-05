package cases

import (
	"fmt"
	"strings"
)

func Camel(s string) string {
	i := strings.Index(s, "_")
	if i == -1 {
		return strings.Title(s)
	}
	if i == len(s)-1 {
		return s[:len(s)-1]
	}

	s1 := s[:i]
	c := string(s[i+1])
	s2 := s[i+2:]
	return Camel(strings.Title(fmt.Sprintf("%s%s%s", s1, strings.Title(c), s2)))
}
