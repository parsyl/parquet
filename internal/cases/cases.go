package cases

import (
	"fmt"
	"strings"
)

func Camel(s string) string {
	return camel(ids(s))
}

func camel(s string) string {
	i := strings.Index(s, "_")
	if i == -1 {
		return ids(strings.Title(s))
	}
	if i == len(s)-1 {
		return s[:len(s)-1]
	}

	s1 := s[:i]
	c := string(s[i+1])
	s2 := s[i+2:]
	return Camel(strings.Title(fmt.Sprintf("%s%s%s", s1, strings.Title(c), s2)))
}

func ids(s string) string {
	i := strings.Index(s, "id")
	if i == -1 {
		return s
	}

	if s == "id" {
		return "ID"
	}

	i = strings.Index(s, "id_")
	if i == 0 {
		s = strings.Replace(s, "id_", "ID_", 1)
	}

	i = strings.Index(s, "_id")
	if len(s) > 3 && i == len(s)-3 {
		s = s[:len(s)-3] + "_ID"
	}

	i = strings.Index(s, "_id_")
	if i > 0 {
		s = strings.Replace(s, "_id_", "_ID_", -1)
	}

	return s
}
