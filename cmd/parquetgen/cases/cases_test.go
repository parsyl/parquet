package cases_test

import (
	"fmt"
	"testing"

	"github.com/parsyl/parquet/cmd/parquetgen/cases"
	"github.com/stretchr/testify/assert"
)

type testCase struct {
	in, out string
}

func TestCamelCase(t *testing.T) {
	testCases := []testCase{
		{"HelloWorld!", "HelloWorld!"},
		{"helloWorld!", "HelloWorld!"},
		{"hello_world!", "HelloWorld!"},
		{"hello_world_dammit", "HelloWorldDammit"},
		{"hello_world_", "HelloWorld"},
		{"_hello_world", "HelloWorld"},
		{"_hello_world_", "HelloWorld"},
		{"hello", "Hello"},
		{"id", "ID"},
		{"user_id", "UserID"},
		{"identification", "Identification"},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("camel-case-%d", i), func(t *testing.T) {
			assert.Equal(t, tc.out, cases.Camel(tc.in))
		})
	}
}
