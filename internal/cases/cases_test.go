package cases_test

import (
	"fmt"
	"testing"

	"github.com/parsyl/parquet/internal/cases"
	"github.com/stretchr/testify/assert"
)

type testCase struct {
	in, out string
}

func TestCamelCase(t *testing.T) {
	testCases := []testCase{
		{"HelloWorld!", "HelloWorld!"},
		{"hello_world!", "HelloWorld!"},
		{"hello_world_dammit", "HelloWorldDammit"},
		{"hello_world_", "HelloWorld"},
		{"hello", "Hello"},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("camel-case-%d", i), func(t *testing.T) {
			assert.Equal(t, tc.out, cases.Camel(tc.in))
		})
	}
}
