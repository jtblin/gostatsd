package statsd

import (
	"reflect"
	"testing"

	"github.com/jtblin/gostatsd/types"
)

func TestParseLine(t *testing.T) {
	tests := map[string]types.Metric{
		"foo.bar.baz:2|c":               {Name: "foo.bar.baz", Value: 2.0, Type: types.COUNTER, Tags: types.Tags{Items: []string{}}},
		"abc.def.g:3|g":                 {Name: "abc.def.g", Value: 3, Type: types.GAUGE, Tags: types.Tags{Items: []string{}}},
		"def.g:10|ms":                   {Name: "def.g", Value: 10, Type: types.TIMER, Tags: types.Tags{Items: []string{}}},
		"smp.rte:5|c|@0.1":              {Name: "smp.rte", Value: 50, Type: types.COUNTER, Tags: types.Tags{Items: []string{}}},
		"smp.rte:5|c|@0.1|#foo:bar,baz": {Name: "smp.rte", Value: 50, Type: types.COUNTER, Tags: types.Tags{Items: []string{"baz", "foo:bar"}}},
		"smp.rte:5|c|#foo:bar,baz":      {Name: "smp.rte", Value: 5, Type: types.COUNTER, Tags: types.Tags{Items: []string{"baz", "foo:bar"}}},
	}

	for input, expected := range tests {
		result, err := parseLine([]byte(input))
		if err != nil {
			t.Errorf("test %s error: %s", input, err)
			continue
		}
		if reflect.DeepEqual(result, expected) {
			t.Errorf("test %s: expected %s, got %s", input, expected, result)
			continue
		}
	}

	failing := []string{"fOO|bar:bazkk", "foo.bar.baz:1|q"}
	for _, tc := range failing {
		result, err := parseLine([]byte(tc))
		if err == nil {
			t.Errorf("test %s: expected error but got %s", tc, result)
		}
	}
}
