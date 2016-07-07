package output

import (
	"fmt"
	"testing"
	"time"
)

func TestRending(t *testing.T) {
	m := &Metric{}
	m.AddPoint(&Point{"foo", 0.1})
	m.AddPoint(&Point{"bar", 1.0})

	tstamp := time.Now().Unix()

	r := m.Render()

	expectedOutput := fmt.Sprintf(
		"foo 0.100000 %d\nbar 1.000000 %d",
		tstamp,
		tstamp,
	)

	if r != expectedOutput {
		t.Errorf("Wrong output: %s and it's %s", r, expectedOutput)
	}
}
