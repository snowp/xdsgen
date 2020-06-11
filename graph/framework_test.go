package graph

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)


// Two inputs, a & b, with an intermediate node:
// +--------+               +--------+
// |        |               |        |
// |   a    |               |   b    |
// |        |               |        |
// +--------+-+             +---+----+
// |          |                 |
// |          |                 |
// |          |                 |
// |      +---v---+             |
// |      |       |             |
// |   +--+  int  |             |
// |   |  |       |             |
// |   |  +-------+             |
// |   |                        |
// |   |                        |
// | +-v----+                   |
// | |      |                   |
// +>+ test +<------------------+
//   |      |
//   +------+
type intermediatePipeline struct {
	SomeInt int `pipeline:"a"`
}

func (i intermediatePipeline) Generate() interface{} {
	return i.SomeInt * 2
}

type testPipeline struct {
	SomeInt    int    `pipeline:"a"`
	SomeString string `pipeline:"b"`
	Intermediate int `pipeline:"intermediate"`
}

func (t testPipeline) Generate() interface{} {
	return fmt.Sprintf("int: %d, int: %d, str: %s", t.SomeInt, t.Intermediate, t.SomeString)
}

func TestFramework(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	g := NewGenerator(ctx)

	aInput, err := g.RegisterInput("a")
	assert.NoError(t, err)
	bInput, err := g.RegisterInput("b")
	assert.NoError(t, err)

	err = g.RegisterPipelineStage("test", testPipeline{}, true)
	assert.NoError(t, err)

	err = g.RegisterPipelineStage("intermediate", intermediatePipeline{}, false)
	assert.NoError(t, err)

	err = g.Finalize()
	assert.NoError(t, err)

	checkOutput := func() *GeneratedOutput {
		select {
		case o := <-g.Out:
			return &o
		default:
			return nil
		}
	}

	assert.Nil(t, checkOutput())

	aInput <- 32

	assert.Nil(t, checkOutput())
	bInput <- "f"

	timedGetOutput := func() *GeneratedOutput {
		t.Helper()
		select {
		case o := <-g.Out:
			return &o
		case <-time.NewTimer(5 * time.Second).C:
			t.Fatal("timed out")
			return nil
		}
	}

	output := timedGetOutput()
	assert.NotNil(t, output)
	assert.Equal(t, "test", output.Name)
	assert.Equal(t, "int: 32, int: 64, str: f", output.Value)

	aInput <- 10
	output = timedGetOutput()
	assert.NotNil(t, output)
	assert.Equal(t, "test", output.Name)
	assert.Equal(t, "int: 10, int: 20, str: f", output.Value)

	bInput <- "lala"
	output = timedGetOutput()
	assert.NotNil(t, output)
	assert.Equal(t, "test", output.Name)
	assert.Equal(t, "int: 10, int: 20, str: lala", output.Value)
}
