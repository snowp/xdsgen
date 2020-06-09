package xdsgen

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

type testPipeline struct {
	SomeInt int `pipeline:"a"`
	SomeString string `pipeline:"b"`
}

func (t testPipeline) Generate() interface{} {
	return fmt.Sprintf("int: %d, str: %s", t.SomeInt, t.SomeString)
}
func TestFramework(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	g := NewGenerator(ctx)

	aInput, err := g.RegisterInput("a")
	assert.NoError(t, err)
	bInput, err := g.RegisterInput("b")
	assert.NoError(t, err)

	err = g.RegisterOutput("test", testPipeline{})
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

	output := <- g.Out
	assert.NotNil(t, output)
	assert.Equal(t, output.Name, "test")
	assert.Equal(t, output.Value, "int: 32, str: f")

	go func() {
		aInput <- 10
		bInput <- "lala"
	}()

	output = <- g.Out
	assert.NotNil(t, output)
	assert.Equal(t, output.Name, "test")
	assert.Equal(t, output.Value, "int: 10, str: f")

	output = <- g.Out
	assert.NotNil(t, output)
	assert.Equal(t, output.Name, "test")
	assert.Equal(t, output.Value, "int: 10, str: lala")
}
