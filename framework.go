package xdsgen

import (
	"context"
	"fmt"
	"reflect"
	"sync"
)

type Pipeline interface {
	Generate() interface{}
}

type input struct {
	ch chan interface{}
	latest interface{}
}

type output struct {
	name string
	deps []string
	pipelineType reflect.Type
}

type GeneratedOutput struct {
	Name string
	Value interface{}
}

type Generator struct {
	inputs map[string]*input
	outputByInput map[string][]*output

	publishCh chan map[string]interface{}
	Out chan GeneratedOutput
	ctx context.Context

	sync.Mutex
}

func (g *Generator) outputsForInput(inputName string) map[string]reflect.Type {
	g.Lock()
	defer g.Unlock()

	outputs := map[string]reflect.Type{}
	for _, o := range g.outputByInput[inputName]{
		outputs[o.name] = o.pipelineType
	}

	return outputs
}

func (g *Generator) updateInput(name string, value interface{}) (map[string]interface{}, error) {
	g.Lock()
	defer g.Unlock()
	input, ok := g.inputs[name]
	if !ok {
		return nil, fmt.Errorf("cannot update input %s, not registered", name)
	}

	input.latest = value

	snappedValues := map[string]interface{}{}
	for k, v := range g.inputs {
		snappedValues[k] = v.latest
	}

	return snappedValues, nil
}

func NewGenerator(ctx context.Context) Generator {
	return Generator{ctx: ctx, Out: make(chan GeneratedOutput), inputs: map[string]*input{}, outputByInput: map[string][]*output{}}
}

func (g *Generator) RegisterOutput(name string, pipeline interface{}) error {
	deps := []string{}
	t := reflect.TypeOf(pipeline)
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if dep := f.Tag.Get("pipeline"); dep != "" {
			deps = append(deps, dep)
		}
	}

	output := &output{name: name, deps: deps, pipelineType: reflect.TypeOf(pipeline)}
	for _, d := range deps {
		if _, ok := g.inputs[d]; !ok {
			return fmt.Errorf("input %s not defined", d)
		}
		g.outputByInput[d] = append(g.outputByInput[d], output)
	}

	return nil
}

func evaluatePipeline(pipelineType reflect.Type, values map[string]interface{}) (interface{}, error) {
	p := reflect.New(pipelineType).Elem()

	// TODO cache index mappings
	for i := 0; i < pipelineType.NumField(); i++ {
		f := pipelineType.Field(i)
		if dep := f.Tag.Get("pipeline"); dep != "" {
			f := p.Field(i)

			v, ok := values[dep]
			if !ok {
				return nil, fmt.Errorf("dependency %s not provided", dep)
			}

			f.Set(reflect.ValueOf(v))
		}
	}

	pipeline, ok := p.Interface().(Pipeline)
	if !ok {
		return nil, fmt.Errorf("failed to convert output type %v to Pipeline", p)
	}

	return pipeline.Generate(), nil
}

func (g *Generator) RegisterInput(name string) (chan<-interface{}, error) {
	if _, ok := g.inputs[name]; ok {
		return nil, fmt.Errorf("cannt register same input multiple times")
	}

	input := &input{
		ch: make(chan interface{}),
	}

	go func() {
		for {
			select {
			case v := <-input.ch:
				snappedValues, err := g.updateInput(name, v)
				if err != nil {
					panic(err)
				}

				anyNils := false
				// If any of the inputs haven't been provided yet, do nothing.
				for _, v := range snappedValues {
					if v == nil {
						anyNils = true
						break
					}
				}

				if anyNils {
					break
				}

				outputs := g.outputsForInput(name)
				for name, pipelineType := range outputs {
					v, err := evaluatePipeline(pipelineType, snappedValues)
					if err != nil {
						panic(err)
					}

					select {
					case g.Out <- GeneratedOutput{
						Name:  name,
						Value: v,
					}:
					case <-g.ctx.Done():
						return
					}
				}
			case <-g.ctx.Done():
				return
			}
		}
	}()

	g.inputs[name] = input

	return input.ch, nil
}
