package graph

import (
	"context"
	"fmt"
	"reflect"
)

type PipelineStage interface {
	Generate() interface{}
}

type inputUpdate struct {
	name string
	val  interface{}
}

// name of a dependency + the field index of the stage impl that it should be injected into
type stageDependency struct {
	name  string
	index int
}

type pipelineStage struct {
	name     string
	implType reflect.Type

	// list of stages that this stage depends on
	deps []stageDependency

	// nil unless this is a terminal stage
	outCh chan<- GeneratedOutput
}

// GeneratedOutput wraps the output value for a terminating pipeline stage
type GeneratedOutput struct {
	// Name is the name of the stage that generated this output
	Name string
	// Value is the generated value. Will never be nil
	Value interface{}
}

type Generator struct {
	inputs           map[string]struct{}
	topoOrderByInput map[string][]*pipelineStage
	pipelineStages   map[string]*pipelineStage

	ctx context.Context

	evalCh chan inputUpdate
	Out    chan GeneratedOutput
}

func NewGenerator(ctx context.Context) Generator {
	return Generator{ctx: ctx, Out: make(chan GeneratedOutput), evalCh: make(chan inputUpdate, 10), inputs: map[string]struct{}{}, topoOrderByInput: map[string][]*pipelineStage{}, pipelineStages: map[string]*pipelineStage{}}
}

func (g *Generator) RegisterPipelineStage(name string, pipeline PipelineStage, terminal bool) error {
	if _, ok := g.pipelineStages[name]; ok {
		return fmt.Errorf("stage %s already registered", name)
	}

	t := reflect.TypeOf(pipeline)

	var out chan<- GeneratedOutput
	if terminal {
		out = g.Out
	}

	deps := []stageDependency{}
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if name, ok := f.Tag.Lookup("pipeline"); ok {
			deps = append(deps, stageDependency{name: name, index: i})
		}
	}

	stage := &pipelineStage{
		name:     name,
		outCh:    out,
		implType: t,
		deps:     deps,
	}

	g.pipelineStages[name] = stage

	return nil
}

func evaluatePipeline(stage *pipelineStage, values map[string]interface{}) (interface{}, error) {
	p := reflect.New(stage.implType).Elem()

	// TODO cache index mappings
	for _, dep := range stage.deps {
		f := p.Field(dep.index)

		v, ok := values[dep.name]
		if !ok {
			// TODO bubble this up somehow?
			return nil, nil
		}

		if v == nil {
			// TODO bubble this up somehow?
			return nil, nil
		}

		f.Set(reflect.ValueOf(v))
	}

	pipeline, ok := p.Interface().(PipelineStage)
	if !ok {
		return nil, fmt.Errorf("failed to convert output type %v to Pipeline", p)
	}

	return pipeline.Generate(), nil
}

func (g *Generator) Finalize() error {
	// At this point we know
	//   1) all the inputs
	//   2) each individual pipeline with its dependencies (parent nodes)
	//
	// Using this we want to construct a topological ordering per input that dictates the order in which pipelines are
	// regenerated whenever one of the inputs change.
	//
	// For the purpose of keeping this simple, we perform a DFS traversal of the graph
	// starting at each input, recording the traversal path. This also serves to do cycle detection.
	// See https://en.wikipedia.org/wiki/Topological_sorting#Depth-first_search. Potentially optimizations would be
	// possible here but given initial use cases will be fairly small this seems like a reasonable starting point.
	//
	// The current encoding of the (hopefully a) DAG has edges pointing to its dependencies, so a topo sort as is will
	// result in computing the semantically reverse order: the order of processing if the outputs have to be processed
	// before the inputs. Instead, we reverse the graph by constructing an adjacency map. We want the *reverse postordering*
	// of this traversal.

	directInputDependencies := map[string][]string{}
	// TODO USE POINTERS HERE to avoid copying strings around everywhere
	adjacency := map[string][]string{}

	for _, stage := range g.pipelineStages {
		for _, dep := range stage.deps {
			// If the dependency is an input, add it to the direct input map instead of the adjacency matrix.
			_, ok := g.inputs[dep.name]
			if ok {
				directInputDependencies[dep.name] = append(directInputDependencies[dep.name], stage.name)
				continue
			}

			dependentStage, ok := g.pipelineStages[dep.name]
			if !ok {
				return fmt.Errorf("dependency on stage %s declared but stage is not defined", dep.name)
			}

			adjacency[dependentStage.name] = append(adjacency[dependentStage.name], stage.name)
		}
	}

	for key, value := range directInputDependencies {
		d := newDfs(g.pipelineStages, adjacency, func(stage *pipelineStage) {
			// Append the entry to get the reverse traversal order.
			g.topoOrderByInput[key] = append([]*pipelineStage{stage}, g.topoOrderByInput[key]...)
		})
		err := d.do(value)
		if err != nil {
			return err
		}
	}

	go func() {
		currentValues := map[string]interface{}{}
		for {
			select {
			case v := <-g.evalCh:
				currentValues[v.name] = v.val

				stages := g.topoOrderByInput[v.name]

				for _, s := range stages {
					v, err := evaluatePipeline(s, currentValues)
					if err != nil {
						panic(err)
					}

					// If we returned nil we failed to DI all the values - we're not ready to construct this stage.
					// This also means that we cannot safely proceed with further stage, so bail out now.
					// TODO(snowp): We might need to require initial values? we'll see
					if v == nil {
						break
					}

					// update the snapped values to include the newly constructed value
					currentValues[s.name] = v

					if s.outCh != nil {
						select {
						case <-g.ctx.Done():
						case s.outCh <- GeneratedOutput{
							Value: v,
							Name:  s.name,
						}:
						}
					}
				}
			case <-g.ctx.Done():
				return
			}
		}
	}()

	return nil
}

// Helper type for managing DFS state
type dfs struct {
	stages    map[string]*pipelineStage
	adjacency map[string][]string

	totalSeen map[*pipelineStage]struct{}
	tempSeen  map[*pipelineStage]struct{}
	addStage  func(stage *pipelineStage)
}

func newDfs(stages map[string]*pipelineStage, adjacency map[string][]string, addStage func(stage *pipelineStage)) dfs {
	return dfs{
		stages:    stages,
		adjacency: adjacency,
		totalSeen: map[*pipelineStage]struct{}{},
		tempSeen:  map[*pipelineStage]struct{}{},
		addStage:  addStage,
	}
}

func (d *dfs) do(children []string) error {
	for _, c := range children {
		stage := d.stages[c]
		if _, ok := d.totalSeen[stage]; ok {
			continue
		}
		if _, ok := d.tempSeen[stage]; ok {
			return fmt.Errorf("cycle detected")
		}

		d.tempSeen[stage] = struct{}{}
		d.totalSeen[stage] = struct{}{}

		err := d.do(d.adjacency[c])

		d.addStage(stage)
		if err != nil {
			return err
		}

		delete(d.tempSeen, stage)
	}

	return nil
}

func (g *Generator) RegisterInput(name string) (chan<- interface{}, error) {
	if _, ok := g.inputs[name]; ok {
		return nil, fmt.Errorf("cannt register same input multiple times")
	}

	g.inputs[name] = struct{}{}
	inputCh := make(chan interface{})

	go func() {
		for {
			select {
			case v := <-inputCh:
				select {
				case g.evalCh <- inputUpdate{name: name, val: v}:
				case <-g.ctx.Done():
					return

				}
			case <-g.ctx.Done():
				return
			}
		}
	}()

	return inputCh, nil
}
