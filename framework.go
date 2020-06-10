package xdsgen

import (
	"context"
	"fmt"
	"reflect"
	"sync"
)

type PipelineStage interface {
	Generate() interface{}
}

type input struct {
	ch     chan interface{}
	latest interface{}
}

type pipelineStage struct {
	name     string
	latest   interface{}
	implType reflect.Type

	// children of this stage - these should be evaluated whenever this pipeline stage is re-evaluated.
	children []*pipelineStage
	// list of stages that this stage depends on - essentially the list of values that should be injected into this stage
	deps []string

	// nil unless this is a terminal stage
	outCh chan<- GeneratedOutput
}

type GeneratedOutput struct {
	Name  string
	Value interface{}
}

type Generator struct {
	inputs                  map[string]*input
	topoOrderByInput map[string][]*pipelineStage
	pipelineStages          map[string]*pipelineStage

	publishCh chan map[string]interface{}
	Out       chan GeneratedOutput
	ctx       context.Context

	sync.Mutex
}

func (g *Generator) updateStage(name string, value interface{}) (map[string]interface{}, error) {
	g.Lock()
	defer g.Unlock()
	stage, ok := g.pipelineStages[name]
	if !ok {
		return nil, fmt.Errorf("cannot update stage %s, not registered", name)
	}

	stage.latest = value

	snappedValues := map[string]interface{}{}
	for k, v := range g.inputs {
		snappedValues[k] = v.latest
	}

	for k, v := range g.pipelineStages {
		snappedValues[k] = v.latest
	}


	return snappedValues, nil
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

	for k, v := range g.pipelineStages {
		snappedValues[k] = v.latest
	}

	return snappedValues, nil
}

func NewGenerator(ctx context.Context) Generator {
	return Generator{ctx: ctx, Out: make(chan GeneratedOutput), inputs: map[string]*input{}, topoOrderByInput: map[string][]*pipelineStage{}, pipelineStages: map[string]*pipelineStage{}}
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

	deps := []string{}
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if name, ok := f.Tag.Lookup("pipeline"); ok {
			deps = append(deps, name)
		}
	}

	stage := &pipelineStage{
		name:     name,
		outCh:    out,
		implType: t,
		deps: deps,
	}

	g.pipelineStages[name] = stage

	return nil
}

func evaluatePipeline(stage *pipelineStage, values map[string]interface{}) (interface{}, error) {
	p := reflect.New(stage.implType).Elem()

	// TODO cache index mappings
	for i := 0; i < stage.implType.NumField(); i++ {
		f := stage.implType.Field(i)
		if dep := f.Tag.Get("pipeline"); dep != "" {
			f := p.Field(i)

			v, ok := values[dep]
			if !ok {
				// in this case one of our dependent values haven't been computed. Skip this one intentionally, we can rely on this object being created again if our dependencies end up being recomputed during this run
				// TODO(snowp): We might need to require seeded values or do more thorough recinoytatuib
				return nil, nil
			}

			if v == nil {
				return nil, nil
			}

			f.Set(reflect.ValueOf(v))
		}
	}

	pipeline, ok := p.Interface().(PipelineStage)
	if !ok {
		return nil, fmt.Errorf("failed to convert output type %v to Pipeline", p)
	}

	return pipeline.Generate(), nil
}

func (g *Generator) Finalize() error {
	g.Lock()
	defer g.Unlock()
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
	// The current encoding of the (hopefully) DSG has edges pointing to its dependencies, so a topo sort as is will
	// result in computing the semantically reverse order: the order of processing if the outputs have to be processed
	// before the inputs. Instead, we reverse the graph by constructing an adjacency map. We want the *reverse postordering*
	// of this traversal.

	directInputDependencies := map[string][]string{}
	// TODO USE POINTERS HERE to avoid copying strings around everywhere
	adjacency := map[string][]string{}

	for _, stage := range g.pipelineStages {
		for _, dep := range stage.deps {
			// If the dependency is an input, add it to the direct input map instead of the adjacency matrix.
			_, ok := g.inputs[dep]
			if ok {
				directInputDependencies[dep] = append(directInputDependencies[dep], stage.name)
				continue
			}

			dependentStage, ok := g.pipelineStages[dep]
			if !ok {
				return fmt.Errorf("dependency on stage %s declared but stage is not defined", dep)
			}

			adjacency[dependentStage.name] = append(adjacency[dependentStage.name], stage.name)
		}
	}

	// With the adjacency matrix we can start performing the DFS.
	// Keep track of what we've seen to avoid cycles.
	for key, value := range directInputDependencies {

		exclusions := map[string]struct{}{}
		for k := range g.inputs {
			if k != key {
				exclusions[k] = struct{}{}
			}
		}
		d := newDfs(g.pipelineStages, adjacency, func(stage *pipelineStage) {
			g.topoOrderByInput[key] = append([]*pipelineStage{stage}, g.topoOrderByInput[key]...)
		})
		d.exclusions = exclusions
		err := d.do(value)
		if err != nil {
			return err
		}
	}

	return nil
}

type dfs struct {
	stages map[string]*pipelineStage
	adjacency map[string][]string
	exclusions map[string]struct{}

	totalSeen map[*pipelineStage]struct{}
	tempSeen map[*pipelineStage]struct{}
	addStage func(stage *pipelineStage)
}

func newDfs(stages map[string]*pipelineStage, adjacency map[string][]string, addStage func(stage *pipelineStage)) dfs {
	return dfs{
		stages:    stages,
		adjacency: adjacency,
		totalSeen: map[*pipelineStage]struct{}{},
		tempSeen: map[*pipelineStage]struct{}{},
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

				g.Lock()
				stages := g.topoOrderByInput[name]
				g.Unlock()

				for _, s := range stages {
					v, err := evaluatePipeline(s, snappedValues)
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
					snappedValues, err = g.updateStage(s.name, v)
					if err != nil {
						panic(err)
					}

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

	g.inputs[name] = input

	return input.ch, nil
}
