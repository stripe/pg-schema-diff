package graph

import (
	"fmt"
	"io"
)

const attr = `fontname="Helvetica,Arial,sans-serif"
node [fontname="Helvetica,Arial,sans-serif"]
edge [fontname="Helvetica,Arial,sans-serif"]`

// dotBuilder wraps an io.Writer and writes a graph in DOT format
type dotBuilder struct {
	io.Writer
}

// init initializes a graph
func (b *dotBuilder) init() error {
	_, err := fmt.Fprintln(b, `digraph G {`)
	if err != nil {
		return err
	}

	_, err = fmt.Fprintln(b, attr)
	if err != nil {
		return err
	}

	return nil
}

// finish closes the opening curly bracket of the current graph
func (b *dotBuilder) finish() error {
	_, err := fmt.Fprintln(b, "}")
	return err
}

// addNode adds a node to the graph
func (b *dotBuilder) addNode(id int, label string) error {
	attr := fmt.Sprintf(`label="%s"`, label)
	_, err := fmt.Fprintf(b, "n%d [%s]\n", id, attr)
	return err
}

// addEdge adds an edge to the graph
func (b *dotBuilder) addEdge(from, to int) error {
	_, err := fmt.Fprintf(b, "n%d -> n%d\n", from, to)
	return err
}