package graph

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDOTEncoding(t *testing.T) {
	for _, tc := range []struct {
		name     string
		adjList  map[string][]string
		expected string
	}{
		{
			name:    "empty graph",
			adjList: map[string][]string{},
			expected: `digraph G {
node [fontname="Helvetica,Arial,sans-serif"]
}
`,
		},
		{
			name: "multiple vertices",
			adjList: map[string][]string{
				"v_0": {"v_1", "v_2"},
				"v_1": {"v_2"},
				"v_2": {"v_3"},
				"v_3": {},
				"v_4": {"v_0", "v_3"},
			},
			expected: `digraph G {
node [fontname="Helvetica,Arial,sans-serif"]
n0 [label="v_0"]
n1 [label="v_1"]
n2 [label="v_2"]
n3 [label="v_3"]
n4 [label="v_4"]
n0 -> n1
n0 -> n2
n1 -> n2
n2 -> n3
n4 -> n0
n4 -> n3
}
`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := NewGraph[vertex]()

			// add vertices
			for id := range tc.adjList {
				g.AddVertex(NewV(id))
			}

			// add edges
			for id, neighbors := range tc.adjList {
				for _, neighborId := range neighbors {
					assert.NoError(t, g.AddEdge(id, neighborId))
				}
			}

			// encode to DOT
			buf := bytes.Buffer{}
			// sort vertices to ensure deterministic ordering of nodes and edges
			assert.NoError(t, EncodeDOT(g, &buf, true))
			assert.Equal(t, tc.expected, buf.String())
		})
	}
}
