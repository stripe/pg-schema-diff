package namegenerator

import (
	"bytes"
	"errors"
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGenerate(t *testing.T) {
	require.Len(t, adjectives, 64)
	require.Len(t, nouns, 64)

	name, err := Generate(bytes.NewReader([]byte{0, 1, 0xde, 0xad, 0xbe, 0xef, 0x01}))
	require.NoError(t, err)
	require.Equal(t, "amber_beaver_deadbeef01", name)
	require.Regexp(t, regexp.MustCompile(`^[a-z]+_[a-z]+_[0-9a-f]{10}$`), name)
}

func TestGenerateReturnsRandomSourceError(t *testing.T) {
	expectedErr := errors.New("random source failed")
	_, err := Generate(errorReader{err: expectedErr})
	require.ErrorIs(t, err, expectedErr)
}

type errorReader struct {
	err error
}

func (r errorReader) Read([]byte) (int, error) {
	return 0, r.err
}
