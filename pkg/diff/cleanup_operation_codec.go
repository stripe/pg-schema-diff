package diff

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
)

const (
	cleanupOperationCodecVersion = 1
	cleanupDigestDomain          = "pg-schema-diff/cleanup-statements/v1"
	cleanupDigestPrefix          = "sha256:"
)

type cleanupOperationKind string

const (
	cleanupOperationKindDropTable     cleanupOperationKind = "drop_table"
	cleanupOperationKindDropSequence  cleanupOperationKind = "drop_sequence"
	cleanupOperationKindDropFunction  cleanupOperationKind = "drop_function"
	cleanupOperationKindDropType      cleanupOperationKind = "drop_type"
	cleanupOperationKindDropCollation cleanupOperationKind = "drop_collation"
	cleanupOperationKindDropOperator  cleanupOperationKind = "drop_operator"
	cleanupOperationKindDropSchema    cleanupOperationKind = "drop_schema"
)

type cleanupOperationV1 struct {
	Version   int                          `json:"version"`
	ID        string                       `json:"id"`
	Kind      cleanupOperationKind         `json:"kind"`
	Object    archivalMarkerObjectIdentity `json:"object"`
	DependsOn []string                     `json:"depends_on"`
	Restrict  bool                         `json:"restrict"`
}

type cleanupOperationDigest string

func canonicalizeCleanupOperations(operations []cleanupOperationV1) ([]cleanupOperationV1, error) {
	operations = cloneCleanupOperations(operations)
	byID := make(map[string]int, len(operations))
	for idx, operation := range operations {
		if operation.Version != cleanupOperationCodecVersion {
			return nil, fmt.Errorf("cleanup operation %q has unsupported version %d", operation.ID, operation.Version)
		}
		if err := validateArchivalCodecString("cleanup operation ID", operation.ID); err != nil {
			return nil, err
		}
		if _, duplicate := byID[operation.ID]; duplicate {
			return nil, fmt.Errorf("duplicate cleanup operation ID %q", operation.ID)
		}
		byID[operation.ID] = idx
		if err := validateCleanupOperationIdentity(operation); err != nil {
			return nil, fmt.Errorf("cleanup operation %q: %w", operation.ID, err)
		}
	}

	indegree := make(map[string]int, len(operations))
	dependents := make(map[string][]string, len(operations))
	for idx := range operations {
		operation := &operations[idx]
		slices.Sort(operation.DependsOn)
		for dependencyIdx, dependencyID := range operation.DependsOn {
			if _, ok := byID[dependencyID]; !ok {
				return nil, fmt.Errorf("cleanup operation %q depends on missing operation %q",
					operation.ID, dependencyID)
			}
			if dependencyID == operation.ID {
				return nil, fmt.Errorf("cleanup operation %q has a self-edge", operation.ID)
			}
			if dependencyIdx > 0 && operation.DependsOn[dependencyIdx-1] == dependencyID {
				return nil, fmt.Errorf("cleanup operation %q has duplicate dependency edge to %q",
					operation.ID, dependencyID)
			}
			indegree[operation.ID]++
			dependents[dependencyID] = append(dependents[dependencyID], operation.ID)
		}
	}
	for operationID := range dependents {
		slices.Sort(dependents[operationID])
	}

	ready := make([]string, 0, len(operations))
	for _, operation := range operations {
		if indegree[operation.ID] == 0 {
			ready = append(ready, operation.ID)
		}
	}
	slices.Sort(ready)
	ordered := make([]cleanupOperationV1, 0, len(operations))
	for len(ready) > 0 {
		operationID := ready[0]
		ready = ready[1:]
		ordered = append(ordered, operations[byID[operationID]])
		for _, dependentID := range dependents[operationID] {
			indegree[dependentID]--
			if indegree[dependentID] == 0 {
				ready = append(ready, dependentID)
				slices.Sort(ready)
			}
		}
	}
	if len(ordered) != len(operations) {
		return nil, fmt.Errorf("cleanup operation dependency graph contains a cycle")
	}
	return ordered, nil
}

func marshalCanonicalCleanupOperations(operations []cleanupOperationV1) ([]byte, error) {
	ordered, err := canonicalizeCleanupOperations(operations)
	if err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(ordered)
	if err != nil {
		return nil, fmt.Errorf("marshaling canonical cleanup operations: %w", err)
	}
	return encoded, nil
}

func computeCleanupOperationDigest(operations []cleanupOperationV1) (cleanupOperationDigest, error) {
	canonicalOperations, err := marshalCanonicalCleanupOperations(operations)
	if err != nil {
		return "", err
	}
	hasher := sha256.New()
	_, _ = hasher.Write([]byte(cleanupDigestDomain))
	_, _ = hasher.Write([]byte{0})
	_, _ = hasher.Write(canonicalOperations)
	return cleanupOperationDigest(cleanupDigestPrefix + hex.EncodeToString(hasher.Sum(nil))), nil
}

func parseCleanupOperationDigest(encoded string) (cleanupOperationDigest, error) {
	if !strings.HasPrefix(encoded, cleanupDigestPrefix) {
		return "", fmt.Errorf("digest must use the %q prefix", cleanupDigestPrefix)
	}
	hexDigest := strings.TrimPrefix(encoded, cleanupDigestPrefix)
	if len(hexDigest) != sha256.Size*2 {
		return "", fmt.Errorf("SHA-256 digest must contain %d lowercase hexadecimal characters", sha256.Size*2)
	}
	if strings.ToLower(hexDigest) != hexDigest {
		return "", fmt.Errorf("SHA-256 digest must use lowercase hexadecimal")
	}
	decoded, err := hex.DecodeString(hexDigest)
	if err != nil || len(decoded) != sha256.Size {
		return "", fmt.Errorf("invalid SHA-256 digest encoding")
	}
	return cleanupOperationDigest(encoded), nil
}

func (d cleanupOperationDigest) String() string {
	return string(d)
}

func (d cleanupOperationDigest) MarshalText() ([]byte, error) {
	parsed, err := parseCleanupOperationDigest(string(d))
	if err != nil {
		return nil, err
	}
	return []byte(parsed), nil
}

func (d *cleanupOperationDigest) UnmarshalText(encoded []byte) error {
	parsed, err := parseCleanupOperationDigest(string(encoded))
	if err != nil {
		return err
	}
	*d = parsed
	return nil
}

func validateCleanupOperationIdentity(operation cleanupOperationV1) error {
	expectedKind := archivalMarkerObjectKind("")
	switch operation.Kind {
	case cleanupOperationKindDropTable:
		expectedKind = archivalMarkerObjectKindTable
	case cleanupOperationKindDropSequence:
		expectedKind = archivalMarkerObjectKindSequence
	case cleanupOperationKindDropFunction:
		expectedKind = archivalMarkerObjectKindFunction
	case cleanupOperationKindDropType:
		expectedKind = archivalMarkerObjectKindType
	case cleanupOperationKindDropCollation:
		expectedKind = archivalMarkerObjectKindCollation
	case cleanupOperationKindDropOperator:
		expectedKind = archivalMarkerObjectKindOperator
	case cleanupOperationKindDropSchema:
		expectedKind = archivalMarkerObjectKindSchema
	default:
		return fmt.Errorf("unsupported operation kind %q", operation.Kind)
	}
	if err := validateMarkerObjectKind(operation.Object, expectedKind); err != nil {
		return fmt.Errorf("object identity: %w", err)
	}
	return nil
}

func cloneCleanupOperations(operations []cleanupOperationV1) []cleanupOperationV1 {
	result := cloneOrEmpty(operations)
	for idx := range result {
		result[idx].Object = cloneMarkerObject(result[idx].Object)
		result[idx].DependsOn = cloneOrEmpty(result[idx].DependsOn)
	}
	return result
}
