package main

import (
	"fmt"
	"math"
	"strings"

	"github.com/manifoldco/promptui"
)

func header(header string) string {
	const headerTargetWidth = 80

	if len(header) > headerTargetWidth {
		return header
	}

	if len(header) > 0 {
		header = fmt.Sprintf(" %s ", header)
	}
	hashTagsOnSide := int(math.Ceil(float64(headerTargetWidth-len(header)) / 2))

	rightHashTags := strings.Repeat("#", hashTagsOnSide)
	leftHashTags := rightHashTags
	if headerTargetWidth-len(header)-2*hashTagsOnSide > 0 {
		leftHashTags += "#"
	}
	return fmt.Sprintf("%s%s%s", leftHashTags, header, rightHashTags)
}

// MustContinuePrompt prompts the user if they want to continue, and returns an error otherwise.
// promptui requires the ContinueLabel to be one line
func mustContinuePrompt(continueLabel string) error {
	if len(continueLabel) == 0 {
		continueLabel = "Continue?"
	}
	if _, result, err := (&promptui.Select{
		Label: continueLabel,
		Items: []string{"No", "Yes"},
	}).Run(); err != nil {
		return err
	} else if result == "No" {
		return fmt.Errorf("user aborted")
	}
	return nil
}
