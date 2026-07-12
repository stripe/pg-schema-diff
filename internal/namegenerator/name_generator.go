package namegenerator

import (
	"encoding/hex"
	"fmt"
	"io"
)

const randomSuffixBytes = 5

var adjectives = [...]string{
	"amber", "brisk", "calm", "clever", "coral", "crisp", "daring", "eager",
	"fair", "fleet", "gentle", "golden", "happy", "hardy", "hazel", "ivory",
	"jolly", "keen", "lively", "lucid", "lucky", "mellow", "merry", "nimble",
	"noble", "olive", "patient", "peach", "plucky", "proud", "quick", "quiet",
	"rapid", "ready", "rosy", "royal", "ruby", "sandy", "sharp", "silver",
	"smart", "steady", "sunny", "swift", "tidy", "vivid", "warm", "wise",
	"witty", "young", "zesty", "bold", "bright", "cool", "cozy", "fresh",
	"grand", "green", "light", "misty", "neat", "red", "soft", "true",
}

var nouns = [...]string{
	"badger", "beaver", "bison", "bobcat", "canary", "caribou", "cobra", "condor",
	"cougar", "coyote", "crane", "dolphin", "eagle", "falcon", "ferret", "finch",
	"fox", "gecko", "heron", "ibis", "jaguar", "kestrel", "koala", "lark",
	"lemur", "lion", "lynx", "marten", "moose", "newt", "otter", "owl",
	"panda", "parrot", "penguin", "puma", "quail", "raven", "robin", "salmon",
	"seal", "shark", "sparrow", "stoat", "swan", "swift", "tiger", "toucan",
	"trout", "turtle", "viper", "walrus", "whale", "wolf", "wombat", "yak",
	"zebra", "alpaca", "antelope", "beetle", "buffalo", "rabbit", "ram", "wren",
}

// Generate returns a readable lowercase identifier with 52 bits of entropy.
func Generate(random io.Reader) (string, error) {
	var data [2 + randomSuffixBytes]byte
	if _, err := io.ReadFull(random, data[:]); err != nil {
		return "", fmt.Errorf("reading random name data: %w", err)
	}

	return adjectives[int(data[0])%len(adjectives)] + "_" +
		nouns[int(data[1])%len(nouns)] + "_" +
		hex.EncodeToString(data[2:]), nil
}
