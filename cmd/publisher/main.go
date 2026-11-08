// main.go is the stub publisher CLI.
// Full implementation arrives in Phase 1 once gRPC code is generated.
package main

import (
	"log"
)

func main() {
	// TODO(phase1): parse flags (broker addr, topic, payload, rate),
	//               dial gRPC, call PubSubService.Publish in a loop.
	log.Fatal("publisher: not yet implemented — coming in Phase 1")
}
