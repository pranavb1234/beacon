// main.go is the stub subscriber CLI.
// Full implementation arrives in Phase 1 once gRPC code is generated.
package main

import (
	"log"
)

func main() {
	// TODO(phase1): parse flags (broker addr, topic, group-id, start-seq),
	//               dial gRPC, open Subscribe stream, print events, send ACKs.
	log.Fatal("subscriber: not yet implemented — coming in Phase 1")
}
