// edge: the small supporting services that surround the GridGain clusters in each simulated DC.
//
//	edge lb        per-DC HTTP load balancer with cross-DC failover
//	edge wan       inter-DC link emulator (latency / jitter / partition) for GridGain DCR and failover traffic
//	edge loadgen   steady payments traffic generator for the DC's users
//	edge agent     per-DC control + status agent used by the dashboard
package main

import (
	"fmt"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("usage: edge lb|wan|loadgen|agent")
		os.Exit(2)
	}
	switch os.Args[1] {
	case "lb":
		runLB()
	case "wan":
		runWAN()
	case "loadgen":
		runLoadgen()
	case "agent":
		runAgent()
	default:
		fmt.Println("unknown command", os.Args[1])
		os.Exit(2)
	}
}
