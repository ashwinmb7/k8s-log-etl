package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"k8s-log-etl/internal/report"
	"k8s-log-etl/internal/stages"
	"log"
	"os"
)

func main() {
	var totalLine int
	var failedLine int
	var parsedLine int

	file, err := os.Open("examples/k8s_logs.jsonl")

	rep := report.NewReport()

	if err != nil {
		log.Fatal(err)
	}

	defer file.Close()

	//Create scanner to read file line by line
	scanner := bufio.NewScanner(file)

	enc := json.NewEncoder(os.Stdout)

	for scanner.Scan() {

		line := scanner.Text()

		if len(line) == 0 {
			continue
		}

		totalLine++

		var js map[string]interface{}
		jsonerr := json.Unmarshal([]byte(line), &js)

		if jsonerr != nil {
			failedLine++
		} else {
			normalized, normerr := stages.Normalize(js)

			if normerr != nil {
				failedLine++
				fmt.Fprintf(os.Stderr, "Normalization error: %v\n", normerr)
			} else {
				enc.Encode(normalized)
			}

			parsedLine++
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	if err := rep.WriteJSON("report.json"); err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Total Lines: %d, Parsed Lines: %d, Failed Lines: %d\n", totalLine, parsedLine, failedLine)

}
