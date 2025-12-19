package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"k8s-log-etl/internal/report"
	"k8s-log-etl/internal/stages"
	"log"
	"os"
	"strings"
)

func main() {
	rep := report.NewReport()
	file, err := os.Open("examples/k8s_logs.jsonl")

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

		rep.TotalLines++

		var js map[string]interface{}
		jsonerr := json.Unmarshal([]byte(line), &js)

		if jsonerr != nil {
			rep.JSONFailed++
		} else {
			rep.JSONParsed++
			normalized, normerr := stages.Normalize(js)

			if normerr != nil {
				rep.NormalizedFailed++
				fmt.Fprintf(os.Stderr, "Normalization error: %v\n", normerr)
			} else {
				rep.NormalizedOK++
				rep.AddLevel(normalized.Level)
				rep.AddService(normalized.Service)

				level := strings.ToUpper(normalized.Level)
				if level == "WARN" || level == "ERROR" {
					if err := enc.Encode(normalized); err != nil {
						fmt.Fprintf(os.Stderr, "write output error: %v\n", err)
					} else {
						rep.WrittenOK++
					}
				}
			}
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	if err := rep.WriteJSON("report.json"); err != nil {
		log.Fatal(err)
	}

	fmt.Printf(
		"Total Lines: %d, JSON Parsed: %d, JSON Failed: %d, Normalized OK: %d, Normalized Failed: %d, Written OK: %d\n",
		rep.TotalLines,
		rep.JSONParsed,
		rep.JSONFailed,
		rep.NormalizedOK,
		rep.NormalizedFailed,
		rep.WrittenOK,
	)

}
