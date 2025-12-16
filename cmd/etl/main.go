package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
)

func main() {
	var totalLine int
	var failedLine int
	var parsedLine int

	file, err := os.Open("examples/k8s_logs.jsonl")

	if err != nil {
		log.Fatal(err)
	}

	defer file.Close()

	//Create scanner to read file line by line
	scanner := bufio.NewScanner(file)

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
			parsedLine++
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Total Lines: %d, Parsed Lines: %d, Failed Lines: %d\n", totalLine, parsedLine, failedLine)
}
