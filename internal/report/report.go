package report

import (
	"encoding/json"
	"os"
)

// Report aggregates ETL processing statistics.
type Report struct {
	TotalLines       int            `json:"total_lines"`
	JSONFailed       int            `json:"json_failed"`
	JSONParsed       int            `json:"json_parsed"`
	NormalizedOK     int            `json:"normalized_ok"`
	NormalizedFailed int            `json:"normalized_failed"`
	WrittenOK        int            `json:"written_ok"`
	ByLevel          map[string]int `json:"by_level"`
	ByService        map[string]int `json:"by_service"`
}

// NewReport initializes a Report with maps ready to use.
func NewReport() *Report {
	return &Report{
		ByLevel:   make(map[string]int),
		ByService: make(map[string]int),
	}
}

// AddLevel increments the count for a log level.
func (r *Report) AddLevel(level string) {
	if level == "" {
		return
	}
	r.ByLevel[level]++
}

// AddService increments the count for a service.
func (r *Report) AddService(service string) {
	if service == "" {
		return
	}
	r.ByService[service]++
}

// WriteJSON writes the report to a JSON file at the given path.
func (r *Report) WriteJSON(path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return enc.Encode(r)
}
