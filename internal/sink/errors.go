package sink

type ErrorRecord struct {
	Line  int    `json:"line"`
	Error string `json:"error"`
	Stage string `json:"stage"`
	Raw   string `json:"raw"`
}
