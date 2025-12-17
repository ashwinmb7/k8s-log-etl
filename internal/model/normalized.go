package model

type Normalized struct {
	TS        string
	Level     string
	Service   string
	Namespace string
	Pod       string
	Node      string
	Message   string
	TraceID   string
	Fields    map[string]any
}
