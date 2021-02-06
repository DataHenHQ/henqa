package customtypes

type ErrorStat struct {
	Field            string  `json:"field"`
	ErrorType        string  `json:"error_type"`
	ErrorDescription string  `json:"error_description"`
	ErrorCount       uint64  `json:"error_count"`
	RecordCount      uint64  `json:"record_count"`
	ErrorPercent     float64 `json:"error_percent"`
}

type ErrorStats map[string]*ErrorStat

func (e *ErrorStat) IncErrCount() {
	e.ErrorCount = e.ErrorCount + 1
}

func (e *ErrorStat) CalculatePercentage() {
	e.ErrorPercent = float64(e.ErrorCount) / float64(e.RecordCount) * 100.0
}
