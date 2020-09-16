package dataframe

// StringVal records a string value and an indication of whether the value
// was available. This allows for missing values in the data
type StringVal struct {
	Val  string
	IsNA bool
}
