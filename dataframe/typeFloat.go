package dataframe

import "strconv"

// FloatVal records a float64 value and an indication of whether the value
// was available. This allows for missing values in the data
type FloatVal struct {
	Val  float64
	IsNA bool
}

// SetVal will parse the string and set the value accordingly. If the
// parsing fails IsNA will be set to true and a non-nil error will be
// returned, otherwise the error will be nil.
func (v *FloatVal) SetVal(s string) error {
	var err error
	v.Val, err = strconv.ParseFloat(s, 64)
	if err != nil {
		v.IsNA = true
	}
	return err
}
