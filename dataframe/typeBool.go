package dataframe

import "strconv"

// BoolVal records a bool value and an indication of whether the value
// was available. This allows for missing values in the data
type BoolVal struct {
	Val  bool
	IsNA bool
}

// SetVal will parse the string and set the value accordingly. If the
// parsing fails IsNA will be set to true and a non-nil error will be
// returned, otherwise the error will be nil.
func (v *BoolVal) SetVal(s string) error {
	var err error
	v.Val, err = strconv.ParseBool(s)
	if err != nil {
		v.IsNA = true
	}
	return err
}
