// Code generated by "stringer -type=ColType -trimprefix=ColType"; DO NOT EDIT.

package dataframe

import "strconv"

func _() {
	// An "invalid array index" compiler error signifies that the constant values have changed.
	// Re-run the stringer command to generate them again.
	var x [1]struct{}
	_ = x[ColTypeUnknown-0]
	_ = x[ColTypeBool-1]
	_ = x[ColTypeInt-2]
	_ = x[ColTypeFloat-3]
	_ = x[ColTypeString-4]
	_ = x[ColTypeMaxVal-5]
}

const _ColType_name = "UnknownBoolIntFloatStringMaxVal"

var _ColType_index = [...]uint8{0, 7, 11, 14, 19, 25, 31}

func (i ColType) String() string {
	if i >= ColType(len(_ColType_index)-1) {
		return "ColType(" + strconv.FormatInt(int64(i), 10) + ")"
	}
	return _ColType_name[_ColType_index[i]:_ColType_index[i+1]]
}
