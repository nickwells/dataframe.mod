package dataframe

import "fmt"

// ColType records the type of the data in the column and hence
// which set of Values holds the column data
type ColType uint

// ColTypeUnknown indicates that the column type is unknown
// ColTypeBool indicates that the column holds bools
// ColTypeInt indicates that the column holds integers
// ColTypeFloat indicates that the column holds floats
// ColTypeString indicates that the column holds strings
// ColTypeMaxVal is a guard value used to ensure validity
const (
	ColTypeUnknown ColType = iota
	ColTypeBool
	ColTypeInt
	ColTypeFloat
	ColTypeString
	ColTypeMaxVal

	BitFlagBool  = uint64(1) << ColTypeBool
	BitFlagInt   = uint64(1) << ColTypeInt
	BitFlagFloat = uint64(1) << ColTypeFloat

	BitmaskNonStringDataTypes = BitFlagBool | BitFlagInt | BitFlagFloat
)

// ColInfo records information about an individual column
type ColInfo struct {
	name    string  // column name
	colType ColType // data type
}

// String returns a formatted string describing the ColInfo value
func (ci ColInfo) String() string {
	return ci.name + "(" + ci.colType.String() + ")"
}

// Check returns an error if the colType is unknown or invalid or if the name
// is blank. It returns nil otherwise
func (ci ColInfo) Check() error {
	if ci.name == "" {
		return dfErrorf("The column name is invalid: it must not be blank")
	}
	if ci.colType <= ColTypeUnknown || ci.colType >= ColTypeMaxVal {
		return dfErrorf("The column type is invalid: %s", ci.colType)
	}
	return nil
}

// MultiColInfo records information about a collection of columns. Each entry
// in info corresponds to a column in the RowData. The corresponding entry
// in valIdx gives the index into the slice of data of that type in the
// type-specific slice in the associated RowData
type MultiColInfo struct {
	info      []ColInfo
	valIdx    []int
	nameToCol map[string]int
}

// Add checks that the ColInfo is valid and that the name is unique and then
// adds the new column. It returns nil if there were no problems or a
// descriptive error otherwise.
func (mci *MultiColInfo) Add(ci ColInfo) error {
	err := ci.Check()
	if err != nil {
		return err
	}
	if otherIdx, exists := mci.nameToCol[ci.name]; exists {
		return dfErrorf("Column name already used: %s", mci.ColDesc(otherIdx))
	}

	count := 0
	for _, existingCi := range mci.info {
		if ci.colType == existingCi.colType {
			count++
		}
	}

	mci.valIdx = append(mci.valIdx, count)
	mci.nameToCol[ci.name] = len(mci.info)
	mci.info = append(mci.info, ci)

	return nil
}

// NewMultiColInfo creates a MultiColInfo record, populating it with the
// supplied ColInfo records. It will return a pointer to the new record and a
// nil error if no errors are found or a nil pointer and an error if there
// are any problems. All the columns must have a unique, non-empty name and a
// known type.
func NewMultiColInfo(cis ...ColInfo) (*MultiColInfo, error) {
	mci := &MultiColInfo{
		nameToCol: make(map[string]int),
	}

	for i, ci := range cis {
		err := mci.Add(ci)
		if err != nil {
			return nil, dfErrorf("Column %d (%q): %s", i, ci.name, err)
		}
	}

	return mci, nil
}

// Match returns an error showing the first detected difference between mci
// and other or nil if they are identical
func (mci MultiColInfo) Match(other MultiColInfo) error {
	if len(mci.info) != len(other.info) {
		return fmt.Errorf("Differing numbers of columns: %d != %d",
			len(mci.info), len(other.info))
	}
	for i, ci := range mci.info {
		if ci.colType != other.info[i].colType {
			return fmt.Errorf("%s has a different type: %s != %s",
				mci.ColDesc(i), ci.colType, other.info[i].colType)
		}
		if ci.name != other.info[i].name {
			return fmt.Errorf("%s has a different name: %q != %q",
				mci.ColDesc(i), ci.name, other.info[i].name)
		}
	}
	for i, idx := range mci.valIdx {
		if idx != other.valIdx[i] {
			return fmt.Errorf("%s has a different value idx: %d != %d",
				mci.ColDesc(i), idx, other.valIdx[i])
		}
	}
	return nil
}

// ColDesc returns a string describing the column
func (mci MultiColInfo) ColDesc(i int) string {
	if i < 0 || i >= len(mci.info) {
		return fmt.Sprintf("invalid column index: %d", i)
	}
	return fmt.Sprintf("Column %d (%q: %q)",
		i, mci.info[i].name, mci.info[i].colType)
}

// Clone builds a copy of the MultiColInfo value
func (mci MultiColInfo) Clone() MultiColInfo {
	return MultiColInfo{
		info:      cloneColInfoSlice(mci.info),
		valIdx:    cloneIntSlice(mci.valIdx),
		nameToCol: cloneColNameMap(mci.nameToCol),
	}
}

// cloneColInfoSlice creates a new slice of ColInfo and copies the values
// from the supplied slice into it
func cloneColInfoSlice(ci []ColInfo) []ColInfo {
	rval := make([]ColInfo, len(ci))
	copy(rval, ci)
	return rval
}

// cloneIntSlice creates a new slice of int and copies the values
// from the supplied slice into it
func cloneIntSlice(vi []int) []int {
	rval := make([]int, len(vi))
	copy(rval, vi)
	return rval
}

// cloneColNameMap creates a new map of strings to ints and copies the values
// from the supplied map into it
func cloneColNameMap(m map[string]int) map[string]int {
	rval := make(map[string]int)
	for k, v := range m {
		rval[k] = v
	}
	return rval
}

// Diff returns true if ci is different from ci2
func (ci *ColInfo) Diff(ci2 *ColInfo) bool {
	if ci.colType != ci2.colType {
		return true
	}
	if ci.name != ci2.name {
		return true
	}
	return false
}

// NewColInfo returns a column with the name and type set
func NewColInfo(name string, colType ColType) ColInfo {
	if colType <= ColTypeUnknown || colType >= ColTypeMaxVal {
		panic(dfErrorf("Unexpected column type: %q", colType))
	}
	return ColInfo{
		name:    name,
		colType: colType,
	}
}

// Name returns the column name
func (ci ColInfo) Name() string { return ci.name }

// ColType returns the column's type
func (ci ColInfo) ColType() ColType { return ci.colType }
