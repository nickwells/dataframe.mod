package dataframe

import (
	"fmt"
)

// DF records the data and details of the type and name of each column
type DF struct {
	mci        MultiColInfo
	floatCols  [][]FloatVal
	intCols    [][]IntVal
	boolCols   [][]BoolVal
	stringCols [][]StringVal

	// TODO: Consider whether the error details sit properly in the dataframe
	// or whether they should be a return value from the ReadTable funcs
	errors    []error
	maxErrors int
	errCount  int64
}

// RowCount returns the number of rows in the dataframe
func (df *DF) RowCount() int {
	if len(df.mci.valIdx) == 0 {
		return 0
	}
	i := df.mci.valIdx[0]
	colType := df.mci.info[0].colType

	switch colType {
	case ColTypeBool:
		return len(df.boolCols[i])
	case ColTypeInt:
		return len(df.intCols[i])
	case ColTypeFloat:
		return len(df.floatCols[i])
	case ColTypeString:
		return len(df.stringCols[i])
	}

	panic(fmt.Sprintf("Unexpected column type: %d", colType))
}

// Row returns the i'th row from the dataframe. If i is negative or greater
// than or equal to the maximum number of rows then the values will all be NA
func (df *DF) Row(i int) *Row {
	if i < 0 || i >= df.RowCount() {
		return df.RowNA()
	}

	r := &Row{
		mci: df.mci.Clone(),
	}
	for cidx, cinfo := range df.mci.info {
		switch cinfo.colType {
		case ColTypeBool:
			r.rd.boolVals = append(r.rd.boolVals,
				df.boolCols[df.mci.valIdx[cidx]][i])
		case ColTypeInt:
			r.rd.intVals = append(r.rd.intVals,
				df.intCols[df.mci.valIdx[cidx]][i])
		case ColTypeFloat:
			r.rd.floatVals = append(r.rd.floatVals,
				df.floatCols[df.mci.valIdx[cidx]][i])
		case ColTypeString:
			r.rd.stringVals = append(r.rd.stringVals,
				df.stringCols[df.mci.valIdx[cidx]][i])
		}
	}
	return r
}

// RowZero returns a row with the same columns as the dataframe but with all
// columns having their zero value
func (df *DF) RowZero() *Row {
	rval := &Row{
		mci: df.mci.Clone(),
		rd: RowData{
			boolVals:   make([]BoolVal, 0),
			intVals:    make([]IntVal, 0),
			floatVals:  make([]FloatVal, 0),
			stringVals: make([]StringVal, 0),
		},
	}
	for _, ci := range df.mci.info {
		switch ci.colType {
		case ColTypeBool:
			rval.rd.boolVals = append(rval.rd.boolVals, BoolVal{})
		case ColTypeInt:
			rval.rd.intVals = append(rval.rd.intVals, IntVal{})
		case ColTypeFloat:
			rval.rd.floatVals = append(rval.rd.floatVals, FloatVal{})
		case ColTypeString:
			rval.rd.stringVals = append(rval.rd.stringVals, StringVal{})
		default:
			panic(dfErrorf("unexpected column type: %s", ci.colType))
		}
	}
	return rval
}

// RowNA returns a row with the same columns as the dataframe but with all
// columns having an NA value
func (df *DF) RowNA() *Row {
	rval := &Row{
		mci: df.mci.Clone(),
		rd: RowData{
			boolVals:   make([]BoolVal, 0),
			intVals:    make([]IntVal, 0),
			floatVals:  make([]FloatVal, 0),
			stringVals: make([]StringVal, 0),
		},
	}
	for _, ci := range df.mci.info {
		switch ci.colType {
		case ColTypeBool:
			rval.rd.boolVals = append(rval.rd.boolVals, BoolVal{IsNA: true})
		case ColTypeInt:
			rval.rd.intVals = append(rval.rd.intVals, IntVal{IsNA: true})
		case ColTypeFloat:
			rval.rd.floatVals = append(rval.rd.floatVals, FloatVal{IsNA: true})
		case ColTypeString:
			rval.rd.stringVals = append(rval.rd.stringVals,
				StringVal{IsNA: true})
		default:
			panic(dfErrorf("unexpected column type: %s", ci.colType))
		}
	}
	return rval
}

// Clone creates an empty copy of the dataframe with the same column details
// and column instances but with no data. The error values are all set to
// their respective zero values.
func (df *DF) Clone() *DF {
	cloneVal := &DF{
		mci:        df.mci.Clone(),
		floatCols:  make([][]FloatVal, len(df.floatCols)),
		boolCols:   make([][]BoolVal, len(df.boolCols)),
		intCols:    make([][]IntVal, len(df.intCols)),
		stringCols: make([][]StringVal, len(df.stringCols)),
	}
	return cloneVal
}

// assertTypeByName checks that actual == want and returns an error if not.
func assertTypeByName(actual, want ColType, name string) error {
	if actual != want {
		return dfErrorf("The column named %q is of type %q not %q",
			name, actual, want)
	}
	return nil
}

// assertTypeByIdx checks that actual == want and returns an error if not.
func assertTypeByIdx(actual, want ColType, idx int) error {
	if actual != want {
		return dfErrorf("The column with index %d is of type %q not %q",
			idx, actual, want)
	}
	return nil
}

// FloatColByName returns the slice of FloatVals for the named column. The
// error is non-nil if there is a problem (no such column or it's not a float
// column)
func (df DF) FloatColByName(name string) ([]FloatVal, error) {
	i, ok := df.mci.nameToCol[name]
	if !ok {
		return nil, dfErrorf("Unknown column name: %q", name)
	}

	ci := df.mci.info[i]
	if err := assertTypeByName(ci.colType, ColTypeFloat, name); err != nil {
		return nil, err
	}

	return df.floatCols[df.mci.valIdx[i]], nil
}

// FloatColByIdx returns the slice of FloatVals for the indexed column. The
// error is non-nil if there is a problem (no such column or it's not a float
// column)
func (df DF) FloatColByIdx(i int) ([]FloatVal, error) {
	if i < 0 || i >= len(df.mci.info) {
		return nil, dfErrorf("There is no column %d (valid range: 0-%d)",
			i, len(df.mci.info)-1)
	}

	ci := df.mci.info[i]
	if err := assertTypeByIdx(ci.colType, ColTypeFloat, i); err != nil {
		return nil, err
	}

	return df.floatCols[df.mci.valIdx[i]], nil
}

// BoolColByName returns the slice of BoolVals for the named column. The
// error is non-nil if there is a problem (no such column or it's not a bool
// column)
func (df DF) BoolColByName(name string) ([]BoolVal, error) {
	i, ok := df.mci.nameToCol[name]
	if !ok {
		return nil, dfErrorf("Unknown column name: %q", name)
	}

	ci := df.mci.info[i]
	if err := assertTypeByName(ci.colType, ColTypeBool, name); err != nil {
		return nil, err
	}

	return df.boolCols[df.mci.valIdx[i]], nil
}

// BoolColByIdx returns the slice of BoolVals for the indexed column. The
// error is non-nil if there is a problem (no such column or it's not a bool
// column)
func (df DF) BoolColByIdx(i int) ([]BoolVal, error) {
	if i < 0 || i >= len(df.mci.info) {
		return nil, dfErrorf("There is no column %d (valid range: 0-%d)",
			i, len(df.mci.info)-1)
	}

	ci := df.mci.info[i]
	if err := assertTypeByIdx(ci.colType, ColTypeBool, i); err != nil {
		return nil, err
	}

	return df.boolCols[df.mci.valIdx[i]], nil
}

// IntColByName returns the slice of IntVals for the named column. The error
// is non-nil if there is a problem (no such column or it's not an int
// column)
func (df DF) IntColByName(name string) ([]IntVal, error) {
	i, ok := df.mci.nameToCol[name]
	if !ok {
		return nil, dfErrorf("Unknown column name: %q", name)
	}

	ci := df.mci.info[i]
	if err := assertTypeByName(ci.colType, ColTypeInt, name); err != nil {
		return nil, err
	}

	return df.intCols[df.mci.valIdx[i]], nil
}

// IntColByIdx returns the slice of IntVals for the indexed column. The error
// is non-nil if there is a problem (no such column or it's not an int
// column)
func (df DF) IntColByIdx(i int) ([]IntVal, error) {
	if i < 0 || i >= len(df.mci.info) {
		return nil, dfErrorf("There is no column %d (valid range: 0-%d)",
			i, len(df.mci.info)-1)
	}

	ci := df.mci.info[i]
	if err := assertTypeByIdx(ci.colType, ColTypeInt, i); err != nil {
		return nil, err
	}

	return df.intCols[df.mci.valIdx[i]], nil
}

// StringColByName returns the slice of StringVals for the named column. The
// error is non-nil if there is a problem (no such column or it's not a
// string column)
func (df DF) StringColByName(name string) ([]StringVal, error) {
	i, ok := df.mci.nameToCol[name]
	if !ok {
		return nil, dfErrorf("Unknown column name: %q", name)
	}

	ci := df.mci.info[i]
	if err := assertTypeByName(ci.colType, ColTypeString, name); err != nil {
		return nil, err
	}

	return df.stringCols[df.mci.valIdx[i]], nil
}

// StringColByIdx returns the slice of StringVals for the indexed column. The
// error is non-nil if there is a problem (no such column or it's not a
// string column)
func (df DF) StringColByIdx(i int) ([]StringVal, error) {
	if i < 0 || i >= len(df.mci.info) {
		return nil, dfErrorf("There is no column %d (valid range: 0-%d)",
			i, len(df.mci.info)-1)
	}

	ci := df.mci.info[i]
	if err := assertTypeByIdx(ci.colType, ColTypeString, i); err != nil {
		return nil, err
	}

	return df.stringCols[df.mci.valIdx[i]], nil
}

// (df DF) String converts a DataFrame to a string
func (df DF) String() string {
	return fmt.Sprintf("%d rows, %d columns", df.RowCount(), len(df.mci.info))
}

type DFOpt func(*DF) error

// New returns a new DataFrame
func NewDF(opts ...DFOpt) (*DF, error) {
	df := &DF{
		maxErrors: 500,
	}

	for _, o := range opts {
		err := o(df)
		if err != nil {
			return nil, err
		}
	}

	return df, nil
}

func MaxErrors(n int) DFOpt {
	return func(df *DF) error {
		if n < 0 {
			return dfError(
				fmt.Sprintf("the maximum number of errors must be >= 0: %d", n))
		}
		df.maxErrors = n
		return nil
	}
}

func ColNames(names []string) DFOpt {
	return func(df *DF) error {
		return df.SetColNames(names...)
	}
}

// ErrCount returns the number of errors that were detected while constructing
// the DataFrame. Note that this can be greater than the number of entries in
// the slice returned by Errors
func (df DF) ErrCount() int64 {
	return df.errCount
}

// Errors returns the slice of errors that were detected while constructing
// the DataFrame. Note that this will only be the first maxErrors errors.
func (df DF) Errors() []error {
	return df.errors
}

// ColCount returns the number of columns that the DataFrame has
func (df DF) ColCount() int {
	return len(df.mci.info)
}

// Columns returns a copy of the details of the columns that the DataFrame has
func (df DF) Columns() []ColInfo {
	return cloneColInfoSlice(df.mci.info)
}

// ColInfoByName returns the column detail of the named column or an error if
// there is no column with that name
func (df DF) ColInfoByName(name string) (ColInfo, error) {
	i, ok := df.mci.nameToCol[name]
	if !ok {
		return ColInfo{}, dfErrorf("Unknown column name: %q", name)
	}
	return df.mci.info[i], nil
}

// ColInfoByIdx returns the column detail of the indexed column or an error if
// there is no column with that idx
func (df DF) ColInfoByIdx(i int) (ColInfo, error) {
	if i < 0 || i >= len(df.mci.info) {
		return ColInfo{}, dfErrorf("There is no column %d (valid range: 0-%d)",
			i, len(df.mci.info)-1)
	}
	return df.mci.info[i], nil
}

// SetColNames sets the names of the columns of the DataFrame to the given names
func (df *DF) SetColNames(names ...string) error {
	if len(names) == 0 {
		err := ErrNoNamesGiven
		df.addError(err)
		return err
	}

	if len(df.mci.info) == 0 {
		df.mci.info = make([]ColInfo, len(names))
	} else if len(df.mci.info) != len(names) {
		err := dfError(fmt.Sprintf(
			"the number of columns (%d) and number of names (%d) differ",
			len(df.mci.info), len(names)))
		df.addError(err)
		return err
	}

	colNameToIdx := make(map[string]int, len(names))

	for i, name := range names {
		if dup, exists := colNameToIdx[name]; exists {
			err := dfError(fmt.Sprintf(
				"duplicate column name: %q is used for columns %d and %d",
				name, dup, i))
			df.addError(err)
			return err
		}
		colNameToIdx[name] = i
	}

	for i, name := range names {
		df.mci.info[i].name = name
	}
	df.mci.nameToCol = colNameToIdx

	return nil
}

// setIdx will set the index of the column to the next free column
// of the appropriate type
func (df *DF) setIdx(i int) {
	var idx int = -1
	switch df.mci.info[i].colType {
	case ColTypeUnknown:
	case ColTypeBool:
		idx = len(df.boolCols)
		df.boolCols = append(df.boolCols, make([]BoolVal, 0))
	case ColTypeInt:
		idx = len(df.intCols)
		df.intCols = append(df.intCols, make([]IntVal, 0))
	case ColTypeFloat:
		idx = len(df.floatCols)
		df.floatCols = append(df.floatCols, make([]FloatVal, 0))
	case ColTypeString:
		idx = len(df.stringCols)
		df.stringCols = append(df.stringCols, make([]StringVal, 0))
	default:
		panic(dfErrorf("Unexpected column type: %q", df.mci.info[i].colType))
	}
	df.mci.valIdx = append(df.mci.valIdx, idx)
}

// SetColTypes sets the types of the columns of the DataFrame to the given types
func (df *DF) SetColTypes(types ...ColType) error {
	if len(types) == 0 {
		err := ErrNoTypesGiven
		df.addError(err)
		return err
	}

	for i, colType := range types {
		if colType < ColTypeUnknown || colType >= ColTypeMaxVal {
			err := dfErrorf("bad column type: column: %d type: %q", i, colType)
			df.addError(err)
			return err
		}
	}

	if len(df.mci.info) == 0 {
		df.mci.info = make([]ColInfo, len(types))
	}

	if len(df.mci.info) != len(types) {
		err := dfErrorf(
			"the number of columns (%d) and number of types (%d) differ",
			len(df.mci.info), len(types))
		df.addError(err)
		return err
	}

	for i, colType := range types {
		df.mci.info[i].colType = colType
		df.setIdx(i)
	}

	return nil
}

// addError adds an error to the set of DataFrame errors, it applies the
// check on maxErrors and increments the errCount
func (df *DF) addError(err error) {
	df.errCount++
	if len(df.errors) < df.maxErrors {
		df.errors = append(df.errors, err)
	}
}

// AddRow will add a new row to the DataFrame
func (df *DF) AddRow(row *Row) error {
	if err := df.mci.Match(row.mci); err != nil {
		return err
	}

	for i, ci := range df.mci.info {
		vi := df.mci.valIdx[i]
		switch ci.colType {
		case ColTypeBool:
			df.boolCols[vi] = append(df.boolCols[vi], row.rd.boolVals[vi])
		case ColTypeFloat:
			df.floatCols[vi] = append(df.floatCols[vi], row.rd.floatVals[vi])
		case ColTypeInt:
			df.intCols[vi] = append(df.intCols[vi], row.rd.intVals[vi])
		case ColTypeString:
			df.stringCols[vi] = append(df.stringCols[vi], row.rd.stringVals[vi])
		}
	}
	return nil
}

// AddRowFromText will add a new row to the DataFrame
func (df *DF) AddRowFromText(cols []string) {
	if len(cols) != len(df.mci.info) {
		df.addError(dfErrorf("dataframe has %d columns, %d are being added",
			len(df.mci.info), len(cols)))
		return
	}

	for i, c := range df.mci.info {
		valIdx := df.mci.valIdx[i]
		var err error

		switch c.colType {
		case ColTypeBool:
			var v BoolVal
			err = v.SetVal(cols[i])
			df.boolCols[valIdx] = append(df.boolCols[valIdx], v)
		case ColTypeInt:
			var v IntVal
			err = v.SetVal(cols[i])
			df.intCols[valIdx] = append(df.intCols[valIdx], v)
		case ColTypeFloat:
			var v FloatVal
			err = v.SetVal(cols[i])
			df.floatCols[valIdx] = append(df.floatCols[valIdx], v)
		case ColTypeString:
			v := StringVal{Val: cols[i]}
			df.stringCols[valIdx] = append(df.stringCols[valIdx], v)
		default:
			panic(dfErrorf("Unexpected column type: %q", c.colType))
		}

		if err != nil {
			df.addError(dfErrorf("data row: %d column: %d: %s",
				df.RowCount(), i, err))
		}
	}
}

// AddRowsFromText will add a new row to the DataFrame for each of the rows
// of text
func (df *DF) AddRowsFromText(rows [][]string) {
	for _, row := range rows {
		df.AddRowFromText(row)
	}
}
