package dataframe

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"regexp"
	"strconv"

	"github.com/nickwells/check.mod/v2/check"
	"github.com/nickwells/location.mod/location"
)

// lineHandler is a function type for performing successive operations on the
// lines in a file
type lineHandler func(dfr *DFReader, state *dfReadState, df *DF) (
	skip bool, err error)

// dfReadState holds the dynamic details of the current state of the reader
type dfReadState struct {
	loc         *location.L
	dataLineNum int64
	line        string
	cols        []string
	cache       [][]string
}

// newDFReadState creates a dfReadState in an initial state
func newDFReadState(dfr *DFReader, source string) *dfReadState {
	state := &dfReadState{
		loc: location.New(source),
	}

	if dfr.initialLines > 0 {
		state.cache = make([][]string, 0, dfr.initialLines)
	}

	return state
}

const defaultSplitPattern = `\s+`

// DFReader holds the configurable options for building a dataframe from
// an io.Reader
type DFReader struct {
	hasHeader      bool
	skipBlankLines bool
	allowErrors    bool

	commentRegex *regexp.Regexp

	colNames     []string
	colTypes     []ColType
	skipLines    int64
	initialLines int64
	skipCols     map[int]bool

	maxCols    int
	splitRegex *regexp.Regexp
}

type DFReaderOpt func(*DFReader) error

// NewDFReader creates a new DFReader applying the options and returning an
// error if any of the option functions fails
func NewDFReader(opts ...DFReaderOpt) (*DFReader, error) {
	dfr := &DFReader{
		initialLines: 10,
		splitRegex:   regexp.MustCompile(defaultSplitPattern),
		skipCols:     make(map[int]bool),
		maxCols:      -1,
	}
	for _, o := range opts {
		err := o(dfr)
		if err != nil {
			return nil, err
		}
	}

	if dfr.initialLines == 0 && len(dfr.colTypes) == 0 {
		return nil, ErrNoTypeInfo
	}

	if dfr.hasHeader {
		dfr.initialLines++
	}

	if len(dfr.colTypes) != 0 {
		dfr.initialLines = 0
	}

	return dfr, nil
}

// HasHeader will cause the DFReader to treat the first line
// after any skipped lines (or blank lines if ignored) as a series of column
// headings and will treat the names as column names
func HasHeader(dfr *DFReader) error {
	dfr.hasHeader = true
	if len(dfr.colNames) != 0 {
		return ErrHasNamesAndHeader
	}
	return nil
}

// SkipBlankLines will cause the DFReader to ignore any blank
// lines
func SkipBlankLines(dfr *DFReader) error {
	dfr.skipBlankLines = true
	return nil
}

// AllowErrors will cause the DFReader to not return on error. The returned
// dataframe may have some associated errors
func AllowErrors(dfr *DFReader) error {
	dfr.allowErrors = true
	return nil
}

// DFRSkipCols returns a function which will specify the columns in the
// source data to be skipped. Note that columns are numbered from zero not
// one.
func DFRSkipCols(skips ...int) DFReaderOpt {
	if len(skips) == 0 {
		panic(ErrNoSkipColsGiven)
	}

	if err := check.SliceAll[[]int](check.ValGE(int(0)))(skips); err != nil {
		panic(dfErrorf("a negative skip index has been given: %s", err))
	}

	if err := check.SliceHasNoDups(skips); err != nil {
		panic(dfErrorf("a duplicate skip index has been given: %s", err))
	}

	return func(dfr *DFReader) error {
		if len(dfr.skipCols) != 0 {
			return ErrSkipIndexesAlreadySet
		}

		for i, si := range skips {
			// we must repeat the duplicate test in case this is called twice
			if _, ok := dfr.skipCols[si]; ok {
				return dfErrorf(
					"a duplicate skip index has been given: skips[%d] == %d",
					i, si)
			}
			dfr.skipCols[si] = true
		}

		return nil
	}
}

// DFRColNames returns a function which will specify the column names
// for the DFReader to use
func DFRColNames(names ...string) DFReaderOpt {
	return func(dfr *DFReader) error {
		if dfr.hasHeader {
			return ErrHasNamesAndHeader
		}

		if len(names) == 0 {
			return ErrNoNamesGiven
		}

		if len(dfr.colNames) != 0 {
			return ErrNamesAlreadySet
		}

		if len(dfr.colTypes) != 0 && len(dfr.colTypes) != len(names) {
			return dfErrorf(
				"the number of column types (%d) and names (%d) differ",
				len(dfr.colTypes), len(names))
		}

		dfr.colNames = names

		return nil
	}
}

// DFRColTypes returns a function which will specify the column types
// for the DFReader to use
func DFRColTypes(types ...ColType) DFReaderOpt {
	return func(dfr *DFReader) error {
		if len(types) == 0 {
			return ErrNoTypesGiven
		}

		if len(dfr.colTypes) != 0 {
			return ErrTypesAlreadySet
		}

		if len(dfr.colNames) != 0 && len(dfr.colNames) != len(types) {
			return dfErrorf(
				"the number of column types (%d) and names (%d) differ",
				len(types), len(dfr.colNames))
		}

		dfr.colTypes = types

		return nil
	}
}

// SkipLines returns a function which will specify the number of lines for
// the DFReader to skip at the start of the input. The default is zero. It
// will panic if the number of lines passed is less than 0.
func SkipLines(n int64) DFReaderOpt {
	if n < 0 {
		panic(dfErrorf("the number of lines to skip (%d) must be >= 0", n))
	}

	return func(dfr *DFReader) error {
		dfr.skipLines = n
		return nil
	}
}

// InitialLines returns a function which will specify the number of lines for
// the DFReader to read at the start of the input after any header. These
// lines are used to determine the number of columns and their data
// types. The default is 10
func InitialLines(n int64) DFReaderOpt {
	return func(dfr *DFReader) error {
		if n < 0 {
			return dfErrorf(
				"the number of lines to decide column type (%d) must be >= 0",
				n)
		}
		dfr.initialLines = n
		return nil
	}
}

// CommentPattern returns a function which will specify the comment pattern
// for the DFReader to use when stripping comments
func CommentPattern(pattern string) DFReaderOpt {
	return func(dfr *DFReader) error {
		var err error

		dfr.commentRegex, err = regexp.Compile(pattern)

		if err != nil {
			err = dfErrorf("the regexp to strip comments is invalid: %s", err)
		}

		return err
	}
}

// SplitPattern returns a function which will specify the regular expression
// used by the DFReader when splitting lines into columns.
func SplitPattern(pattern string) DFReaderOpt {
	return func(dfr *DFReader) error {
		var err error

		dfr.splitRegex, err = regexp.Compile(pattern)
		if err != nil {
			err = dfErrorf("the pattern for splitting lines is invalid: %s",
				err)
		}
		return err
	}
}

// ReadFile reads a file and converts the rows into a DataFrame.
func ReadFile(filename string, opts ...DFReaderOpt) (*DF, error) {
	dfr, err := NewDFReader(opts...)
	if err != nil {
		return nil, err
	}
	return dfr.ReadFile(filename)
}

// ReadFile reads from the named file and populates the dataframe
func (dfr *DFReader) ReadFile(filename string) (*DF, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	defer file.Close()

	return dfr.Read(file, "file: "+filename)
}

// setColNames sets the column names either according to the option
// values or else to their default values
func (dfr *DFReader) setColNames(state *dfReadState, df *DF) (bool, error) {
	if len(dfr.colNames) != 0 {
		return false, nil // the column names are already set
	}

	if dfr.hasHeader {
		return true, df.SetColNames(state.cols...)
	}

	names := make([]string, len(state.cols))
	for i := range state.cols {
		names[i] = fmt.Sprintf("V%d", i)
	}
	return false, df.SetColNames(names...)
}

// setColTypes sets the column names either according to the option
// values or else to their default values
func (dfr DFReader) setColTypes(df *DF, cache [][]string) error {
	if len(dfr.colTypes) != 0 {
		return nil // the column types are already set
	}

	return df.SetColTypes(guessColTypes(df.mci.info, cache)...)
}

// makeDF will create a dataframe and then populate those members that can be
// set from the DFReader values
func (dfr DFReader) makeDF() (*DF, error) {
	df, err := NewDF()
	if err != nil {
		return nil, err
	}

	if len(dfr.colNames) > 0 {
		err := df.SetColNames(dfr.colNames...)
		if err != nil {
			return nil, err
		}
	}

	if len(dfr.colTypes) > 0 {
		err := df.SetColTypes(dfr.colTypes...)
		if err != nil {
			return nil, err
		}
	}

	return df, nil
}

// canBeBool returns true if the ColTypeBool bit is set in v and false otherwise
func canBeBool(v uint64) bool { return v&BitFlagBool == BitFlagBool }

// canBeInt returns true if the ColTypeInt bit is set in v and false otherwise
func canBeInt(v uint64) bool { return v&BitFlagInt == BitFlagInt }

// canBeFloat returns true if the ColTypeFloat bit is set in v and false
// otherwise
func canBeFloat(v uint64) bool { return v&BitFlagFloat == BitFlagFloat }

// tryParse will try parsing each column in the rows slice with multiple parsing
// routines and set the bits in canBeTypes appropriately
func tryParse(canBeTypes []uint64, rows [][]string) {
	for _, row := range rows {
		for i, col := range row {
			if _, err := strconv.ParseBool(col); err != nil {
				canBeTypes[i] &= ^BitFlagBool
			}

			if _, err := strconv.ParseInt(col, 0, 64); err != nil {
				canBeTypes[i] &= ^BitFlagInt
			}

			if _, err := strconv.ParseFloat(col, 64); err != nil {
				canBeTypes[i] &= ^BitFlagFloat
			}
		}
	}
}

// initTypeSlice will set the initial type values to all the possible values
func initTypeSlice(canBeTypes []uint64) {
	for i := range canBeTypes {
		canBeTypes[i] = BitmaskNonStringDataTypes
	}
}

// guessColTypes examines the set of strings and tries to work out what the
// column types could be.
func guessColTypes(ci []ColInfo, rows [][]string) []ColType {
	if len(ci) == 0 {
		return nil
	}

	canBeTypes := make([]uint64, len(ci))
	initTypeSlice(canBeTypes)

	tryParse(canBeTypes, rows)

	types := make([]ColType, len(ci))
	for i, v := range canBeTypes {
		if ci[i].colType != ColTypeUnknown {
			types[i] = ci[i].colType
			continue
		}

		if canBeBool(v) {
			types[i] = ColTypeBool
		} else if canBeInt(v) {
			types[i] = ColTypeInt
		} else if canBeFloat(v) {
			types[i] = ColTypeFloat
		} else {
			types[i] = ColTypeString
		}
	}
	return types
}

// stripComments removes any comments from the line and returns the stripped
// line
func stripComments(dfr *DFReader, state *dfReadState, _ *DF) (bool, error) {
	if dfr.commentRegex == nil {
		return false, nil
	}

	parts := dfr.commentRegex.Split(state.line, -1)
	state.line = parts[0]
	return false, nil
}

// splitLine will first split the line into a slice of strings and then
// remove from that slice those columns to be skipped. It will return an
// error if any of the columns to be skipped has an index greater than the
// maximum index into the slice.
func splitLine(dfr *DFReader, state *dfReadState, df *DF) (bool, error) {
	state.cols = dfr.splitRegex.Split(state.line, dfr.maxCols)
	colsToSkip := len(dfr.skipCols)
	if colsToSkip == 0 {
		return false, nil
	}

	for i := len(state.cols) - 1; i >= 0; i-- { // work backwards from the end
		if dfr.skipCols[i] {
			// remove the ith column
			state.cols = append(state.cols[:i], state.cols[i+1:]...)
			colsToSkip--
			if colsToSkip == 0 {
				break
			}
		}
	}
	if colsToSkip > 0 {
		errStr := fmt.Sprintf(
			"%s: some skip columns are after the end of the line: ", state.loc)
		maxIdx := len(state.cols) - 1
		sep := ""
		for i := range dfr.skipCols {
			if i > maxIdx {
				errStr += sep + fmt.Sprintf("%d", i)
				sep = ", "
			}
		}
		err := dfError(errStr)
		df.addError(err)
		return false, err
	}

	return false, nil
}

// skipLine checks to see if the line is in the set to be skipped and if so
// sets skip to true and returns. Otherwise it sets skip to false and
// returns. The error is always nil.
func skipLine(dfr *DFReader, state *dfReadState, _ *DF) (bool, error) {
	if state.loc.Idx() <= dfr.skipLines {
		return true, nil
	}
	return false, nil
}

// skipBlankLine checks to see if the line is blank and if so sets skip to
// true. It may also set an error if blank lines are not expected.
func skipBlankLine(dfr *DFReader, state *dfReadState, df *DF) (bool, error) {
	if state.line != "" {
		return false, nil
	}

	if dfr.skipBlankLines {
		return true, nil
	}

	var err error = dfErrorf("%s: unexpected blank line", state.loc)
	df.addError(err)
	if dfr.allowErrors {
		err = nil
	}

	return true, err
}

// handleLine1 sets the column names from the columns. If any error is
// detected it will add it to the dataframe and if errors are not allowed it
// will return the error. If the line contains the column names rather than
// data (hasHeader == true) it will set skip to true and return
func handleLine1(dfr *DFReader, state *dfReadState, df *DF) (bool, error) {
	state.dataLineNum++
	if state.dataLineNum != 1 {
		return false, nil
	}
	skip, err := dfr.setColNames(state, df)
	if dfr.allowErrors {
		err = nil
	}
	return skip, err
}

// cacheData adds the data to the cache if there is a cache and it isn't yet
// full. If adding the data fills the cache then it uses the cached data to
// populate the dataframe. The cached lines allow the datatypes of the
// columns to be guessed. If the cache is full then the data is added to the
// dataframe directly.
func cacheData(dfr *DFReader, state *dfReadState, df *DF) (bool, error) {
	if state.cache == nil {
		return false, nil
	}

	var err error
	state.cache = append(state.cache, state.cols)
	if len(state.cache) == cap(state.cache) { // cache is full
		err = populateDF(dfr, state, df)
		state.cache = nil // we're finished with the cache now so clear it

		if dfr.allowErrors {
			err = nil
		}
	}
	return true, err
}

// handleData adds the data to the cache if there is still room, and if
// this fills the cache then it uses the cached data to populate the
// dataframe. The cached lines allow the datatypes of the columns to be
// guessed. If the cache is full then the data is added to the dataframe
// directly.
func handleData(dfr *DFReader, state *dfReadState, df *DF) (bool, error) {
	df.AddRowFromText(state.cols)
	if !dfr.allowErrors && df.errCount != 0 {
		return false, dfErrorf("%s: parsing errors", state.loc)
	}
	return false, nil
}

// checkColumns checks that the number of columns is equal to the number of
// parts. If so it returns with skip set to false. If not skip is set to
// true, the error is added to the dataframe and if errors are not allowed
// then the error is returned.
func checkColumns(dfr *DFReader, state *dfReadState, df *DF) (bool, error) {
	if len(df.mci.info) == len(state.cols) {
		return false, nil
	}

	errStr := fmt.Sprintf(
		"%s: the dataframe has %d columns but this line has %d: ",
		state.loc, len(df.mci.info), len(state.cols))
	for i, col := range state.cols {
		errStr += fmt.Sprintf(" col %d: %q", i, col)
	}
	var err error = dfError(errStr)
	df.addError(err)
	if dfr.allowErrors {
		err = nil
	}
	return true, err
}

// Read will construct a DataFrame from the data read off the Reader.
func (dfr *DFReader) Read(rd io.Reader, source string) (*DF, error) {
	df, err := dfr.makeDF()
	if err != nil {
		return nil, err
	}

	state := newDFReadState(dfr, source)
	operations := []lineHandler{
		skipLine,
		stripComments,
		skipBlankLine,
		splitLine,
		handleLine1,
		checkColumns,
		cacheData,
		handleData,
	}

	scanner := bufio.NewScanner(rd)
Loop:
	for scanner.Scan() {
		state.loc.Incr()
		state.line = scanner.Text()

		for _, op := range operations {
			skip, err := op(dfr, state, df)
			if err != nil {
				return nil, err
			}
			if skip {
				continue Loop
			}
		}
	}
	if err = scanner.Err(); err != nil {
		return nil, err
	}

	err = populateDF(dfr, state, df)

	if !dfr.allowErrors && err != nil {
		return nil, err
	}

	return df, nil
}

// populateDF populates the Dataframe from the values in the cache of initial
// lines. It will use those values to guess at the data types of the columns
// and only then will it populate the values.
func populateDF(dfr *DFReader, state *dfReadState, df *DF) error {
	if len(state.cache) == 0 {
		return nil
	}

	err := dfr.setColTypes(df, state.cache)
	if err != nil {
		return err
	}
	df.AddRowsFromText(state.cache)

	if df.errCount != 0 {
		return dfErrorf("%s: %d errors parsing initial lines (first error: %s)",
			state.loc.Source(), df.errCount, df.errors[0])
	}

	return nil
}
