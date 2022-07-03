package dataframe

import "fmt"

type Error interface {
	error
	DataframeError()
}

type dfError string

// Error returns a string representation of the error
func (e dfError) Error() string {
	return "dataframe error: " + string(e)
}

// DataframeError exists purely to classify the error as a dataframe.Error
func (e dfError) DataframeError() {}

var (
	ErrHasNamesAndHeader = dfError("you cannot give column names" +
		" and take names from a header")

	ErrNoSkipColsGiven       = dfError("no column skip indexes have been given")
	ErrSkipIndexesAlreadySet = dfError("the column skip indexes have" +
		" already been set")

	ErrNoNamesGiven    = dfError("no column names have been given")
	ErrNamesAlreadySet = dfError("the column names have already been set")

	ErrNoTypesGiven    = dfError("no column types have been given")
	ErrTypesAlreadySet = dfError("the column types have already been set")

	ErrNoTypeInfo = dfError("either give column types explicitly or" +
		" give some lines to work it out")
)

// dfErrorf formats the arguments into a dfError
func dfErrorf(format string, args ...any) dfError {
	return dfError(fmt.Sprintf(format, args...))
}
