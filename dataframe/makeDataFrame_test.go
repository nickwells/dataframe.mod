package dataframe_test

import (
	"strings"
	"testing"

	"github.com/nickwells/dataframe.mod/dataframe"
	"github.com/nickwells/testhelper.mod/testhelper"
)

var makeDataFrameTests = []struct {
	testhelper.ID
	dfrErr        testhelper.ExpErr
	readErr       testhelper.ExpErr
	content       string
	expDFErrCount int64
	expRowCount   int
	optArgs       []dataframe.DFReaderOpt
	expCols       []dataframe.ColInfo
}{
	{
		ID: testhelper.MkID("valid - basic"),
		content: `9 2 3 4
9 2 3 4`,
		expRowCount: 2,
		expCols: []dataframe.ColInfo{
			dataframe.NewColInfo("V0", dataframe.ColTypeInt),
			dataframe.NewColInfo("V1", dataframe.ColTypeInt),
			dataframe.NewColInfo("V2", dataframe.ColTypeInt),
			dataframe.NewColInfo("V3", dataframe.ColTypeInt),
		},
	},
	{
		ID: testhelper.MkID("valid - blank line ignored"),
		content: `
9 2 3 4
9 2 3 4`,
		expRowCount: 2,
		expCols: []dataframe.ColInfo{
			dataframe.NewColInfo("V0", dataframe.ColTypeInt),
			dataframe.NewColInfo("V1", dataframe.ColTypeInt),
			dataframe.NewColInfo("V2", dataframe.ColTypeInt),
			dataframe.NewColInfo("V3", dataframe.ColTypeInt),
		},
		optArgs: []dataframe.DFReaderOpt{
			dataframe.SkipBlankLines,
		},
	},
	{
		ID: testhelper.MkID("valid - hasHeader"),
		content: `firstCol 2ndCol 3rd lastCol
9 2 3 4
9 2 3 4`,
		expRowCount: 2,
		expCols: []dataframe.ColInfo{
			dataframe.NewColInfo("firstCol", dataframe.ColTypeInt),
			dataframe.NewColInfo("2ndCol", dataframe.ColTypeInt),
			dataframe.NewColInfo("3rd", dataframe.ColTypeInt),
			dataframe.NewColInfo("lastCol", dataframe.ColTypeInt),
		},
		optArgs: []dataframe.DFReaderOpt{
			dataframe.HasHeader,
		},
	},
	{
		ID: testhelper.MkID("valid - column names given"),
		content: `9 2 3 4
9 2 3 4`,
		expRowCount: 2,
		expCols: []dataframe.ColInfo{
			dataframe.NewColInfo("col1", dataframe.ColTypeInt),
			dataframe.NewColInfo("col2", dataframe.ColTypeInt),
			dataframe.NewColInfo("col3", dataframe.ColTypeInt),
			dataframe.NewColInfo("col4", dataframe.ColTypeInt),
		},
		optArgs: []dataframe.DFReaderOpt{
			dataframe.DFRColNames(
				"col1",
				"col2",
				"col3",
				"col4"),
		},
	},
	{
		ID: testhelper.MkID("valid - column types given"),
		content: `hello 2 3 4
world 2 3 4.0`,
		expRowCount: 2,
		expCols: []dataframe.ColInfo{
			dataframe.NewColInfo("V0", dataframe.ColTypeString),
			dataframe.NewColInfo("V1", dataframe.ColTypeInt),
			dataframe.NewColInfo("V2", dataframe.ColTypeInt),
			dataframe.NewColInfo("V3", dataframe.ColTypeFloat),
		},
		optArgs: []dataframe.DFReaderOpt{
			dataframe.DFRColTypes(
				dataframe.ColTypeString,
				dataframe.ColTypeInt,
				dataframe.ColTypeInt,
				dataframe.ColTypeFloat),
		},
	},
	{
		ID: testhelper.MkID("valid - CommentPattern"),
		content: `9 2 3 4    # comment
9 2 3 4`,
		expRowCount: 2,
		expCols: []dataframe.ColInfo{
			dataframe.NewColInfo("V0", dataframe.ColTypeInt),
			dataframe.NewColInfo("V1", dataframe.ColTypeInt),
			dataframe.NewColInfo("V2", dataframe.ColTypeInt),
			dataframe.NewColInfo("V3", dataframe.ColTypeInt),
		},
		optArgs: []dataframe.DFReaderOpt{
			dataframe.CommentPattern(`\s*#.*$`),
		},
	},
	{
		ID: testhelper.MkID("valid - SplitPattern"),
		content: `9,2,3,4
9,2,3,4`,
		expRowCount: 2,
		expCols: []dataframe.ColInfo{
			dataframe.NewColInfo("V0", dataframe.ColTypeInt),
			dataframe.NewColInfo("V1", dataframe.ColTypeInt),
			dataframe.NewColInfo("V2", dataframe.ColTypeInt),
			dataframe.NewColInfo("V3", dataframe.ColTypeInt),
		},
		optArgs: []dataframe.DFReaderOpt{
			dataframe.SplitPattern(`,`),
		},
	},
	{
		ID: testhelper.MkID("valid - SkipLines"),
		content: `9 2 3 4
9 2 3 4`,
		expRowCount: 1,
		expCols: []dataframe.ColInfo{
			dataframe.NewColInfo("V0", dataframe.ColTypeInt),
			dataframe.NewColInfo("V1", dataframe.ColTypeInt),
			dataframe.NewColInfo("V2", dataframe.ColTypeInt),
			dataframe.NewColInfo("V3", dataframe.ColTypeInt),
		},
		optArgs: []dataframe.DFReaderOpt{
			dataframe.SkipLines(1),
		},
	},
	{
		ID: testhelper.MkID("valid - InitialLines"),
		content: `9 2 3 4
9 2 3 4`,
		expRowCount: 2,
		expCols: []dataframe.ColInfo{
			dataframe.NewColInfo("V0", dataframe.ColTypeInt),
			dataframe.NewColInfo("V1", dataframe.ColTypeInt),
			dataframe.NewColInfo("V2", dataframe.ColTypeInt),
			dataframe.NewColInfo("V3", dataframe.ColTypeInt),
		},
		optArgs: []dataframe.DFReaderOpt{
			dataframe.InitialLines(2),
		},
	},
	{
		ID: testhelper.MkID("valid - multi-types"),
		content: `true 2 3.1 4
false -1 3.1e33 4hello
False 9999999 3 4
FALSE 0 3 4
f 2 3 4
1 2 3 4`,
		expRowCount: 6,
		expCols: []dataframe.ColInfo{
			dataframe.NewColInfo("V0", dataframe.ColTypeBool),
			dataframe.NewColInfo("V1", dataframe.ColTypeInt),
			dataframe.NewColInfo("V2", dataframe.ColTypeFloat),
			dataframe.NewColInfo("V3", dataframe.ColTypeString),
		},
	},
	{
		ID: testhelper.MkID("mixed number of columns - 4 and 3"),
		content: `1 2 3 4
1 2 3`,
		readErr: testhelper.MkExpErr(
			"the dataframe has 4 columns but this line has 3: "),
	},
	{
		ID: testhelper.MkID("blank line - not ignored"),
		content: `
1 2 3 4
1 2 3 4`,
		readErr: testhelper.MkExpErr("blank line"),
	},
	{
		ID: testhelper.MkID("has-header and column names given"),
		content: `1 2 3 4
1 2 3 4`,
		dfrErr: testhelper.MkExpErr(dataframe.ErrHasNamesAndHeader.Error()),
		optArgs: []dataframe.DFReaderOpt{
			dataframe.HasHeader,
			dataframe.DFRColNames("col1", "col2", "col3", "col4"),
		},
	},
	{
		ID: testhelper.MkID("has-header and column names given (other order)"),
		content: `1 2 3 4
1 2 3 4`,
		dfrErr: testhelper.MkExpErr(dataframe.ErrHasNamesAndHeader.Error()),
		optArgs: []dataframe.DFReaderOpt{
			dataframe.DFRColNames("col1", "col2", "col3", "col4"),
			dataframe.HasHeader,
		},
	},
	{
		ID: testhelper.MkID("column names given (empty list)"),
		content: `1 2 3 4
1 2 3 4`,
		dfrErr: testhelper.MkExpErr(dataframe.ErrNoNamesGiven.Error()),
		optArgs: []dataframe.DFReaderOpt{
			dataframe.DFRColNames(),
		},
	},
	{
		ID: testhelper.MkID("column names given (duplicates)"),
		content: `1 2 3 4
1 2 3 4`,
		readErr: testhelper.MkExpErr("duplicate column name:",
			"\"A\" is used for columns 0 and 3"),
		optArgs: []dataframe.DFReaderOpt{
			dataframe.DFRColNames("A", "B", "C", "A"),
		},
	},
	{
		ID: testhelper.MkID("column names given twice"),
		content: `1 2 3 4
1 2 3 4`,
		dfrErr: testhelper.MkExpErr(dataframe.ErrNamesAlreadySet.Error()),
		optArgs: []dataframe.DFReaderOpt{
			dataframe.DFRColNames("c1", "c2", "c3", "c4"),
			dataframe.DFRColNames("C1", "C2", "C3", "C4"),
		},
	},
	{
		ID: testhelper.MkID("column types given (empty list)"),
		content: `1 2 3 4
1 2 3 4`,
		dfrErr: testhelper.MkExpErr(dataframe.ErrNoTypesGiven.Error()),
		optArgs: []dataframe.DFReaderOpt{
			dataframe.DFRColTypes(),
		},
	},
	{
		ID: testhelper.MkID("column types given (invalid)"),
		content: `1 2 3 4
1 2 3 4`,
		readErr: testhelper.MkExpErr(
			`dataframe error: bad column type: column: 0 type: "ColTypeMaxVal"`,
		),
		optArgs: []dataframe.DFReaderOpt{
			dataframe.DFRColTypes(dataframe.ColTypeMaxVal),
		},
	},
	{
		ID: testhelper.MkID("column types given twice"),
		content: `1 2 3 4
1 2 3 4`,
		dfrErr: testhelper.MkExpErr(dataframe.ErrTypesAlreadySet.Error()),
		optArgs: []dataframe.DFReaderOpt{
			dataframe.DFRColTypes(dataframe.ColTypeInt,
				dataframe.ColTypeInt,
				dataframe.ColTypeInt,
				dataframe.ColTypeInt),
			dataframe.DFRColTypes(dataframe.ColTypeInt,
				dataframe.ColTypeInt,
				dataframe.ColTypeInt,
				dataframe.ColTypeInt),
		},
	},
	{
		ID: testhelper.MkID("column names and types given - lengths differ"),
		content: `1 2 3 4
1 2 3 4`,
		dfrErr: testhelper.MkExpErr(
			"the number of column types (3) and names (4) differ"),
		optArgs: []dataframe.DFReaderOpt{
			dataframe.DFRColNames("c1", "c2", "c3", "c4"),
			dataframe.DFRColTypes(
				dataframe.ColTypeInt,
				dataframe.ColTypeInt,
				dataframe.ColTypeInt),
		},
	},
	{
		ID: testhelper.MkID("column names and types given - lengths differ" +
			" (other order)"),
		content: `1 2 3 4
1 2 3 4`,
		dfrErr: testhelper.MkExpErr(
			"the number of column types (3) and names (4) differ"),
		optArgs: []dataframe.DFReaderOpt{
			dataframe.DFRColTypes(
				dataframe.ColTypeInt,
				dataframe.ColTypeInt,
				dataframe.ColTypeInt),
			dataframe.DFRColNames("c1", "c2", "c3", "c4"),
		},
	},
	{
		ID: testhelper.MkID("CommentPattern - bad regex"),
		content: `1 2 3 4
1 2 3 4`,
		dfrErr: testhelper.MkExpErr("the regexp to strip comments is invalid"),
		optArgs: []dataframe.DFReaderOpt{
			dataframe.CommentPattern("*"),
		},
	},
	{
		ID: testhelper.MkID("SplitPattern - bad regex"),
		content: `1 2 3 4
1 2 3 4`,
		dfrErr: testhelper.MkExpErr(
			"the pattern for splitting lines is invalid"),
		optArgs: []dataframe.DFReaderOpt{
			dataframe.SplitPattern("*"),
		},
	},
	{
		ID: testhelper.MkID("InitialLines=0 and no colTypes"),
		content: `1 2 3 4
1 2 3 4`,
		dfrErr: testhelper.MkExpErr("either give column types explicitly or" +
			" give some lines to work it out"),
		optArgs: []dataframe.DFReaderOpt{
			dataframe.InitialLines(0),
		},
	},
	{
		ID: testhelper.MkID("InitialLines=1 and bad col"),
		content: `1 2 3 4
bad 2 3 4`,
		readErr: testhelper.MkExpErr("parsing errors"),
		optArgs: []dataframe.DFReaderOpt{
			dataframe.InitialLines(1),
		},
	},
	{
		ID: testhelper.MkID("bad skip cols - has negative values"),
		dfrErr: testhelper.MkExpErr("dataframe error:" +
			" a negative skip index has been given: skips[1] == -1"),
		optArgs: []dataframe.DFReaderOpt{
			dataframe.DFRSkipCols(0, -1),
		},
	},
	{
		ID: testhelper.MkID("bad skip cols - empty list"),
		dfrErr: testhelper.MkExpErr("dataframe error:" +
			" no column skip indexes have been given"),
		optArgs: []dataframe.DFReaderOpt{
			dataframe.DFRSkipCols(),
		},
	},
	{
		ID: testhelper.MkID("bad skip cols - set twice"),
		dfrErr: testhelper.MkExpErr("dataframe error:" +
			" the column skip indexes have already been set"),
		optArgs: []dataframe.DFReaderOpt{
			dataframe.DFRSkipCols(0),
			dataframe.DFRSkipCols(0),
		},
	},
	{
		ID: testhelper.MkID("good - skip cols - skip first"),
		content: `1 1 2 3
1 true 2 3`,
		optArgs: []dataframe.DFReaderOpt{
			dataframe.DFRSkipCols(0),
		},
		expRowCount: 2,
		expCols: []dataframe.ColInfo{
			dataframe.NewColInfo("V0", dataframe.ColTypeBool),
			dataframe.NewColInfo("V1", dataframe.ColTypeInt),
			dataframe.NewColInfo("V2", dataframe.ColTypeInt),
		},
	},
	{
		ID: testhelper.MkID("good - skip cols - skip last"),
		content: `1 1 2 3
2 true 2 3`,
		optArgs: []dataframe.DFReaderOpt{
			dataframe.DFRSkipCols(3),
		},
		expRowCount: 2,
		expCols: []dataframe.ColInfo{
			dataframe.NewColInfo("V0", dataframe.ColTypeInt),
			dataframe.NewColInfo("V1", dataframe.ColTypeBool),
			dataframe.NewColInfo("V2", dataframe.ColTypeInt),
		},
	},
	{
		ID: testhelper.MkID("good - skip cols - skip several"),
		content: `1 1 2 3 4 5 6
2 true 2.0 hello 4 5 6.0`,
		optArgs: []dataframe.DFReaderOpt{
			dataframe.DFRSkipCols(2, 4, 5),
		},
		expRowCount: 2,
		expCols: []dataframe.ColInfo{
			dataframe.NewColInfo("V0", dataframe.ColTypeInt),
			dataframe.NewColInfo("V1", dataframe.ColTypeBool),
			dataframe.NewColInfo("V2", dataframe.ColTypeString),
			dataframe.NewColInfo("V3", dataframe.ColTypeFloat),
		},
	},
}

func TestMakeDF(t *testing.T) {
	for _, tc := range makeDataFrameTests {
		dfr, err := dataframe.NewDFReader(tc.optArgs...)
		if !testhelper.CheckExpErrWithID(t, tc.IDStr(), err, tc.dfrErr) ||
			err != nil {
			continue
		}

		df, err := dfr.Read(strings.NewReader(tc.content), "test string")
		if testhelper.CheckExpErrWithID(t, tc.IDStr(), err, tc.readErr) &&
			err == nil {
			checkColDetails(t, tc.IDStr(), df, tc.expCols)

			if tc.expRowCount != df.RowCount() {
				t.Log(tc.IDStr())
				t.Errorf("\t: dataframe should have %d rows but has %d",
					tc.expRowCount, df.RowCount())
			}

			if tc.expDFErrCount != df.ErrCount() {
				t.Log(tc.IDStr())
				t.Errorf("\t: dataframe should have %d"+
					" construction errors but has %d",
					tc.expDFErrCount, df.ErrCount())
				t.Log("\t: first 5 errors:")
				for i, e := range df.Errors() {
					if i >= 5 {
						break
					}
					t.Log("\t\t: ", e)
				}
			}
		}
	}
}
