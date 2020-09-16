package dataframe_test

import (
	"testing"

	"github.com/nickwells/dataframe.mod/dataframe"
	"github.com/nickwells/testhelper.mod/testhelper"
)

const (
	testData            = "testdata"
	fileNameNoSuchFile  = testData + "/noSuchFile"
	fileNameEmptyFile   = testData + "/emptyFile"
	fileNameLines1Cols4 = testData + "/lines1cols4"
)

func TestReadTable(t *testing.T) {
	var readTableTests = []struct {
		testhelper.ID
		testhelper.ExpErr
		fileName      string
		optArgs       []dataframe.DFReaderOpt
		expCols       []dataframe.ColInfo
		expRowCount   int
		expDFErrCount int64
	}{
		{
			ID: testhelper.MkID("bad filename"),
			ExpErr: testhelper.MkExpErr(
				fileNameNoSuchFile,
				"no such file or directory",
			),
			fileName: fileNameNoSuchFile,
		},
		{
			ID:       testhelper.MkID("empty file"),
			fileName: fileNameEmptyFile,
		},
		{
			ID:       testhelper.MkID("good file - one line, four columns"),
			fileName: fileNameLines1Cols4,
			expCols: []dataframe.ColInfo{
				dataframe.NewColInfo("V0", dataframe.ColTypeBool),
				dataframe.NewColInfo("V1", dataframe.ColTypeInt),
				dataframe.NewColInfo("V2", dataframe.ColTypeInt),
				dataframe.NewColInfo("V3", dataframe.ColTypeInt),
			},
			expRowCount: 1,
		},
	}

	for _, tc := range readTableTests {
		df, err := dataframe.ReadFile(tc.fileName, tc.optArgs...)
		if testhelper.CheckExpErr(t, err, tc) && err == nil {
			checkColDetails(t, tc.IDStr(), df, tc.expCols)
			if df.RowCount() != tc.expRowCount {
				t.Log(tc.IDStr())
				t.Logf("\t: expected row count: %d\n", tc.expRowCount)
				t.Logf("\t:   actual row count: %d\n", df.RowCount())
				t.Errorf("\t: unexpected row count\n")
			}
			if df.ErrCount() != tc.expDFErrCount {
				t.Log(tc.IDStr())
				t.Logf("\t: expected error count: %d\n", tc.expDFErrCount)
				t.Logf("\t:   actual error count: %d\n", df.ErrCount())
				t.Errorf("\t: unexpected error count\n")
			}
		}
	}
}
