package dataframe_test

import (
	"fmt"
	"testing"

	"github.com/nickwells/dataframe.mod/dataframe"
	"github.com/nickwells/testhelper.mod/testhelper"
)

type actionType struct {
	testhelper.ExpErr
	colTypes []dataframe.ColType
	colNames []string
}

func TestDFSetColTypeAndName(t *testing.T) {
	testCases := []struct {
		testhelper.ID
		actions    []actionType
		expColVals []dataframe.ColInfo
	}{
		{
			ID: testhelper.MkID("good - no actions"),
		},
		{
			ID: testhelper.MkID("good - types and names"),
			actions: []actionType{
				{
					colTypes: []dataframe.ColType{
						dataframe.ColTypeInt,
						dataframe.ColTypeFloat,
					},
				},
				{colNames: []string{"c1", "c2"}},
			},
			expColVals: []dataframe.ColInfo{
				dataframe.NewColInfo("c1", dataframe.ColTypeInt),
				dataframe.NewColInfo("c2", dataframe.ColTypeFloat),
			},
		},
		{
			ID: testhelper.MkID("good - types and no names"),
			actions: []actionType{
				{
					colTypes: []dataframe.ColType{
						dataframe.ColTypeInt,
						dataframe.ColTypeFloat,
					},
				},
			},
			expColVals: []dataframe.ColInfo{
				dataframe.NewColInfo("", dataframe.ColTypeInt),
				dataframe.NewColInfo("", dataframe.ColTypeFloat),
			},
		},
		{
			ID: testhelper.MkID("bad - too many col names"),
			actions: []actionType{
				{
					colTypes: []dataframe.ColType{
						dataframe.ColTypeInt,
						dataframe.ColTypeFloat,
					},
				},
				{
					ExpErr: testhelper.MkExpErr(
						"dataframe error: " +
							"the number of columns (2) and" +
							" number of names (3) differ",
					),
					colNames: []string{"c1", "c2", "extraCol"},
				},
			},
			expColVals: []dataframe.ColInfo{
				dataframe.NewColInfo("", dataframe.ColTypeInt),
				dataframe.NewColInfo("", dataframe.ColTypeFloat),
			},
		},
		{
			ID: testhelper.MkID("bad - too few col names"),
			actions: []actionType{
				{
					colTypes: []dataframe.ColType{
						dataframe.ColTypeInt,
						dataframe.ColTypeFloat,
					},
				},
				{
					ExpErr: testhelper.MkExpErr(
						"dataframe error: " +
							"the number of columns (2) and" +
							" number of names (1) differ",
					),
					colNames: []string{"c1"},
				},
			},
			expColVals: []dataframe.ColInfo{
				dataframe.NewColInfo("", dataframe.ColTypeInt),
				dataframe.NewColInfo("", dataframe.ColTypeFloat),
			},
		},
		{
			ID: testhelper.MkID("bad - no col names"),
			actions: []actionType{
				{
					colTypes: []dataframe.ColType{
						dataframe.ColTypeInt,
						dataframe.ColTypeFloat,
					},
				},
				{
					ExpErr: testhelper.MkExpErr(
						"dataframe error: " +
							"no column names have been given",
					),
					colNames: []string{},
				},
			},
			expColVals: []dataframe.ColInfo{
				dataframe.NewColInfo("", dataframe.ColTypeInt),
				dataframe.NewColInfo("", dataframe.ColTypeFloat),
			},
		},
		{
			ID: testhelper.MkID("good - names and types"),
			actions: []actionType{
				{colNames: []string{"c1", "c2"}},
				{
					colTypes: []dataframe.ColType{
						dataframe.ColTypeInt,
						dataframe.ColTypeFloat,
					},
				},
			},
			expColVals: []dataframe.ColInfo{
				dataframe.NewColInfo("c1", dataframe.ColTypeInt),
				dataframe.NewColInfo("c2", dataframe.ColTypeFloat),
			},
		},
		{
			ID: testhelper.MkID("good - set and reset names"),
			actions: []actionType{
				{
					colTypes: []dataframe.ColType{
						dataframe.ColTypeInt,
						dataframe.ColTypeFloat,
					},
				},
				{colNames: []string{"c1", "c2"}},
				{colNames: []string{"c3", "c4"}},
			},
			expColVals: []dataframe.ColInfo{
				dataframe.NewColInfo("c3", dataframe.ColTypeInt),
				dataframe.NewColInfo("c4", dataframe.ColTypeFloat),
			},
		},
		{
			ID: testhelper.MkID("bad - duplicate names"),
			actions: []actionType{
				{
					colTypes: []dataframe.ColType{
						dataframe.ColTypeInt,
						dataframe.ColTypeFloat,
					},
				},
				{
					ExpErr: testhelper.MkExpErr(
						"dataframe error: " +
							`duplicate column name: "c1" is used` +
							" for columns 0 and 1",
					),
					colNames: []string{"c1", "c1"},
				},
			},
			expColVals: []dataframe.ColInfo{
				dataframe.NewColInfo("", dataframe.ColTypeInt),
				dataframe.NewColInfo("", dataframe.ColTypeFloat),
			},
		},
		{
			ID: testhelper.MkID("bad - too few col types"),
			actions: []actionType{
				{
					colTypes: []dataframe.ColType{
						dataframe.ColTypeInt,
						dataframe.ColTypeFloat,
					},
				},
				{colNames: []string{"c1", "c2"}},
				{
					ExpErr: testhelper.MkExpErr(
						"dataframe error: " +
							"the number of columns (2) and" +
							" number of types (1) differ",
					),
					colTypes: []dataframe.ColType{
						dataframe.ColTypeInt,
					},
				},
			},
			expColVals: []dataframe.ColInfo{
				dataframe.NewColInfo("c1", dataframe.ColTypeInt),
				dataframe.NewColInfo("c2", dataframe.ColTypeFloat),
			},
		},
		{
			ID: testhelper.MkID("bad - no col types"),
			actions: []actionType{
				{
					colTypes: []dataframe.ColType{
						dataframe.ColTypeInt,
						dataframe.ColTypeFloat,
					},
				},
				{colNames: []string{"c1", "c2"}},
				{
					ExpErr: testhelper.MkExpErr(
						"dataframe error: " +
							"no column types have been given",
					),
					colTypes: []dataframe.ColType{},
				},
			},
			expColVals: []dataframe.ColInfo{
				dataframe.NewColInfo("c1", dataframe.ColTypeInt),
				dataframe.NewColInfo("c2", dataframe.ColTypeFloat),
			},
		},
		{
			ID: testhelper.MkID("bad - invalid col type, == ColTypeMaxVal"),
			actions: []actionType{
				{
					colTypes: []dataframe.ColType{
						dataframe.ColTypeInt,
						dataframe.ColTypeFloat,
					},
				},
				{colNames: []string{"c1", "c2"}},
				{
					ExpErr: testhelper.MkExpErr(
						"dataframe error: " +
							"bad column type: ",
					),
					colTypes: []dataframe.ColType{
						dataframe.ColTypeMaxVal,
						dataframe.ColTypeMaxVal,
					},
				},
			},
			expColVals: []dataframe.ColInfo{
				dataframe.NewColInfo("c1", dataframe.ColTypeInt),
				dataframe.NewColInfo("c2", dataframe.ColTypeFloat),
			},
		},
		{
			ID: testhelper.MkID("bad - invalid col type, > ColTypeMaxVal"),
			actions: []actionType{
				{
					colTypes: []dataframe.ColType{
						dataframe.ColTypeInt,
						dataframe.ColTypeFloat,
					},
				},
				{colNames: []string{"c1", "c2"}},
				{
					ExpErr: testhelper.MkExpErr(
						"dataframe error: " +
							"bad column type: ",
					),
					colTypes: []dataframe.ColType{
						dataframe.ColTypeMaxVal + 1,
						dataframe.ColTypeMaxVal + 1,
					},
				},
			},
			expColVals: []dataframe.ColInfo{
				dataframe.NewColInfo("c1", dataframe.ColTypeInt),
				dataframe.NewColInfo("c2", dataframe.ColTypeFloat),
			},
		},
	}

	for _, tc := range testCases {
		df := &dataframe.DF{}
		var err error
		var next bool
		for i, action := range tc.actions {
			actIdx := fmt.Sprintf(": action[%d]", i)
			if action.colTypes != nil {
				err = df.SetColTypes(action.colTypes...)
				if !testhelper.CheckExpErrWithID(
					t, tc.IDStr()+actIdx, err, action.ExpErr) {
					next = true
				}
			} else if action.colNames != nil {
				err = df.SetColNames(action.colNames...)
				if !testhelper.CheckExpErrWithID(
					t, tc.IDStr()+actIdx, err, action.ExpErr) {
					next = true
				}
			}
		}
		if next {
			continue
		}
		checkColDetails(t, tc.IDStr(), df, tc.expColVals)
	}
}

// checkColVal checks that the column (c) matches its expected values (e)
func checkColVal(t *testing.T, testID, colID string, c, e dataframe.ColInfo) {
	t.Helper()

	if c.Name() != e.Name() {
		t.Log(testID)
		t.Logf("\t: expected name for %s: %q\n",
			colID, e.Name())
		t.Logf("\t:   actual name for %s: %q\n",
			colID, c.Name())
		t.Errorf("\t: unexpected column name\n")
	}

	if c.ColType() != e.ColType() {
		t.Log(testID)
		t.Logf("\t: expected type for %s: %q\n",
			colID, e.ColType())
		t.Logf("\t:   actual type for %s: %q\n",
			colID, c.ColType())
		t.Errorf("\t: unexpected column type\n")
	}
}

// checkColDetails checks that the columns in the dataframe match their
// expected values in number, type and name
func checkColDetails(t *testing.T, id string, df *dataframe.DF, exp []dataframe.ColInfo) {
	t.Helper()

	cols := df.Columns()

	if len(cols) != len(exp) {
		t.Log(id)
		t.Logf("\t: expected: %d\n", len(exp))
		t.Logf("\t:   actual: %d\n", len(cols))
		t.Errorf("\t: unexpected column count\n")
		return
	}
	for i, c := range cols {
		checkColVal(t, id, fmt.Sprintf("col[%d]", i), c, exp[i])
	}
}

func TestDFColInfoByName(t *testing.T) {
	df := &dataframe.DF{}
	_ = df.SetColNames("c1", "c2")
	_ = df.SetColTypes(dataframe.ColTypeBool, dataframe.ColTypeFloat)

	testCases := []struct {
		testhelper.ID
		testhelper.ExpErr
		name   string
		expVal dataframe.ColInfo
	}{
		{
			ID:     testhelper.MkID("all good"),
			name:   "c1",
			expVal: dataframe.NewColInfo("c1", dataframe.ColTypeBool),
		},
		{
			ID: testhelper.MkID("bad name"),
			ExpErr: testhelper.MkExpErr(
				"dataframe error: " +
					`Unknown column name: "xxx"`,
			),
			name: "xxx",
		},
	}

	for _, tc := range testCases {
		ci, err := df.ColInfoByName(tc.name)
		if testhelper.CheckExpErr(t, err, tc) && err == nil {
			checkColVal(t, tc.IDStr(), "column", ci, tc.expVal)
		}
	}
}

func TestDFColInfoByIdx(t *testing.T) {
	df := &dataframe.DF{}
	_ = df.SetColNames("c1", "c2")
	_ = df.SetColTypes(dataframe.ColTypeBool, dataframe.ColTypeFloat)

	testCases := []struct {
		testhelper.ID
		testhelper.ExpErr
		idx    int
		expVal dataframe.ColInfo
	}{
		{
			ID:     testhelper.MkID("all good - first col"),
			idx:    0,
			expVal: dataframe.NewColInfo("c1", dataframe.ColTypeBool),
		},
		{
			ID:     testhelper.MkID("all good - last col"),
			idx:    1,
			expVal: dataframe.NewColInfo("c2", dataframe.ColTypeFloat),
		},
		{
			ID: testhelper.MkID("bad index - < 0"),
			ExpErr: testhelper.MkExpErr(
				"dataframe error: " +
					"There is no column -1" +
					" (valid range: 0-1)",
			),
			idx: -1,
		},
		{
			ID: testhelper.MkID("bad index - == last col + 1"),
			ExpErr: testhelper.MkExpErr(
				"dataframe error: " +
					"There is no column 2" +
					" (valid range: 0-1)",
			),
			idx: 2,
		},
		{
			ID: testhelper.MkID("bad index - == last col + 2"),
			ExpErr: testhelper.MkExpErr(
				"dataframe error: " +
					"There is no column 3" +
					" (valid range: 0-1)",
			),
			idx: 3,
		},
	}

	for _, tc := range testCases {
		ci, err := df.ColInfoByIdx(tc.idx)
		if testhelper.CheckExpErr(t, err, tc) && err == nil {
			checkColVal(t, tc.IDStr(), "column", ci, tc.expVal)
		}
	}
}
