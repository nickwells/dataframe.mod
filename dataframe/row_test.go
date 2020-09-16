package dataframe_test

import (
	"testing"

	"github.com/nickwells/dataframe.mod/dataframe"
	"github.com/nickwells/testhelper.mod/testhelper"
)

// makeTestRow creates and returns a standard Row for the tests to use
func makeTestRow() *dataframe.Row {
	r, err := dataframe.NewRow()
	if err != nil {
		panic(err)
	}

	err = r.AddBool("boolCol", dataframe.BoolVal{Val: true})
	if err != nil {
		panic(err)
	}
	err = r.AddInt("intCol", dataframe.IntVal{Val: 42})
	if err != nil {
		panic(err)
	}
	err = r.AddFloat("floatCol", dataframe.FloatVal{Val: 3.14159})
	if err != nil {
		panic(err)
	}
	err = r.AddString("stringCol", dataframe.StringVal{Val: "Hello, World!"})
	if err != nil {
		panic(err)
	}

	return r
}

func TestRowValByIdx(t *testing.T) {
	testRow := makeTestRow()
	testCases := []struct {
		testhelper.ID
		testhelper.ExpErr
		idx        int
		expColType dataframe.ColType
		expVal     interface{}
	}{
		{
			ID:         testhelper.MkID("good - bool column"),
			idx:        0,
			expColType: dataframe.ColTypeBool,
			expVal:     dataframe.BoolVal{Val: true},
		},
		{
			ID:         testhelper.MkID("good - int column"),
			idx:        1,
			expColType: dataframe.ColTypeInt,
			expVal:     dataframe.IntVal{Val: 42},
		},
		{
			ID:         testhelper.MkID("good - float column"),
			idx:        2,
			expColType: dataframe.ColTypeFloat,
			expVal:     dataframe.FloatVal{Val: 3.14159},
		},
		{
			ID:         testhelper.MkID("good - string column"),
			idx:        3,
			expColType: dataframe.ColTypeString,
			expVal:     dataframe.StringVal{Val: "Hello, World!"},
		},
		{
			ID:  testhelper.MkID("bad column index - too small"),
			idx: -1,
			ExpErr: testhelper.MkExpErr(
				`dataframe error: There is no column -1 (valid range: 0-3)`),
		},
		{
			ID:  testhelper.MkID("bad column index - too big"),
			idx: 4,
			ExpErr: testhelper.MkExpErr(
				`dataframe error: There is no column 4 (valid range: 0-3)`),
		},
	}

	for _, tc := range testCases {
		v, colType, err := testRow.ValByIdx(tc.idx)
		if testhelper.CheckExpErr(t, err, tc) && err == nil {
			if colType != tc.expColType {
				t.Log(tc.IDStr())
				t.Logf("\t: expected column type: %q", tc.expColType)
				t.Logf("\t:   actual column type: %q", colType)
				t.Errorf("\t: Unexpected column type")
			} else if colType == dataframe.ColTypeBool {
				compareBoolVals(t, tc.IDStr(), tc.expVal, v)
			} else if colType == dataframe.ColTypeInt {
				compareIntVals(t, tc.IDStr(), tc.expVal, v)
			} else if colType == dataframe.ColTypeFloat {
				compareFloatVals(t, tc.IDStr(), tc.expVal, v)
			} else if colType == dataframe.ColTypeString {
				compareStringVals(t, tc.IDStr(), tc.expVal, v)
			} else {
				t.Fatalf("%s: BAD TEST - column type %q is not handled",
					tc.IDStr(), colType)
			}
		}
	}
}

func TestRowValByName(t *testing.T) {
	testRow := makeTestRow()
	testCases := []struct {
		testhelper.ID
		testhelper.ExpErr
		name       string
		expColType dataframe.ColType
		expVal     interface{}
	}{
		{
			ID:         testhelper.MkID("good - bool column"),
			name:       "boolCol",
			expColType: dataframe.ColTypeBool,
			expVal:     dataframe.BoolVal{Val: true},
		},
		{
			ID:         testhelper.MkID("good - int column"),
			name:       "intCol",
			expColType: dataframe.ColTypeInt,
			expVal:     dataframe.IntVal{Val: 42},
		},
		{
			ID:         testhelper.MkID("good - float column"),
			name:       "floatCol",
			expColType: dataframe.ColTypeFloat,
			expVal:     dataframe.FloatVal{Val: 3.14159},
		},
		{
			ID:         testhelper.MkID("good - string column"),
			name:       "stringCol",
			expColType: dataframe.ColTypeString,
			expVal:     dataframe.StringVal{Val: "Hello, World!"},
		},
		{
			ID:   testhelper.MkID("bad column name"),
			name: "nonesuch",
			ExpErr: testhelper.MkExpErr(
				`dataframe error: Unknown column name: "nonesuch"`),
		},
	}

	for _, tc := range testCases {
		v, colType, err := testRow.ValByName(tc.name)
		if testhelper.CheckExpErr(t, err, tc) && err == nil {
			if colType != tc.expColType {
				t.Log(tc.IDStr())
				t.Logf("\t: expected column type: %q", tc.expColType)
				t.Logf("\t:   actual column type: %q", colType)
				t.Errorf("\t: Unexpected column type")
			} else if colType == dataframe.ColTypeBool {
				compareBoolVals(t, tc.IDStr(), tc.expVal, v)
			} else if colType == dataframe.ColTypeInt {
				compareIntVals(t, tc.IDStr(), tc.expVal, v)
			} else if colType == dataframe.ColTypeFloat {
				compareFloatVals(t, tc.IDStr(), tc.expVal, v)
			} else if colType == dataframe.ColTypeString {
				compareStringVals(t, tc.IDStr(), tc.expVal, v)
			} else {
				t.Fatalf("%s: BAD TEST - column type %q is not handled",
					tc.IDStr(), colType)
			}
		}
	}
}

// compareBoolVals converts the expected and actual values into bool values,
// reporting any conversion failure and then checks that they are the same
// reporting any differeinces
func compareBoolVals(t *testing.T, id string, expected, actual interface{}) {
	t.Helper()

	expVal, ok := expected.(dataframe.BoolVal)
	if !ok {
		t.Fatal(id, ": BAD TEST - expVal should have been of type bool")
		return
	}
	actualVal, ok := actual.(dataframe.BoolVal)
	if !ok {
		t.Log(id)
		t.Logf("\t: expected type: %T", expVal)
		t.Logf("\t:   actual type: %T", actual)
		t.Errorf("\t: value type doesn't match the returned type")
	} else if actualVal.IsNA != expVal.IsNA {
		t.Log(id)
		t.Logf("\t: expected IsNA: %v", expVal.IsNA)
		t.Logf("\t:   actual IsNA: %v", actualVal.IsNA)
		t.Errorf("\t: the IsNA flags don't match")
	} else if !actualVal.IsNA && expVal.Val != actualVal.Val {
		t.Log(id)
		t.Logf("\t: expected val: %v", expVal.Val)
		t.Logf("\t:   actual val: %v", actualVal.Val)
		t.Errorf("\t: the values don't match")
	}
}

// compareIntVals converts the expected and actual values into int values,
// reporting any conversion failure and then checks that they are the same
// reporting any differeinces
func compareIntVals(t *testing.T, id string, expected, actual interface{}) {
	t.Helper()

	expVal, ok := expected.(dataframe.IntVal)
	if !ok {
		t.Fatal(id, ": BAD TEST - expVal should have been of type int")
		return
	}
	actualVal, ok := actual.(dataframe.IntVal)
	if !ok {
		t.Log(id)
		t.Logf("\t: expected type: %T", expVal)
		t.Logf("\t:   actual type: %T", actual)
		t.Errorf("\t: value type doesn't match the returned type")
	} else if actualVal.IsNA != expVal.IsNA {
		t.Log(id)
		t.Logf("\t: expected IsNA: %v", expVal.IsNA)
		t.Logf("\t:   actual IsNA: %v", actualVal.IsNA)
		t.Errorf("\t: the IsNA flags don't match")
	} else if !actualVal.IsNA && expVal.Val != actualVal.Val {
		t.Log(id)
		t.Logf("\t: expected val: %v", expVal.Val)
		t.Logf("\t:   actual val: %v", actualVal.Val)
		t.Errorf("\t: the values don't match")
	}
}

// compareFloatVals converts the expected and actual values into float values,
// reporting any conversion failure and then checks that they are the same
// reporting any differeinces
func compareFloatVals(t *testing.T, id string, expected, actual interface{}) {
	t.Helper()

	expVal, ok := expected.(dataframe.FloatVal)
	if !ok {
		t.Fatal(id, ": BAD TEST - expVal should have been of type float")
		return
	}
	actualVal, ok := actual.(dataframe.FloatVal)
	if !ok {
		t.Log(id)
		t.Logf("\t: expected type: %T", expVal)
		t.Logf("\t:   actual type: %T", actual)
		t.Errorf("\t: value type doesn't match the returned type")
	} else if actualVal.IsNA != expVal.IsNA {
		t.Log(id)
		t.Logf("\t: expected IsNA: %v", expVal.IsNA)
		t.Logf("\t:   actual IsNA: %v", actualVal.IsNA)
		t.Errorf("\t: the IsNA flags don't match")
	} else if !actualVal.IsNA && expVal.Val != actualVal.Val {
		t.Log(id)
		t.Logf("\t: expected val: %v", expVal.Val)
		t.Logf("\t:   actual val: %v", actualVal.Val)
		t.Errorf("\t: the values don't match")
	}
}

// compareStringVals converts the expected and actual values into string values,
// reporting any conversion failure and then checks that they are the same
// reporting any differeinces
func compareStringVals(t *testing.T, id string, expected, actual interface{}) {
	t.Helper()

	expVal, ok := expected.(dataframe.StringVal)
	if !ok {
		t.Fatal(id, ": BAD TEST - expVal should have been of type string")
		return
	}
	actualVal, ok := actual.(dataframe.StringVal)
	if !ok {
		t.Log(id)
		t.Logf("\t: expected type: %T", expVal)
		t.Logf("\t:   actual type: %T", actual)
		t.Errorf("\t: value type doesn't match the returned type")
	} else if actualVal.IsNA != expVal.IsNA {
		t.Log(id)
		t.Logf("\t: expected IsNA: %v", expVal.IsNA)
		t.Logf("\t:   actual IsNA: %v", actualVal.IsNA)
		t.Errorf("\t: the IsNA flags don't match")
	} else if !actualVal.IsNA && expVal.Val != actualVal.Val {
		t.Log(id)
		t.Logf("\t: expected val: %v", expVal.Val)
		t.Logf("\t:   actual val: %v", actualVal.Val)
		t.Errorf("\t: the values don't match")
	}
}

type colTest struct {
	name    string
	expType dataframe.ColType
	expVal  interface{}
}

func cmpCol(t *testing.T, id string, i int, c dataframe.Column, cTest colTest) {
	t.Helper()

	name, colType := c.Info()
	expName, expType, expVal := cTest.name, cTest.expType, cTest.expVal
	if name != expName {
		t.Log(id)
		t.Logf("\t: col %d: expected name: %q", i, expName)
		t.Logf("\t: col %d:   actual name: %q", i, name)
		t.Errorf("\t: unexpected column name")
	}
	if colType != expType {
		t.Log(id)
		t.Logf("\t: col %d: expected type: %q", i, expType)
		t.Logf("\t: col %d:   actual type: %q", i, colType)
		t.Errorf("\t: unexpected column type")
	} else {
		v, err := c.GetVal(0)
		if err != nil {
			t.Log(id)
			t.Fatal("\t: Unexpected error:", err)
		}
		switch colType {
		case dataframe.ColTypeBool:
			compareBoolVals(t, id, expVal, v)
		case dataframe.ColTypeInt:
			compareIntVals(t, id, expVal, v)
		case dataframe.ColTypeFloat:
			compareFloatVals(t, id, expVal, v)
		case dataframe.ColTypeString:
			compareStringVals(t, id, expVal, v)
		default:
			t.Fatalf(
				"%s: BAD TEST - column type %q is not tested",
				id, colType)
		}
	}
}

func TestColsByName(t *testing.T) {
	testRow := makeTestRow()
	testCases := []struct {
		testhelper.ID
		testhelper.ExpErr
		cols []colTest
	}{
		{
			ID: testhelper.MkID("all columns"),
			cols: []colTest{
				{
					name:    "boolCol",
					expType: dataframe.ColTypeBool,
					expVal:  dataframe.BoolVal{Val: true},
				},
				{
					name:    "intCol",
					expType: dataframe.ColTypeInt,
					expVal:  dataframe.IntVal{Val: 42},
				},
				{
					name:    "floatCol",
					expType: dataframe.ColTypeFloat,
					expVal:  dataframe.FloatVal{Val: 3.14159},
				},
				{
					name:    "stringCol",
					expType: dataframe.ColTypeString,
					expVal:  dataframe.StringVal{Val: "Hello, World!"},
				},
			},
		},
	}

	for _, tc := range testCases {
		names := make([]string, 0, len(tc.cols))
		for _, c := range tc.cols {
			names = append(names, c.name)
		}
		cols, err := testRow.ColsByName(names...)
		if testhelper.CheckExpErr(t, err, tc) && err == nil {
			if len(cols) != len(tc.cols) {
				t.Log(tc.IDStr())
				t.Logf("\t: expected # of results: %d", len(tc.cols))
				t.Logf("\t:   actual # of results: %d", len(cols))
				t.Errorf("\t: the wrong number of results\n")
			} else {
				for i, c := range cols {
					cmpCol(t, tc.IDStr(), i, c, tc.cols[i])
				}
			}
		}
	}
}

func TestColsByIdx(t *testing.T) {
	testRow := makeTestRow()
	testCases := []struct {
		testhelper.ID
		testhelper.ExpErr
		indexes []int
		cols    []colTest
	}{
		{
			ID:      testhelper.MkID("all columns"),
			indexes: []int{0, 1, 2, 3},
			cols: []colTest{
				{
					name:    "boolCol",
					expType: dataframe.ColTypeBool,
					expVal:  dataframe.BoolVal{Val: true},
				},
				{
					name:    "intCol",
					expType: dataframe.ColTypeInt,
					expVal:  dataframe.IntVal{Val: 42},
				},
				{
					name:    "floatCol",
					expType: dataframe.ColTypeFloat,
					expVal:  dataframe.FloatVal{Val: 3.14159},
				},
				{
					name:    "stringCol",
					expType: dataframe.ColTypeString,
					expVal:  dataframe.StringVal{Val: "Hello, World!"},
				},
			},
		},
	}

	for _, tc := range testCases {
		if len(tc.indexes) != len(tc.cols) {
			t.Fatal(
				tc.IDStr(),
				" - BAD TEST: different # of indexes and expected columns")
		}
		cols, err := testRow.ColsByIdx(tc.indexes...)
		if testhelper.CheckExpErr(t, err, tc) && err == nil {
			if len(cols) != len(tc.cols) {
				t.Log(tc.IDStr())
				t.Logf("\t: expected # of results: %d", len(tc.cols))
				t.Logf("\t:   actual # of results: %d", len(cols))
				t.Errorf("\t: the wrong number of results\n")
			} else {
				for i, c := range cols {
					cmpCol(t, tc.IDStr(), i, c, tc.cols[i])
				}
			}
		}
	}
}
