package dataframe

import (
	"errors"
	"testing"

	"github.com/nickwells/testhelper.mod/v2/testhelper"
)

// TestBitPatterns ...
func TestBitPatterns(t *testing.T) {
	if !canBeBool(BitFlagBool) {
		t.Errorf("canBeBool failed when passed BitFlagBool (%d)\n", BitFlagBool)
	}
	if canBeBool(0) {
		t.Errorf("canBeBool failed when passed zero\n")
	}

	if !canBeInt(BitFlagInt) {
		t.Errorf("canBeInt failed when passed BitFlagInt (%d)\n", BitFlagInt)
	}
	if canBeInt(0) {
		t.Errorf("canBeInt failed when passed zero\n")
	}

	if !canBeFloat(BitFlagFloat) {
		t.Errorf("canBeFloat failed when passed BitFlagFloat (%d)\n",
			BitFlagFloat)
	}
	if canBeFloat(0) {
		t.Errorf("canBeFloat failed when passed zero\n")
	}
}

// TestAddError tests the DataFrame addError function
func TestAddError(t *testing.T) {
	df, _ := NewDF(MaxErrors(2))

	testCases := []struct {
		msg       string
		expCount  int64
		expErrors int64
	}{
		{
			msg:       "test1",
			expCount:  1,
			expErrors: 1,
		},
		{
			msg:       "test2",
			expCount:  2,
			expErrors: 2,
		},
		{
			msg:       "test3",
			expCount:  3,
			expErrors: 2,
		},
		{
			msg:       "test4",
			expCount:  4,
			expErrors: 2,
		},
	}
	for i, tc := range testCases {
		df.addError(errors.New(tc.msg))
		if df.errCount != tc.expCount {
			t.Errorf(
				"test %d: adding %s should cause %d errors but there are: %d",
				i, tc.msg, tc.expCount, df.errCount)
		}

		if int64(len(df.errors)) != tc.expErrors {
			t.Errorf(
				"test %d: adding %s should cause %d errors in the slice"+
					" but there are: %d",
				i, tc.msg, tc.expErrors, len(df.errors))
		}

		if tc.expErrors == tc.expCount {
			if len(df.errors) == 0 {
				t.Errorf(
					"test %d: adding %s should have put something into"+
						" the errors slice but it's empty",
					i, tc.msg)
			} else if df.errors[len(df.errors)-1].Error() != tc.msg {
				t.Errorf(
					"test %d: adding %s should have made it the last"+
						" entry in the errors slice but it's '%s'",
					i, tc.msg, df.errors[len(df.errors)-1])
			}
		}
	}
}

// TestTryParse ...
func TestTryParse(t *testing.T) {
	testCases := []struct {
		testhelper.ID
		expectedTypeFlags []uint64
		data              [][]string
	}{
		{
			ID: testhelper.MkID("bool int|float"),
			expectedTypeFlags: []uint64{
				BitFlagBool,
				BitFlagInt | BitFlagFloat,
			},
			data: [][]string{
				{"true", "1"},
				{"false", "4"},
			},
		},
	}

	for _, tc := range testCases {
		canBeTypes := make([]uint64, len(tc.data[0]))

		initTypeSlice(canBeTypes)
		tryParse(canBeTypes, tc.data)

		for j, colT := range canBeTypes {
			if colT != tc.expectedTypeFlags[j] {
				t.Log(tc.IDStr())
				t.Errorf("\t: failed: type of column %d expected: %d got %d",
					j, tc.expectedTypeFlags[j], colT)
			}
		}
	}
}
