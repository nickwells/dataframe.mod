package dataframe

// Column represents a single column in a dataframe
type Column struct {
	ci ColInfo

	// only one of the value slices will be populated
	boolVals   []BoolVal
	intVals    []IntVal
	floatVals  []FloatVal
	stringVals []StringVal
}

// SetInfo sets the column name and type. It will panic if the column type is
// unknown or invalid
func (c *Column) SetInfo(name string, colType ColType) {
	if colType <= ColTypeUnknown || colType >= ColTypeMaxVal {
		panic(dfErrorf("Unexpected column type: %q", colType))
	}

	c.ci.name = name
	c.ci.colType = colType
}

// Info returns the column info - the name and column type
func (c *Column) Info() (string, ColType) {
	return c.ci.name, c.ci.colType
}

// AddBoolVal adds a bool value to the column. It will panic if the column
// type is not bool
func (c *Column) AddBoolVal(v BoolVal) {
	if c.ci.colType != ColTypeBool {
		panic(dfErrorf("Adding a BoolVal to a %q column", c.ci.colType))
	}

	c.boolVals = append(c.boolVals, v)
}

// AddIntVal adds a int value to the column. It will panic if the column
// type is not int
func (c *Column) AddIntVal(v IntVal) {
	if c.ci.colType != ColTypeInt {
		panic(dfErrorf("Adding a IntVal to a %q column", c.ci.colType))
	}

	c.intVals = append(c.intVals, v)
}

// AddFloatVal adds a float value to the column. It will panic if the column
// type is not float
func (c *Column) AddFloatVal(v FloatVal) {
	if c.ci.colType != ColTypeFloat {
		panic(dfErrorf("Adding a FloatVal to a %q column", c.ci.colType))
	}

	c.floatVals = append(c.floatVals, v)
}

// AddStringVal adds a string value to the column. It will panic if the column
// type is not string
func (c *Column) AddStringVal(v StringVal) {
	if c.ci.colType != ColTypeString {
		panic(dfErrorf("Adding a StringVal to a %q column", c.ci.colType))
	}

	c.stringVals = append(c.stringVals, v)
}

// RowCount returns the number of rows in the column
func (c Column) RowCount() int {
	switch c.ci.colType {
	case ColTypeBool:
		return len(c.boolVals)
	case ColTypeInt:
		return len(c.intVals)
	case ColTypeFloat:
		return len(c.floatVals)
	case ColTypeString:
		return len(c.stringVals)
	default:
		panic(dfErrorf("Unexpected column type: %q", c.ci.colType))
	}
}

// checkRowIdx returns an error if the passed row index is outside the valid
// range
func (c Column) checkRowIdx(i int) error {
	if i < 0 || i >= c.RowCount() {
		return dfErrorf("There is no row %d (valid range: 0-%d)",
			i, c.RowCount()-1)
	}
	return nil
}

// GetVal returns the ith row of the column. It will return an error if i is
// not in the range of rows
func (c Column) GetVal(i int) (interface{}, error) {
	if err := c.checkRowIdx(i); err != nil {
		return nil, err
	}

	switch c.ci.colType {
	case ColTypeBool:
		return c.boolVals[i], nil
	case ColTypeInt:
		return c.intVals[i], nil
	case ColTypeFloat:
		return c.floatVals[i], nil
	case ColTypeString:
		return c.stringVals[i], nil
	default:
		panic(dfErrorf("Unexpected column type: %q", c.ci.colType))
	}
}

// GetBoolVal returns the ith row of the bool column. It will return an error
// if i is not in the range of rows or if the column is not a bool column
func (c Column) GetBoolVal(i int) (BoolVal, error) {
	if c.ci.colType != ColTypeBool {
		return BoolVal{IsNA: true},
			dfErrorf("Getting a BoolVal from a %q column", c.ci.colType)
	}
	if err := c.checkRowIdx(i); err != nil {
		return BoolVal{IsNA: true}, err
	}

	return c.boolVals[i], nil
}

// GetIntVal returns the ith row of the int column. It will return an error
// if i is not in the range of rows or if the column is not a int column
func (c Column) GetIntVal(i int) (IntVal, error) {
	if c.ci.colType != ColTypeInt {
		return IntVal{IsNA: true},
			dfErrorf("Getting a IntVal from a %q column", c.ci.colType)
	}
	if err := c.checkRowIdx(i); err != nil {
		return IntVal{IsNA: true}, err
	}

	return c.intVals[i], nil
}

// GetFloatVal returns the ith row of the float column. It will return an error
// if i is not in the range of rows or if the column is not a float column
func (c Column) GetFloatVal(i int) (FloatVal, error) {
	if c.ci.colType != ColTypeFloat {
		return FloatVal{IsNA: true},
			dfErrorf("Getting a FloatVal from a %q column", c.ci.colType)
	}
	if err := c.checkRowIdx(i); err != nil {
		return FloatVal{IsNA: true}, err
	}

	return c.floatVals[i], nil
}

// GetStringVal returns the ith row of the string column. It will return an error
// if i is not in the range of rows or if the column is not a string column
func (c Column) GetStringVal(i int) (StringVal, error) {
	if c.ci.colType != ColTypeString {
		return StringVal{IsNA: true},
			dfErrorf("Getting a StringVal from a %q column", c.ci.colType)
	}
	if err := c.checkRowIdx(i); err != nil {
		return StringVal{IsNA: true}, err
	}

	return c.stringVals[i], nil
}
