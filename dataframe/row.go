package dataframe

// RowData holds all the data for a row. It has any bool values in the
// boolVals slice, ints in the intVals slice and so on. It should be used
// with a MultiColInfo which maps the values to names (and vice versa).
type RowData struct {
	boolVals   []BoolVal
	intVals    []IntVal
	floatVals  []FloatVal
	stringVals []StringVal
}

// Row is a single RowData with the associated MultiColInfo to give semantic
// meaning to the values.
type Row struct {
	mci MultiColInfo
	rd  RowData
}

// NewRow creates an empty row with the columns as given and the data all set
// to the IsNA values
func NewRow(cis ...ColInfo) (*Row, error) {
	mci, err := NewMultiColInfo(cis...)
	if err != nil {
		return nil, err
	}
	r := &Row{
		mci: *mci,
	}
	for _, ci := range mci.info {
		switch ci.colType {
		case ColTypeBool:
			r.rd.boolVals = append(r.rd.boolVals, BoolVal{IsNA: true})
		case ColTypeInt:
			r.rd.intVals = append(r.rd.intVals, IntVal{IsNA: true})
		case ColTypeFloat:
			r.rd.floatVals = append(r.rd.floatVals, FloatVal{IsNA: true})
		case ColTypeString:
			r.rd.stringVals = append(r.rd.stringVals, StringVal{IsNA: true})
		}
	}
	return r, nil
}

// AddBool adds a new bool val to the row. If the name is already in the row
// an error is returned
func (r *Row) AddBool(name string, v BoolVal) error {
	err := (&r.mci).Add(ColInfo{name: name, colType: ColTypeBool})
	if err != nil {
		return err
	}

	r.rd.boolVals = append(r.rd.boolVals, v)

	return nil
}

// AddInt adds a new int val to the row. If the name is already in the row
// an error is returned
func (r *Row) AddInt(name string, v IntVal) error {
	err := (&r.mci).Add(ColInfo{name: name, colType: ColTypeInt})
	if err != nil {
		return err
	}

	r.rd.intVals = append(r.rd.intVals, v)

	return nil
}

// AddFloat adds a new float val to the row. If the name is already in the row
// an error is returned
func (r *Row) AddFloat(name string, v FloatVal) error {
	err := (&r.mci).Add(ColInfo{name: name, colType: ColTypeFloat})
	if err != nil {
		return err
	}

	r.rd.floatVals = append(r.rd.floatVals, v)

	return nil
}

// AddString adds a new string val to the row. If the name is already in the row
// an error is returned
func (r *Row) AddString(name string, v StringVal) error {
	err := (&r.mci).Add(ColInfo{name: name, colType: ColTypeString})
	if err != nil {
		return err
	}

	r.rd.stringVals = append(r.rd.stringVals, v)

	return nil
}

// ValByIdx returns a value and its associated type from the Row
// corresponding to the supplied column index. If the column index is not
// recognised then an error is returned.
func (r *Row) ValByIdx(idx int) (any, ColType, error) {
	if idx < 0 || idx >= len(r.mci.info) {
		return nil,
			ColTypeUnknown,
			dfErrorf("There is no column %d (valid range: 0-%d)",
				idx, len(r.mci.info)-1)
	}
	cType := r.mci.info[idx].colType
	switch cType {
	case ColTypeBool:
		return r.rd.boolVals[r.mci.valIdx[idx]], cType, nil
	case ColTypeInt:
		return r.rd.intVals[r.mci.valIdx[idx]], cType, nil
	case ColTypeFloat:
		return r.rd.floatVals[r.mci.valIdx[idx]], cType, nil
	case ColTypeString:
		return r.rd.stringVals[r.mci.valIdx[idx]], cType, nil
	}

	return nil, cType, dfErrorf("Unexpected column type: %q", cType)
}

// ValByName returns a value and its associated type from the Row
// corresponding to the supplied column name. If the column name is not
// recognised then an error is returned.
func (r *Row) ValByName(name string) (any, ColType, error) {
	ci, ok := r.mci.nameToCol[name]
	if !ok {
		return nil, ColTypeUnknown, dfErrorf("Unknown column name: %q", name)
	}
	return r.ValByIdx(ci)
}

// MakeDF creates a dataframe with the same structure (same column types and
// names in the same order) as the row.
func (r *Row) MakeDF() *DF {
	return &DF{
		mci: r.mci.Clone(),

		boolCols:   make([][]BoolVal, len(r.rd.boolVals)),
		intCols:    make([][]IntVal, len(r.rd.intVals)),
		floatCols:  make([][]FloatVal, len(r.rd.floatVals)),
		stringCols: make([][]StringVal, len(r.rd.stringVals)),
	}
}

// ColsByIdx returns a slice of columns from the row with the given
// indexes. It will return an error if any index is out of range
func (r *Row) ColsByIdx(indexes ...int) ([]Column, error) {
	rval := make([]Column, 0, len(indexes))

	for _, i := range indexes {
		if i < 0 || i >= len(r.mci.info) {
			return nil, dfErrorf("There is no column %d (valid range: 0-%d)",
				i, len(r.mci.info)-1)
		}
		col := Column{ci: r.mci.info[i]}
		switch col.ci.colType {
		case ColTypeBool:
			col.boolVals = append(col.boolVals, r.rd.boolVals[r.mci.valIdx[i]])
		case ColTypeInt:
			col.intVals = append(col.intVals, r.rd.intVals[r.mci.valIdx[i]])
		case ColTypeFloat:
			col.floatVals = append(col.floatVals, r.rd.floatVals[r.mci.valIdx[i]])
		case ColTypeString:
			col.stringVals = append(col.stringVals, r.rd.stringVals[r.mci.valIdx[i]])
		default:
			panic(dfErrorf("Unexpected column type: %q", col.ci.colType))
		}
		rval = append(rval, col)
	}
	return rval, nil
}

// ColsByName returns a slice of columns from the row with the given
// names. It will return an error if any name is not found
func (r *Row) ColsByName(names ...string) ([]Column, error) {
	rval := make([]Column, 0, len(names))

	for _, name := range names {
		i, ok := r.mci.nameToCol[name]
		if !ok {
			return nil, dfErrorf("Unknown column name: %q", name)
		}
		cols, err := r.ColsByIdx(i)
		if err != nil {
			return nil, err
		}
		rval = append(rval, cols...)
	}
	return rval, nil
}

// ColsByNameOrPanic calls ColsByName and returns the columns or panics if a
// non-nil error is returned
func (r *Row) ColsByNameOrPanic(names ...string) []Column {
	cols, err := r.ColsByName(names...)
	if err != nil {
		panic(err)
	}
	return cols
}
