package table

type Column_type int

const (
	CT_none Column_type = iota
	CT_number
	CT_text
)

type Column struct {
	Type     Column_type
	Name     string
	Is_key   bool
	Not_null bool
}

type Table struct {
	Table_name     string
	Columns_struct []Column
}

// NewTable creates a new Table
// Returns: 0 on success, -1 on error
func NewTable(name string, t *Table) int {
	if name == "" {
		return -1
	}
	t.Table_name = name
	t.Columns_struct = make([]Column, 0)
	return 0
}

// AddColumn adds a column to the table
// Returns: 0 on success, -1 on error
func AddColumn(t *Table, name string, colType Column_type, isKey bool, notNull bool) int {
	if name == "" {
		return -1
	}
	t.Columns_struct = append(t.Columns_struct, Column{
		Type:     colType,
		Name:     name,
		Is_key:   isKey,
		Not_null: notNull,
	})
	return 0
}

// AddText adds a text column
// Returns: 0 on success, -1 on error
func AddText(t *Table, name string, isKey bool, notNull bool) int {
	return AddColumn(t, name, CT_text, isKey, notNull)
}

// AddNumber adds a number column
// Returns: 0 on success, -1 on error
func AddNumber(t *Table, name string, isKey bool, notNull bool) int {
	return AddColumn(t, name, CT_number, isKey, notNull)
}

// Reset clears all columns
// Returns: 0 on success
func Reset(t *Table) int {
	t.Columns_struct = make([]Column, 0)
	return 0
}
