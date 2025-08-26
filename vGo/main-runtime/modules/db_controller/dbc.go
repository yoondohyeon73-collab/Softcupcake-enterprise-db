package dbcontroller

import (
	"fmt"
	"os"
	"path/filepath"
	dbinfo "sedb/db_info"
	"sedb/modules/parsers"
	"sedb/modules/table"
	"strconv"
	"strings"
)

// Row는 테이블의 단일 행을 나타냅니다.
type Row struct {
	Key  string                 `json:"key"`
	Data map[string]interface{} `json:"data"`
}

// TableData는 완전한 테이블 데이터 구조를 나타냅니다.
type TableData struct {
	Columns []table.Column `json:"columns"`
	Rows    []Row          `json:"rows"`
}

// printError는 오류 메시지를 출력하고 오류 코드를 반환합니다.
func printError(err string) int {
	fmt.Println(err)
	return 1
}

// tableExists는 테이블이 이미 존재하는지 확인합니다.
func tableExists(tableName string, dbInfo dbinfo.DB_info) bool {
	tablePath := filepath.Join("./", dbInfo.Db_name, "tables", tableName+".tff")
	_, err := os.Stat(tablePath)
	return !os.IsNotExist(err)
}

// loadTableData는 TFF 파일에서 테이블 구조와 데이터를 불러옵니다.
func loadTableData(tableName string, dbInfo dbinfo.DB_info) (*TableData, error) {
	tablePath := filepath.Join("./", dbInfo.Db_name, "tables", tableName+".tff")

	content, err := os.ReadFile(tablePath)
	if err != nil {
		return nil, err
	}

	// 헤더(테이블 구조) 파싱
	var headerTokens []parsers.Tff_token
	if parsers.ParseHeader(string(content), &headerTokens) != 0 {
		return nil, fmt.Errorf("failed to parse table header")
	}

	tableData := &TableData{
		Columns: make([]table.Column, 0),
		Rows:    make([]Row, 0),
	}

	// 헤더 토큰에서 열 정의 추출
	i := 0
	for i < len(headerTokens) {
		if headerTokens[i].Token_type == parsers.Tff_TableS {
			i++ // TABLE_S 건너뛰기
			if i < len(headerTokens) && headerTokens[i].Token_type == parsers.Tff_begin {
				i++ // BEGIN 건너뛰기

				// END가 나올 때까지 열 파싱
				for i < len(headerTokens) && headerTokens[i].Token_type != parsers.Tff_end {
					if i+1 < len(headerTokens) {
						// 열 타입
						var colType table.Column_type
						switch headerTokens[i].Token_type {
						case parsers.Tff_Cnumber:
							colType = table.CT_number
						case parsers.Tff_Ctext:
							colType = table.CT_text
						default:
							colType = table.CT_none
						}
						i++

						// 열 이름
						if i < len(headerTokens) && headerTokens[i].Token_type == parsers.Tff_ColumnName {
							colName := headerTokens[i].Token.(string)
							i++

							// 속성 확인
							isKey := false
							notNull := false

							for i < len(headerTokens) &&
								(headerTokens[i].Token_type == parsers.Tff_Key ||
									headerTokens[i].Token_type == parsers.Tff_Notnull) {

								switch headerTokens[i].Token_type {
								case parsers.Tff_Key:
									isKey = true
								case parsers.Tff_Notnull:
									notNull = true
								}
								i++
							}

							tableData.Columns = append(tableData.Columns, table.Column{
								Type:     colType,
								Name:     colName,
								Is_key:   isKey,
								Not_null: notNull,
							})
						}
					} else {
						i++
					}
				}
			}
		} else {
			i++
		}
	}

	// 데이터 섹션 파싱
	lines := strings.Split(string(content), "\n")
	inDataSection := false

	for _, line := range lines {
		line = strings.TrimSpace(line)

		if strings.HasPrefix(line, "DATA_SECTION") {
			inDataSection = true
			continue
		}

		if inDataSection && strings.HasPrefix(line, "Data->") && strings.HasSuffix(line, "->End") {
			var dataTokens []parsers.Tff_token
			if parsers.ParseDataLine(line, &dataTokens) == 0 {
				row := Row{
					Data: make(map[string]interface{}),
				}

				dataIndex := 0
				for _, token := range dataTokens {
					if token.Token_type == parsers.Tff_float64 || token.Token_type == parsers.Tff_string {
						if dataIndex < len(tableData.Columns) {
							col := tableData.Columns[dataIndex]

							var value interface{}
							if token.Token_type == parsers.Tff_float64 {
								value = fmt.Sprintf("%.0f", token.Token.(float64))
							} else {
								value = token.Token.(string)
							}

							if col.Is_key {
								row.Key = fmt.Sprintf("%v", value)
							}
							row.Data[col.Name] = value
							dataIndex++
						}
					}
				}

				if len(row.Data) > 0 {
					tableData.Rows = append(tableData.Rows, row)
				}
			}
		}
	}

	return tableData, nil
}

// saveTableData는 테이블 데이터를 TFF 파일에 저장합니다.
func saveTableData(tableData *TableData, tableName string, dbInfo dbinfo.DB_info) error {
	tablePath := filepath.Join("./", dbInfo.Db_name, "tables", tableName+".tff")

	file, err := os.Create(tablePath)
	if err != nil {
		return err
	}
	defer file.Close()

	// 제목 작성
	_, err = file.WriteString(fmt.Sprintf("Title : \"%s\"\n\n", tableName))
	if err != nil {
		return err
	}

	// TABLE_S 섹션 작성
	_, err = file.WriteString("TABLE_S BEGIN\n")
	if err != nil {
		return err
	}

	for i, col := range tableData.Columns {
		var colTypeStr string
		switch col.Type {
		case table.CT_number:
			colTypeStr = "NUMBER"
		case table.CT_text:
			colTypeStr = "TEXT"
		default:
			colTypeStr = "UNKNOWN"
		}

		line := fmt.Sprintf("    %s %s", colTypeStr, col.Name)

		if col.Not_null {
			line += " NOTNULL"
		}
		if col.Is_key {
			line += " KEY"
		}

		if i < len(tableData.Columns)-1 {
			line += ","
		}
		line += "\n"

		_, err = file.WriteString(line)
		if err != nil {
			return err
		}
	}

	_, err = file.WriteString("END\n\n")
	if err != nil {
		return err
	}

	// DATA_SECTION 작성
	_, err = file.WriteString("DATA_SECTION :\n")
	if err != nil {
		return err
	}

	for _, row := range tableData.Rows {
		dataStr := "Data-> ["
		for i, col := range tableData.Columns {
			if i > 0 {
				dataStr += ", "
			}
			dataStr += fmt.Sprintf("%v", row.Data[col.Name])
		}
		dataStr += "] ->End\n"

		_, err = file.WriteString(dataStr)
		if err != nil {
			return err
		}
	}

	return nil
}

// validateDataTypes는 열 타입에 따라 데이터를 검증합니다.
func validateDataTypes(data []string, columns []table.Column) error {
	if len(data) != len(columns) {
		return fmt.Errorf("data structure mismatch: expected %d values, received %d values",
			len(columns), len(data))
	}

	for i, value := range data {
		col := columns[i]

		// NOT NULL 제약 조건 확인
		if col.Not_null && value == "" {
			return fmt.Errorf("column '%s' cannot be NULL", col.Name)
		}

		// 데이터 타입 확인
		if value != "" {
			switch col.Type {
			case table.CT_number:
				if _, err := strconv.ParseFloat(value, 64); err != nil {
					return fmt.Errorf("invalid number format for column '%s': %s",
						col.Name, value)
				}
			case table.CT_text:
				// 텍스트 유효성 검사는 필요한 경우 여기에 추가할 수 있습니다.
				break
			}
		}
	}

	return nil
}

// findKeyColumn은 열 목록에서 키 열을 반환합니다.
func findKeyColumn(columns []table.Column) *table.Column {
	for i := range columns {
		if columns[i].Is_key {
			return &columns[i]
		}
	}
	return nil
}

// keyExists는 키가 테이블 데이터에 이미 존재하는지 확인합니다.
func keyExists(key string, tableData *TableData) bool {
	for _, row := range tableData.Rows {
		if row.Key == key {
			return true
		}
	}
	return false
}

// enhancedErrorChecker는 포괄적인 구문 및 의미 검증을 수행합니다.
func enhancedErrorChecker(tokens []parsers.SC_token, errBuffer *string) int {
	// parsers 패키지의 기존 Error_checker를 사용합니다.
	return parsers.Error_checker(tokens, errBuffer)
}

// handleCreateTable은 CREATE TABLE 명령을 처리합니다.
func handleCreateTable(tokens []parsers.SC_token, dbInfo dbinfo.DB_info) int {
	if len(tokens) < 2 {
		return printError("syntax error: table name is missing")
	}

	tableName := tokens[1].Token.(string)
	if tableName == "" {
		return printError("syntax error: invalid table name")
	}

	if tableExists(tableName, dbInfo) {
		return printError(fmt.Sprintf("error: table '%s' already exists", tableName))
	}

	var newTable table.Table
	if table.NewTable(tableName, &newTable) != 0 {
		return printError("error: failed to create table structure")
	}

	var columnStartIdx int = -1
	for i, token := range tokens {
		if token.Token_type == parsers.SC_parenOpen {
			columnStartIdx = i + 1
			break
		}
	}

	if columnStartIdx == -1 {
		return printError("syntax error: missing opening parenthesis")
	}

	keyColumnCount := 0
	i := columnStartIdx

	for i < len(tokens) && tokens[i].Token_type != parsers.SC_parenClose {
		if i >= len(tokens) {
			return printError("syntax error: unexpected end of statement")
		}

		var colType table.Column_type
		typeToken := tokens[i]

		switch typeToken.Token_type {
		case parsers.SC_columnNumber:
			colType = table.CT_number
		case parsers.SC_columnText:
			colType = table.CT_text
		default:
			return printError(fmt.Sprintf("syntax error: unknown column type '%v'",
				typeToken.Token))
		}
		i++

		if i >= len(tokens) {
			return printError("syntax error: column name is missing")
		}
		colName := tokens[i].Token.(string)
		if colName == "" {
			return printError("syntax error: invalid column name")
		}
		i++

		var isKey bool = false
		var notNull bool = false

		for i < len(tokens) &&
			tokens[i].Token_type != parsers.SC_comma &&
			tokens[i].Token_type != parsers.SC_parenClose {

			switch tokens[i].Token_type {
			case parsers.SC_notNull:
				notNull = true
			case parsers.SC_key:
				isKey = true
				keyColumnCount++
			default:
				return printError(fmt.Sprintf("syntax error: unknown attribute '%v'",
					tokens[i].Token))
			}
			i++
		}

		if table.AddColumn(&newTable, colName, colType, isKey, notNull) != 0 {
			return printError("error: failed to add column")
		}

		if i < len(tokens) && tokens[i].Token_type == parsers.SC_comma {
			i++
		}
	}

	if keyColumnCount != 1 {
		if keyColumnCount == 0 {
			return printError("error: exactly one KEY column is required")
		} else {
			return printError("error: only one KEY column is allowed per table")
		}
	}

	for _, col := range newTable.Columns_struct {
		if col.Is_key && !col.Not_null {
			return printError("error: KEY column cannot allow NULL values")
		}
	}

	if len(newTable.Columns_struct) == 0 {
		return printError("error: table must have at least one column")
	}

	tableData := &TableData{
		Columns: newTable.Columns_struct,
		Rows:    make([]Row, 0),
	}

	if err := saveTableData(tableData, tableName, dbInfo); err != nil {
		return printError(fmt.Sprintf("error: failed to create table file: %v", err))
	}

	fmt.Printf("Table '%s' created successfully\n", tableName)
	return 0
}

// parseDataFromTokens는 ADD/UPDATE 명령 토큰에서 데이터 값을 추출합니다.
func parseDataFromTokens(tokens []parsers.SC_token, startIdx int) []string {
	var dataValues []string
	inParens := false

	for i := startIdx; i < len(tokens); i++ {
		if tokens[i].Token_type == parsers.SC_parenOpen {
			inParens = true
			continue
		}
		if tokens[i].Token_type == parsers.SC_parenClose {
			break
		}
		if inParens && tokens[i].Token_type != parsers.SC_comma {
			dataValues = append(dataValues, fmt.Sprintf("%v", tokens[i].Token))
		}
	}

	return dataValues
}

// handleAdd는 ADD 명령을 처리합니다.
func handleAdd(tokens []parsers.SC_token, dbInfo dbinfo.DB_info) int {
	if len(tokens) < 4 {
		return printError("syntax error: incomplete ADD statement")
	}

	tableName := tokens[1].Token.(string)
	if !tableExists(tableName, dbInfo) {
		return printError(fmt.Sprintf("error: table '%s' does not exist", tableName))
	}

	tableData, err := loadTableData(tableName, dbInfo)
	if err != nil {
		return printError(fmt.Sprintf("error: failed to load table: %v", err))
	}

	dataTokens := parseDataFromTokens(tokens, 2)

	if len(dataTokens) == 0 {
		return printError("error: no data provided")
	}

	if err := validateDataTypes(dataTokens, tableData.Columns); err != nil {
		return printError(fmt.Sprintf("error: %v", err))
	}

	// 키 열 찾기 및 중복 확인
	keyCol := findKeyColumn(tableData.Columns)
	if keyCol == nil {
		return printError("error: cannot find key column")
	}

	var keyValue string
	for i, col := range tableData.Columns {
		if col.Is_key {
			keyValue = dataTokens[i]
			break
		}
	}

	if keyValue == "" {
		return printError("error: key value cannot be empty")
	}

	if keyExists(keyValue, tableData) {
		return printError(fmt.Sprintf("error: key '%s' already exists", keyValue))
	}

	// 새 행 생성
	newRow := Row{
		Key:  keyValue,
		Data: make(map[string]interface{}),
	}

	for i, col := range tableData.Columns {
		if i < len(dataTokens) {
			newRow.Data[col.Name] = dataTokens[i]
		}
	}

	tableData.Rows = append(tableData.Rows, newRow)

	if err := saveTableData(tableData, tableName, dbInfo); err != nil {
		return printError(fmt.Sprintf("error: failed to save table: %v", err))
	}

	fmt.Printf("Data successfully added to table '%s'\n", tableName)
	return 0
}

// handleUpdate는 UPDATE 명령을 처리합니다.
func handleUpdate(tokens []parsers.SC_token, dbInfo dbinfo.DB_info) int {
	if len(tokens) < 5 {
		return printError("syntax error: incomplete UPDATE statement")
	}

	tableName := tokens[1].Token.(string)
	keyValue := fmt.Sprintf("%v", tokens[2].Token)

	if !tableExists(tableName, dbInfo) {
		return printError(fmt.Sprintf("error: table '%s' does not exist", tableName))
	}

	tableData, err := loadTableData(tableName, dbInfo)
	if err != nil {
		return printError(fmt.Sprintf("error: failed to load table: %v", err))
	}

	// 업데이트할 행 찾기
	var targetRowIndex int = -1
	for i, row := range tableData.Rows {
		if row.Key == keyValue {
			targetRowIndex = i
			break
		}
	}

	if targetRowIndex == -1 {
		return printError(fmt.Sprintf("error: key '%s' not found", keyValue))
	}

	dataTokens := parseDataFromTokens(tokens, 3)

	if len(dataTokens) == 0 {
		return printError("error: no update data provided")
	}

	if err := validateDataTypes(dataTokens, tableData.Columns); err != nil {
		return printError(fmt.Sprintf("error: %v", err))
	}

	// 행 데이터 업데이트
	for i, col := range tableData.Columns {
		if i < len(dataTokens) {
			tableData.Rows[targetRowIndex].Data[col.Name] = dataTokens[i]
		}
	}

	if err := saveTableData(tableData, tableName, dbInfo); err != nil {
		return printError(fmt.Sprintf("error: failed to save table: %v", err))
	}

	fmt.Printf("Table '%s' updated successfully\n", tableName)
	return 0
}

// handleGet는 GET 명령을 처리합니다.
func handleGet(tokens []parsers.SC_token, dbInfo dbinfo.DB_info) int {
	if len(tokens) < 3 {
		return printError("syntax error: incomplete GET statement")
	}

	tableName := tokens[1].Token.(string)
	keyValue := fmt.Sprintf("%v", tokens[2].Token)

	if !tableExists(tableName, dbInfo) {
		return printError(fmt.Sprintf("error: table '%s' does not exist", tableName))
	}

	tableData, err := loadTableData(tableName, dbInfo)
	if err != nil {
		return printError(fmt.Sprintf("error: failed to load table: %v", err))
	}

	// 행 찾기
	var targetRow *Row
	for i := range tableData.Rows {
		if tableData.Rows[i].Key == keyValue {
			targetRow = &tableData.Rows[i]
			break
		}
	}

	if targetRow == nil {
		return printError(fmt.Sprintf("error: key '%s' not found", keyValue))
	}

	// 결과 표시
	fmt.Printf("Data for key '%s' in table '%s':\n", keyValue, tableName)
	for _, col := range tableData.Columns {
		fmt.Printf("  %s: %v\n", col.Name, targetRow.Data[col.Name])
	}

	return 0
}

// handleDelete는 DELETE 명령을 처리합니다.
func handleDelete(tokens []parsers.SC_token, dbInfo dbinfo.DB_info) int {
	if len(tokens) < 3 {
		return printError("syntax error: incomplete DELETE statement")
	}

	tableName := tokens[1].Token.(string)
	keyValue := fmt.Sprintf("%v", tokens[2].Token)

	if !tableExists(tableName, dbInfo) {
		return printError(fmt.Sprintf("error: table '%s' does not exist", tableName))
	}

	tableData, err := loadTableData(tableName, dbInfo)
	if err != nil {
		return printError(fmt.Sprintf("error: failed to load table: %v", err))
	}

	// 행 찾기 및 제거
	var targetRowIndex int = -1
	for i, row := range tableData.Rows {
		if row.Key == keyValue {
			targetRowIndex = i
			break
		}
	}

	if targetRowIndex == -1 {
		return printError(fmt.Sprintf("error: key '%s' not found", keyValue))
	}

	// 행 제거
	tableData.Rows = append(tableData.Rows[:targetRowIndex],
		tableData.Rows[targetRowIndex+1:]...)

	if err := saveTableData(tableData, tableName, dbInfo); err != nil {
		return printError(fmt.Sprintf("error: failed to save table: %v", err))
	}

	fmt.Printf("Row with key '%s' successfully deleted from table '%s'\n",
		keyValue, tableName)
	return 0
}

// CmdExec은 데이터베이스 명령을 실행합니다.
func CmdExec(script string, dbInfo dbinfo.DB_info) int {
	// 스크립트를 토큰으로 파싱
	var scriptTokens []parsers.SC_token
	if parsers.Parsing_script(script, &scriptTokens) != 0 {
		return printError("error: failed to parse script")
	}

	// parsers 패키지를 사용한 오류 검사
	var errBuffer string
	err := enhancedErrorChecker(scriptTokens, &errBuffer)
	if err == 1 {
		return printError(errBuffer)
	}

	// 첫 번째 토큰에 따라 명령 실행
	if len(scriptTokens) == 0 {
		return printError("error: empty script")
	}

	switch scriptTokens[0].Token_type {
	case parsers.SC_createTable:
		return handleCreateTable(scriptTokens, dbInfo)
	case parsers.SC_get:
		return handleGet(scriptTokens, dbInfo)
	case parsers.SC_update:
		return handleUpdate(scriptTokens, dbInfo)
	case parsers.SC_delete:
		return handleDelete(scriptTokens, dbInfo)
	case parsers.SC_add:
		return handleAdd(scriptTokens, dbInfo)
	default:
		return printError("error: unknown command")
	}
}