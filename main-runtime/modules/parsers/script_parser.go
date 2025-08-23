package parsers

import (
	"strings"
	"unicode"
)

type Sc_tokenT int

const (
	SC_none Sc_tokenT = iota

	// DB조작 키워드
	SC_createTable // 테이블 생성
	SC_add         // 데이터 추가
	SC_update      // 데이터 업데이트
	SC_get         // 데이터 가져오기
	SC_delete      // 데이터 삭제

	// 특수 키워드
	SC_key     // 열 키 지정
	SC_notNull // 열 널 허용 하지 아니함

	// 일반 토큰 타입
	SC_number // 숫자 타입 토큰
	SC_string // 문자/문자열 타입 토큰

	// 열 타입
	SC_columnNumber // 열 타입 숫자
	SC_columnText   // 열 타입 문자/문자열

	// 특수 토큰 타입
	SC_tableName  // 테이블 이름
	SC_columnName // 열 이름

	// 특수문자
	SC_comma      // , <- 콤마
	SC_parenOpen  // ( <- 소괄호 열림
	SC_parenClose // ) <- 소괄호 닫힘
	SC_endCmd     // ; <- 명령어 종료
)

type SC_token struct {
	Token      interface{}
	Token_type Sc_tokenT
}

func Parsing_script(input string, tokens *[]SC_token) int {
	i := 0
	n := len(input)

	for i < n {
		// 공백, 줄바꿈 무시
		if unicode.IsSpace(rune(input[i])) {
			i++
			continue
		}

		c := input[i]
		var prev string
		// 특수문자 처리
		switch c {
		case ',':
			*tokens = append(*tokens, SC_token{Token: ",", Token_type: SC_comma})
			i++
			continue
		case '(':
			*tokens = append(*tokens, SC_token{Token: "(", Token_type: SC_parenOpen})
			i++
			continue
		case ')':
			*tokens = append(*tokens, SC_token{Token: ")", Token_type: SC_parenClose})
			i++
			continue
		case ';':
			*tokens = append(*tokens, SC_token{Token: ";", Token_type: SC_endCmd})
			i++
			continue
		case '"':
			i++
			start := i
			escaped := false
			for i < n {
				if input[i] == '\\' && !escaped {
					escaped = true
					i++
					continue
				}
				if input[i] == '"' && !escaped {
					break // 종료 큰따옴표 발견
				}
				escaped = false
				i++
			}
			if i >= n {
				return 1 // 에러: 문자열 종료 없음
			}
			strVal := input[start:i]
			*tokens = append(*tokens, SC_token{Token: strVal, Token_type: SC_columnText})
			i++ // 종료 큰따옴표 넘김
			continue

		// 알파벳 혹은 _ 로 시작하는 식별자, 키워드, 테이블명, 열이름 등
		if isIdentStart(c) {
			start := i
			i++
			for i < n && isIdentPart(input[i]) {
				i++
			}
			word := input[start:i]
			lowerWord := strings.ToLower(word)

			switch lowerWord {
			case "create_table", "createtable":
				*tokens = append(*tokens, SC_token{Token: word, Token_type: SC_createTable})
			case "add":
				*tokens = append(*tokens, SC_token{Token: word, Token_type: SC_add})
			case "update":
				*tokens = append(*tokens, SC_token{Token: word, Token_type: SC_update})
			case "get":
				*tokens = append(*tokens, SC_token{Token: word, Token_type: SC_get})
			case "delete", "del":
				*tokens = append(*tokens, SC_token{Token: word, Token_type: SC_delete})
			case "key":
				*tokens = append(*tokens, SC_token{Token: word, Token_type: SC_key})
			case "notnull":
				*tokens = append(*tokens, SC_token{Token: word, Token_type: SC_notNull})
			case "number":
				*tokens = append(*tokens, SC_token{Token: word, Token_type: SC_columnNumber})
			case "text":
				*tokens = append(*tokens, SC_token{Token: word, Token_type: SC_columnText})
			default:
				// 토큰 구분은 파서에서 판단
				// 여기선 기본적으로 테이블명/열 이름으로 간주
				*tokens = append(*tokens, SC_token{Token: word, Token_type: SC_tableName})
			}
			continue
		}

		// 숫자 처리: 정수, 실수 (부호 포함)
		if unicode.IsDigit(rune(c)) || c == '-' {
			start := i
			i++
			dotCount := 0
			for i < n {
				ch := input[i]
				if unicode.IsDigit(rune(ch)) {
					i++
				} else if ch == '.' {
					dotCount++
					if dotCount > 1 {
						break
					}
					i++
				} else {
					break
				}
			}
			val := input[start:i]
			*tokens = append(*tokens, SC_token{Token: val, Token_type: SC_number})
			continue
		}

		// 알 수 없는 문자 발견 시 에러
		return 1

		prev = c
	}
	return 0
}

func isIdentStart(c byte) bool {
	return (c >= 'A' && c <= 'Z') ||
		(c >= 'a' && c <= 'z') ||
		c == '_'
}

func isIdentPart(c byte) bool {
	return isIdentStart(c) || (c >= '0' && c <= '9')
}

func Error_checker(tokens []SC_token, error_stream *string) int {
	if len(tokens) == 0 {
		*error_stream = "Empty token list\n<none>"
		return 1
	}

	pos := 0
	cmd := tokens[pos]
	pos++

	tokenOrNone := func(pos int) string {
		if pos >= len(tokens) {
			return "<none>"
		}
		tok := tokens[pos]
		if str, ok := tok.Token.(string); ok {
			return str
		}
		return "<non-string token>"
	}

	switch cmd.Token_type {
	case SC_createTable:
		// 테이블 이름 확인
		if pos >= len(tokens) || tokens[pos].Token_type != SC_tableName {
			*error_stream = "Missing or invalid table name\n" + tokenOrNone(pos)
			return 1
		}
		pos++

		// '(' 확인
		if pos >= len(tokens) || tokens[pos].Token_type != SC_parenOpen {
			*error_stream = "Expected '(' after table name\n" + tokenOrNone(pos)
			return 1
		}
		pos++

		keyCount := 0
		expectColumn := true

		for pos < len(tokens) {
			if tokens[pos].Token_type == SC_parenClose {
				pos++
				break
			}

			if !expectColumn {
				if tokens[pos].Token_type != SC_comma {
					*error_stream = "Expected ',' between columns\n" + tokenOrNone(pos)
					return 1
				}
				pos++
				expectColumn = true
				continue
			}

			// 열 타입 체크
			if pos >= len(tokens) || (tokens[pos].Token_type != SC_columnNumber && tokens[pos].Token_type != SC_columnText) {
				*error_stream = "Expected column type 'number' or 'text'\n" + tokenOrNone(pos)
				return 1
			}
			pos++

			// 열 이름 체크
			if pos >= len(tokens) || tokens[pos].Token_type != SC_tableName {
				*error_stream = "Expected column name\n" + tokenOrNone(pos)
				return 1
			}
			pos++

			// 선택 속성 (NOTNULL, KEY) 체크
			for pos < len(tokens) {
				tokType := tokens[pos].Token_type
				if tokType == SC_notNull {
					pos++
				} else if tokType == SC_key {
					keyCount++
					if keyCount > 1 {
						*error_stream = "Multiple KEY columns are not allowed\n" + tokenOrNone(pos)
						return 1
					}
					pos++
				} else {
					break
				}
			}

			expectColumn = false
		}

		if pos > len(tokens) {
			*error_stream = "Unexpected end of tokens\n<none>"
			return 1
		}

		// ')' 닫힘 체크
		if tokens[pos-1].Token_type != SC_parenClose {
			*error_stream = "Missing closing ')'\n" + tokenOrNone(pos-1)
			return 1
		}

		// 세미콜론 체크
		if pos >= len(tokens) || tokens[pos].Token_type != SC_endCmd {
			*error_stream = "Missing semicolon ';'\n" + tokenOrNone(pos)
			return 1
		}
		pos++

		return 0

	case SC_add:
		if pos >= len(tokens) || tokens[pos].Token_type != SC_tableName {
			*error_stream = "Missing or invalid table name\n" + tokenOrNone(pos)
			return 1
		}
		pos++

		if pos >= len(tokens) || tokens[pos].Token_type != SC_parenOpen {
			*error_stream = "Expected '(' after table name\n" + tokenOrNone(pos)
			return 1
		}
		pos++

		expectValue := true
		valueCount := 0

		for pos < len(tokens) {
			if tokens[pos].Token_type == SC_parenClose {
				pos++
				break
			}
			if !expectValue {
				if tokens[pos].Token_type != SC_comma {
					*error_stream = "Expected ',' between values\n" + tokenOrNone(pos)
					return 1
				}
				pos++
				expectValue = true
				continue
			}
			if tokens[pos].Token_type != SC_number && tokens[pos].Token_type != SC_string && tokens[pos].Token_type != SC_columnText {
				*error_stream = "Expected a value (number or string)\n" + tokenOrNone(pos)
				return 1
			}
			valueCount++
			pos++
			expectValue = false
		}

		if valueCount == 0 {
			*error_stream = "No data values provided\n<none>"
			return 1
		}

		if pos >= len(tokens) || tokens[pos].Token_type != SC_endCmd {
			*error_stream = "Missing semicolon ';'\n" + tokenOrNone(pos)
			return 1
		}
		pos++

		return 0

	case SC_update:
		if pos >= len(tokens) || tokens[pos].Token_type != SC_tableName {
			*error_stream = "Missing or invalid table name\n" + tokenOrNone(pos)
			return 1
		}
		pos++

		if pos >= len(tokens) || (tokens[pos].Token_type != SC_number && tokens[pos].Token_type != SC_string && tokens[pos].Token_type != SC_columnText) {
			*error_stream = "Missing or invalid key value\n" + tokenOrNone(pos)
			return 1
		}
		pos++

		if pos >= len(tokens) || tokens[pos].Token_type != SC_parenOpen {
			*error_stream = "Expected '(' after key value\n" + tokenOrNone(pos)
			return 1
		}
		pos++

		expectValue := true
		valueCount := 0

		for pos < len(tokens) {
			if tokens[pos].Token_type == SC_parenClose {
				pos++
				break
			}
			if !expectValue {
				if tokens[pos].Token_type != SC_comma {
					*error_stream = "Expected ',' between values\n" + tokenOrNone(pos)
					return 1
				}
				pos++
				expectValue = true
				continue
			}
			if tokens[pos].Token_type != SC_number && tokens[pos].Token_type != SC_string && tokens[pos].Token_type != SC_columnText {
				*error_stream = "Expected a value (number or string)\n" + tokenOrNone(pos)
				return 1
			}
			valueCount++
			pos++
			expectValue = false
		}

		if valueCount == 0 {
			*error_stream = "No data values provided\n<none>"
			return 1
		}

		if pos >= len(tokens) || tokens[pos].Token_type != SC_endCmd {
			*error_stream = "Missing semicolon ';'\n" + tokenOrNone(pos)
			return 1
		}
		pos++

		return 0

	case SC_get:
		if pos >= len(tokens) || tokens[pos].Token_type != SC_tableName {
			*error_stream = "Missing or invalid table name\n" + tokenOrNone(pos)
			return 1
		}
		pos++

		if pos >= len(tokens) || (tokens[pos].Token_type != SC_number && tokens[pos].Token_type != SC_string && tokens[pos].Token_type != SC_columnText) {
			*error_stream = "Missing or invalid key value\n" + tokenOrNone(pos)
			return 1
		}
		pos++

		if pos >= len(tokens) || tokens[pos].Token_type != SC_endCmd {
			*error_stream = "Missing semicolon ';'\n" + tokenOrNone(pos)
			return 1
		}
		pos++

		return 0

	case SC_delete:
		if pos >= len(tokens) || tokens[pos].Token_type != SC_tableName {
			*error_stream = "Missing or invalid table name\n" + tokenOrNone(pos)
			return 1
		}
		pos++

		if pos >= len(tokens) || (tokens[pos].Token_type != SC_number && tokens[pos].Token_type != SC_string && tokens[pos].Token_type != SC_columnText) {
			*error_stream = "Missing or invalid key value\n" + tokenOrNone(pos)
			return 1
		}
		pos++

		if pos >= len(tokens) || tokens[pos].Token_type != SC_endCmd {
			*error_stream = "Missing semicolon ';'\n" + tokenOrNone(pos)
			return 1
		}
		pos++

		return 0

	default:
		*error_stream = "Unknown command\n" + tokenOrNone(0)
		return 1
	}
}
