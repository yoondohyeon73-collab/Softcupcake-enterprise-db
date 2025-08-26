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
	SC_string // 문자열 값

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

	var last_token SC_token

	for i < n {
		// 공백, 줄바꿈 무시
		if unicode.IsSpace(rune(input[i])) {
			i++
			continue
		}

		c := input[i]
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
			*tokens = append(*tokens, SC_token{Token: strVal, Token_type: SC_string})
			i++ // 종료 큰따옴표 넘김
			continue

		default:
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
					tok := SC_token{Token: word, Token_type: SC_createTable}
					*tokens = append(*tokens, tok)
					last_token = tok
				case "add":
					tok := SC_token{Token: word, Token_type: SC_add}
					*tokens = append(*tokens, tok)
					last_token = tok
				case "update":
					tok := SC_token{Token: word, Token_type: SC_update}
					*tokens = append(*tokens, tok)
					last_token = tok
				case "get":
					tok := SC_token{Token: word, Token_type: SC_get}
					*tokens = append(*tokens, tok)
					last_token = tok
				case "delete", "del":
					tok := SC_token{Token: word, Token_type: SC_delete}
					*tokens = append(*tokens, tok)
					last_token = tok
				case "key":
					tok := SC_token{Token: word, Token_type: SC_key}
					*tokens = append(*tokens, tok)
					last_token = tok
				case "notnull":
					tok := SC_token{Token: word, Token_type: SC_notNull}
					*tokens = append(*tokens, tok)
					last_token = tok
				case "number":
					tok := SC_token{Token: word, Token_type: SC_columnNumber}
					*tokens = append(*tokens, tok)
					last_token = tok
				case "text":
					tok := SC_token{Token: word, Token_type: SC_columnText}
					*tokens = append(*tokens, tok)
					last_token = tok
				default:
					if last_token.Token_type == SC_columnNumber || last_token.Token_type == SC_columnText {
						tok := SC_token{Token: word, Token_type: SC_columnName}
						*tokens = append(*tokens, tok)
						last_token = tok
					} else if last_token.Token_type == SC_createTable {
						tok := SC_token{Token: word, Token_type: SC_tableName}
						*tokens = append(*tokens, tok)
						last_token = tok
					} else {
						return 1
					}
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
		}
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