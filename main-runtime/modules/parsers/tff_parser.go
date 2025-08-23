package parsers

import (
	"strconv"
	"strings"
)

type Tff_tokenT int

const (
	Tff_none Tff_tokenT = iota
	// 선언 키워드
	Tff_title
	Tff_TableS
	Tff_Cnumber
	Tff_Ctext
	Tff_ColumnName
	Tff_dataSection

	// 속성
	Tff_Notnull
	Tff_Key

	// 데이터 타입
	Tff_string
	Tff_float64

	// 특수 토큰
	Tff_begin
	Tff_end
	Tff_dataStart
	Tff_dataEnd
	Tff_comma
)

type Tff_token struct {
	Token      interface{}
	Token_type Tff_tokenT
}

// 숫자 여부 판별 (간단하게)
func isNumeric(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c < '0' || c > '9') && c != '.' && c != '-' {
			return false
		}
	}
	return true
}

// ParseHeader : DATA_SECTION 전까지 파싱
func ParseHeader(input string, tokens *[]Tff_token) int {
	lines := strings.Split(input, "\n")
	inTable := false

	// 속성 매핑
	attrMap := map[string]Tff_tokenT{
		"NOTNULL": Tff_Notnull,
		"KEY":     Tff_Key,
	}

	// 열 타입 매핑
	typeMap := map[string]Tff_tokenT{
		"number": Tff_Cnumber,
		"text":   Tff_Ctext,
	}

	*tokens = make([]Tff_token, 0, len(lines)*2) // 대략 capacity 예측

	for _, raw := range lines {
		line := strings.TrimSpace(raw)
		if line == "" {
			continue
		}

		// 데이터 섹션 시작
		if strings.HasPrefix(line, "DATA_SECTION") {
			*tokens = append(*tokens, Tff_token{"DATA_SECTION", Tff_dataSection})
			return 0
		}

		// 타이틀
		if strings.HasPrefix(line, "Title") {
			colonIdx := strings.Index(line, ":")
			if colonIdx == -1 {
				return 1
			}
			*tokens = append(*tokens, Tff_token{"Title", Tff_title})
			val := strings.TrimSpace(line[colonIdx+1:])
			*tokens = append(*tokens, Tff_token{val, Tff_string})
			continue
		}

		// 테이블 시작
		if strings.HasPrefix(line, "TABLE_S") && strings.Contains(line, "BEGIN") {
			*tokens = append(*tokens, Tff_token{"TABLE_S", Tff_TableS})
			*tokens = append(*tokens, Tff_token{"BEGIN", Tff_begin})
			inTable = true
			continue
		}

		// 테이블 끝
		if inTable && strings.HasPrefix(line, "END") {
			*tokens = append(*tokens, Tff_token{"END", Tff_end})
			inTable = false
			continue
		}

		// 테이블 내용
		if inTable {
			// , 단위로만 쪼개기
			start := 0
			for i := 0; i <= len(line); i++ {
				if i == len(line) || line[i] == ',' {
					part := strings.TrimSpace(line[start:i])
					start = i + 1
					if part == "" {
						continue
					}

					fields := strings.Fields(part)
					if len(fields) < 2 {
						return 1
					}

					colType := strings.ToLower(fields[0])
					if t, ok := typeMap[colType]; ok {
						*tokens = append(*tokens, Tff_token{fields[0], t})
					} else {
						*tokens = append(*tokens, Tff_token{fields[0], Tff_none})
					}

					*tokens = append(*tokens, Tff_token{fields[1], Tff_ColumnName})

					if len(fields) > 2 {
						for _, attr := range fields[2:] {
							if t, ok := attrMap[strings.ToUpper(attr)]; ok {
								*tokens = append(*tokens, Tff_token{attr, t})
							}
						}
					}
				}
			}
		}
	}

	return 0
}

// ParseDataLine : Data-> [ ... ] ->End
func ParseDataLine(line string, tokens *[]Tff_token) int {
	line = strings.TrimSpace(line)
	if !strings.HasPrefix(line, "Data->") || !strings.HasSuffix(line, "->End") {
		return 1
	}

	*tokens = append(*tokens, Tff_token{"Data->", Tff_dataStart})

	body := strings.TrimPrefix(line, "Data->")
	body = strings.TrimSuffix(body, "->End")
	body = strings.TrimSpace(body)
	body = strings.Trim(body, "[]")

	start := 0
	for i := 0; i <= len(body); i++ {
		if i == len(body) || body[i] == ',' {
			part := strings.TrimSpace(body[start:i])
			start = i + 1
			if part == "" {
				continue
			}
			if isNumeric(part) {
				f, _ := strconv.ParseFloat(part, 64)
				*tokens = append(*tokens, Tff_token{f, Tff_float64})
			} else {
				*tokens = append(*tokens, Tff_token{part, Tff_string})
			}
			if i < len(body) {
				*tokens = append(*tokens, Tff_token{",", Tff_comma})
			}
		}
	}

	*tokens = append(*tokens, Tff_token{"->End", Tff_dataEnd})
	return 0
}
