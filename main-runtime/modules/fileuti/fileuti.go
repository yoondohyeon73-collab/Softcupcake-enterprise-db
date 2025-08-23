package fileuti

import (
	"bufio"
	"os"
	"strings"
)

// 파일이 존재하는지 확인
func FileExists(filename string) int {
	_, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return 1 // 파일이 존재하지 않으면 1 반환
	}
	return 0 // 파일이 존재하면 0 반환
}

// 파일을 읽고 내용을 반환
func ReadFile(filename string) (string, int) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return "", 1 // 오류 발생 시 1 반환
	}
	return string(data), 0 // 정상적으로 읽었으면 0 반환
}

// 파일에 문자열을 씁니다.
func WriteToFile(filename, content string) int {
	file, err := os.Create(filename)
	if err != nil {
		return 1 // 오류 발생 시 1 반환
	}
	defer file.Close()

	_, err = file.WriteString(content)
	if err != nil {
		return 1 // 오류 발생 시 1 반환
	}
	return 0 // 정상적으로 작성되었으면 0 반환
}

// 파일에 덧붙여서 씁니다.
func AppendToFile(filename, content string) int {
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return 1 // 오류 발생 시 1 반환
	}
	defer file.Close()

	_, err = file.WriteString(content)
	if err != nil {
		return 1 // 오류 발생 시 1 반환
	}
	return 0 // 정상적으로 덧붙여졌으면 0 반환
}

// 파일 삭제
func DeleteFile(filename string) int {
	err := os.Remove(filename)
	if err != nil {
		return 1 // 오류 발생 시 1 반환
	}
	return 0 // 정상적으로 삭제되었으면 0 반환
}

// 디렉토리 생성
func CreateDirectory(dirName string) int {
	err := os.MkdirAll(dirName, os.ModePerm)
	if err != nil {
		return 1 // 오류 발생 시 1 반환
	}
	return 0 // 디렉토리가 정상적으로 생성되었으면 0 반환
}

// 파일을 한 줄씩 읽기
func ReadFileLineByLine(filename string) ([]string, int) {
	var lines []string
	file, err := os.Open(filename)
	if err != nil {
		return nil, 1 // 오류 발생 시 1 반환
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		return nil, 1 // 오류 발생 시 1 반환
	}

	return lines, 0 // 정상적으로 읽었으면 0 반환
}

// 파일의 내용을 수정하는 함수
func ModifyFileContent(filename, oldContent, newContent string) int {
	data, errCode := ReadFile(filename)
	if errCode != 0 {
		return 1 // 파일 읽기 실패 시 1 반환
	}

	data = strings.ReplaceAll(data, oldContent, newContent)
	errCode = WriteToFile(filename, data)
	return errCode
}
