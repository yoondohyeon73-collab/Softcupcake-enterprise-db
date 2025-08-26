package dbinfo

import (
	"encoding/json"
	"os"
	"strconv"
)

// DBInfo
type DBInfo struct {
	DbName     string  // DB 이름
	IsBeta     bool    // DBMS 베타 여부
	ServerPort int     // 서버 포트
	Version    float32 // DBMS 버전 (소수점 포함)
}

// 내부 JSON 파싱용 구조체
type jsonConfig struct {
	DbName     string `json:"db_name"`
	ServerPort int    `json:"server_port"`
	Version    string `json:"version"`
	IsBeta     bool   `json:"is_beta"`
}

// LoadInfo
func LoadInfo(infoFileLocation string, info *DBInfo) int {
	data, err := os.ReadFile(infoFileLocation)
	if err != nil {
		return -1
	}

	var config jsonConfig
	err = json.Unmarshal(data, &config)
	if err != nil {
		return -1
	}

	// 문자열 version → float 변환
	versionFloat, err := strconv.ParseFloat(config.Version, 32)
	if err != nil {
		return -1
	}

	// DBInfo에 값 채우기
	info.DbName = config.DbName
	info.ServerPort = config.ServerPort
	info.IsBeta = config.IsBeta
	info.Version = float32(versionFloat)

	return 0
}
