package main

import (
	"fmt"
	"strings"
)

// 测试多命令检测逻辑
func splitCommands(query string) []string {
	commands := strings.Split(query, ";")
	
	var result []string
	for _, cmd := range commands {
		cmd = strings.TrimSpace(cmd)
		if cmd != "" {
			result = append(result, cmd)
		}
	}
	
	return result
}

func isMultiCommandQuery(query string) bool {
	trimmed := strings.TrimSpace(query)
	if trimmed == "" {
		return false
	}
	
	commands := splitCommands(query)
	return len(commands) > 1
}

func main() {
	testQueries := []string{
		"SELECT 1;",
		"SELECT 1",
		"set work_mem=1;SELECT 1;",
		"SET DateStyle=ISO; SET client_min_messages=notice; SELECT set_config('bytea_output','hex',false) FROM pg_show_all_settings() WHERE name = 'bytea_output'; SET client_encoding='utf-8';",
		"",
		";;;",
		"SELECT 1; SELECT 2; SELECT 3;",
	}
	
	fmt.Println("=== 多命令检测测试 ===")
	
	for i, query := range testQueries {
		isMulti := isMultiCommandQuery(query)
		commands := splitCommands(query)
		
		fmt.Printf("\n测试 %d:\n", i+1)
		fmt.Printf("查询: %q\n", query)
		fmt.Printf("是否多命令: %v\n", isMulti)
		fmt.Printf("拆分结果 (%d 个命令):\n", len(commands))
		for j, cmd := range commands {
			fmt.Printf("  %d: %q\n", j+1, cmd)
		}
	}
}
