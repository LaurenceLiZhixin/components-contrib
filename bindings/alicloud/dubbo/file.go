package dubbo

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

const BaseStoreLocation = "/dapr" + string(filepath.Separator) + "logs"

// FailBackState get items form target file
func FailBackState(prod string, action string) []string {
	filePath := getFilePath(prod, action, false)
	file, err := os.Open(filePath)
	if err != nil {
		fmt.Println("open the file error：", err)
	}
	defer file.Close()
	reader := bufio.NewReader(file)
	var result []string
	for {
		s, err := reader.ReadString('\n')
		if len(s) != 0 {
			//remove '\n'
			result = append(result, s[0:len(s)-1])
		}
		if err != nil {
			if err != io.EOF {
				fmt.Printf("read the file(%s) fail：%v", filePath, err)
			}
			break
		}
	}
	return result
}

// StoreState store @item to target file path, path is calculated by @prod and @action
func StoreState(prod string, action string, item string) {
	filePath := getFilePath(prod, action, true)

	if file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666); err != nil {
		fmt.Printf("write string(%s) into %s fail for open fail. %v", item, filePath, err)
	} else {
		if _, err = io.WriteString(file, item+"\n"); err != nil {
			fmt.Printf("write string(%s) into %s fail. %v", item, filePath, err)
		}
	}
}

// getFilePath get target file path
func getFilePath(prod string, action string, createDirectory bool) string {
	if createDirectory {
		path := "" + string(filepath.Separator) + prod + string(filepath.Separator) + "SNAPSHOT"
		if !IsDir(path) {
			if err := os.MkdirAll(path, os.ModePerm); err != nil {
				fmt.Printf("make directory fail. %+v", err)
			}
		}
	}
	return BaseStoreLocation + string(filepath.Separator) + prod + string(filepath.Separator) + "SNAPSHOT" +
		string(filepath.Separator) + action
}

// judge if target path is directory
func IsDir(path string) bool {
	s, err := os.Stat(path)
	if err != nil {
		return false
	}
	return s.IsDir()
}
