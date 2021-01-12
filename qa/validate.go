package qa

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
)

func Validate(ins []string, schemas []string, outDir string) (err error) {
	fmt.Println("validates the data in:", ins, "schemas:", schemas, "outDir:", outDir)

	files := getListOfFiles(ins)
	if len(files) > 0 {
		fmt.Println("input files are:")
	}
	for _, f := range files {
		fmt.Println(f)
	}
	return nil
}

func getListOfFiles(ins []string) (files []string) {
	for _, in := range ins {
		fmt.Println("checking in:", in)

		if isDir(in) {
			subDirFiles := getFilesFromDir(in)
			files = append(files, subDirFiles...)
			continue
		}

		if !fileExists(in) {
			fmt.Printf("file does not exist: %v\n", in)
			continue
		}
		fmt.Println("file exists:", in)

		files = append(files, in)
	}

	return uniqueStringSlice(files)
}

func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}

func isDir(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return info.IsDir()
}

func getFilesFromDir(dir string) (files []string) {
	fs, _ := ioutil.ReadDir(dir)
	for _, f := range fs {
		if f.IsDir() {
			continue
		}

		files = append(files, filepath.Join(dir, f.Name()))

	}
	return files
}
