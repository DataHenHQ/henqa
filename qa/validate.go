package qa

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	jsonpatch "github.com/evanphx/json-patch/v5"
	"github.com/xeipuuv/gojsonschema"
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

	mergedSchema, err := getAndMergeSchemaFiles(schemas)
	if err != nil {
		fmt.Println("gotten error with merging schemas:", err.Error())
		fmt.Println("aborting operation.")
	}
	fmt.Println("merged Schema:")
	fmt.Println(string(mergedSchema))

	err = validateWithSchema(files, mergedSchema, outDir)
	if err != nil {
		fmt.Println("gotten error running the validation:", err.Error())
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

func getAndMergeSchemaFiles(files []string) (schema []byte, err error) {

	for _, f := range files {
		nschema, err := ioutil.ReadFile(f)
		if err != nil {
			return nil, err
		}

		if schema == nil {
			schema = nschema
			continue
		}

		newSchema, err := jsonpatch.MergeMergePatches(schema, nschema)
		if err != nil {
			fmt.Print("cannot merge schema:", err.Error())
			return nil, err
		}

		schema = newSchema

	}
	return schema, nil
}

func validateWithSchema(files []string, schema []byte, outDir string) (err error) {
	schemaSl := gojsonschema.NewStringLoader(string(schema))
	sl := gojsonschema.NewSchemaLoader()
	sl.Validate = true
	err = sl.AddSchemas(schemaSl)
	if err != nil {
		return err
	}

	for _, f := range files {
		jsonB, err := ioutil.ReadFile(f)
		if err != nil {
			fmt.Println("gotten error reading ", f, ":", err.Error())
			continue
		}

		doc := gojsonschema.NewStringLoader(string(jsonB))

		res, err := gojsonschema.Validate(schemaSl, doc)
		if err != nil {
			fmt.Println("gotten validation error:", err)
		}

		fmt.Println("validation result:", res.Errors())
	}

	return nil
}
