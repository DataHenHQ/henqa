package qa

import "fmt"

func Validate(in []string, schemas []string, outDir string) (err error) {
	fmt.Println("validates the data in:", in, "schemas:", schemas, "outDir:", outDir)
	return nil
}
