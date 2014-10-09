package sparkey

import (
	_ "fmt"
	"os"
	"testing"
)

const (
	testFilename string = "test.log"
)

func setup() {
	os.Remove(testFilename)
}

func teardown() {
	os.Remove(testFilename)
}

func TestCreateLog_NoCompression(t *testing.T) {
	setup()
	defer teardown()

	_, err := CreateLog(testFilename, None, 0)
	if err != nil {
		t.Error(err)
	}
}

func TestCreateLog_Snappy(t *testing.T) {
	setup()
	defer teardown()

	_, err := CreateLog(testFilename, Snappy, 12)
	if err != nil {
		t.Error(err)
	}
}

func TestAppendLog(t *testing.T) {
	setup()
	defer teardown()

	CreateLog(testFilename, None, 0)
	_, err := AppendLog(testFilename)
	if err != nil {
		t.Error(err)
	}
}

func TestClose(t *testing.T) {
	setup()
	defer teardown()

	lw, _ := CreateLog(testFilename, None, 0)

	if err := lw.Close(); err != nil {
		t.Error(err)
	}
}

func TestFlush(t *testing.T) {
	setup()
	defer teardown()

	lw, _ := CreateLog(testFilename, None, 0)

	if err := lw.Flush(); err != nil {
		t.Error(err)
	}
}

func TestPut(t *testing.T) {
	setup()
	defer teardown()

	lw, _ := CreateLog(testFilename, None, 0)

	if err := lw.Put("abc", "xyz"); err != nil {
		t.Error(err)
	}
}
