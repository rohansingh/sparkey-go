package sparkey

import (
	"os"
	"testing"
)

const (
	testFilename     string = "test.log"
	testHashFilename string = "test.hash"
)

func setup() {
	os.Remove(testFilename)
	os.Remove(testHashFilename)
}

func teardown() {
	os.Remove(testFilename)
	os.Remove(testHashFilename)
}

func TestCreateLog_NoCompression(t *testing.T) {
	setup()
	defer teardown()

	if _, err := CreateLog(testFilename, None, 0); err != nil {
		t.Error(err)
	}

	if _, err := os.Stat(testFilename); err != nil {
		t.Errorf("checking log file: %s", err)
	}
}

func TestCreateLog_Snappy(t *testing.T) {
	setup()
	defer teardown()

	if _, err := CreateLog(testFilename, Snappy, 12); err != nil {
		t.Error(err)
	}

	if _, err := os.Stat(testFilename); err != nil {
		t.Errorf("checking log file: %s", err)
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
