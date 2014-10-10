package sparkey

import (
	"testing"
)

func TestReader(t *testing.T) {
	setup()
	defer teardown()

	ct := Snappy
	blockSize := 16

	key := "abc"
	val := "12345"

	// write a log file
	lw, _ := CreateLog(testFilename, ct, blockSize)
	lw.Put(key, val)
	lw.Close()

	// create a reader for that file
	re, err := NewReader(testFilename, "")
	if err != nil {
		t.Fatalf("creating new Reader: %v", err)
	}

	// verify headers and stuff
	if re.LogFilename != testFilename {
		t.Errorf("LogFilename is %v, want %v", re.LogFilename, testFilename)
	}
	if re.Compression != ct {
		t.Errorf("Compression is %v, want %v", re.Compression, ct)
	}
	if re.CompressionBlockSize != blockSize {
		t.Errorf("CompressionBlockSize is %v, want %v", re.CompressionBlockSize, blockSize)
	}
	if re.MaxKeyLen != uint64(len(key)) {
		t.Errorf("MaxKeyLen is %v, want %v", re.MaxKeyLen, len(key))
	}
	if re.MaxValueLen != uint64(len(val)) {
		t.Errorf("MaxValueLen is %v, want %v", re.MaxValueLen, len(val))
	}

	// make an iterator to get the data and verify state
	it, err := re.Iter()
	if err != nil {
		t.Fatalf("creating iterator: %v", err)
	}
	if is, _ := it.State(); is != New {
		t.Errorf("iterator state is %v, want %v", is, New)
	}

	// iterate to the first record and verify iterator state
	if is, err := it.Next(); err != nil {
		t.Fatalf("Iterator.Next: %v", err)
	} else if is != Active {
		t.Errorf("iterator state is %v, want %v", is, Active)
	}

	// verify correct data is read
	if it.Key != key {
		t.Errorf("read key %v, want %v", it.Key, key)
	}
	if it.Value != val {
		t.Errorf("read value %v, want %v", it.Value, val)
	}

	// iterate to end and verify state
	if is, err := it.Next(); err != nil {
		t.Fatalf("Iterator.Next: %v", err)
	} else if is != Closed {
		t.Errorf("iterator state is %v, want %v", is, Closed)
	}
}
