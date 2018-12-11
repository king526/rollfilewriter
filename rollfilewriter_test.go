package rollfilewriter_test

import (
	"testing"
	"time"

	"github.com/king526/rollfilewriter"
)

func Test_Sync(t *testing.T) {
	f, err := rollfilewriter.OpenFile("sync.log", nil)
	if err != nil {
		t.Fatal(err)
	}
	f.Write([]byte("hello" + time.Now().String() + "\n"))
	f.Close()
}

func Test_ASync(t *testing.T) {
	f, err := rollfilewriter.OpenFile("async.log", &rollfilewriter.Config{ASync: true})
	if err != nil {
		t.Fatal(err)
	}
	f.Write([]byte("hello async" + time.Now().String() + "\n"))
	f.Close()

}
