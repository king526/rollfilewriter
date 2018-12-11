package rollfilewriter

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	rollFormat = "060102150405"
)

type Config struct {
	RollMB     int    //roll at fix size (MB).
	RollPerDay bool   //roll at every day
	Retain     int    //remove roll files. 0 for no remove
	ASync      bool   //write io async
	BufferSize int    // async write buffer,default 512
	OpenNew    bool   //create new file when OpenFile
	Path       string //the base file.

}

type RollFileWriter struct {
	conf     Config
	file     *os.File
	size     int64 //current file size
	curDay   int   //current file day
	rollSize int64 //rolling size

	hisNames []string       //history file names
	syncL    sync.Mutex     //lock for sync write
	asynch   chan []byte    //chan for async write
	lastErr  atomic.Value   //store error for async write
	wg       sync.WaitGroup //close wait for async write
}

// OpenFile open the rolling files for append write.
// a nil config like open write file as os.OpenFile(fileName, os.O_CREATE|os.O_APPEND|os.O_WRONLY,0666)
func OpenFile(fileName string, conf *Config) (*RollFileWriter, error) {
	dir := filepath.Dir(fileName)
	name := filepath.Base(fileName)
	if len(name) == 0 {
		return nil, fmt.Errorf("empty file name")
	}
	os.MkdirAll(dir, os.ModeDir|0666)
	fi, err := os.Stat(fileName)
	if err != nil && os.IsExist(err) {
		return nil, err
	}
	if fi != nil && fi.IsDir() {
		return nil, fmt.Errorf("path is directory")
	}
	w := &RollFileWriter{}
	if conf != nil {
		w.conf = *conf

	}
	w.conf.Path = fileName
	w.rollSize = int64(w.conf.RollMB) * 1024 * 1024
	if w.conf.ASync && w.conf.BufferSize < 1 {
		w.conf.BufferSize = 512
	}
	roll := w.conf.RollPerDay || w.rollSize != 0
	if fi != nil && w.conf.OpenNew {
		if roll {
			w.roll()
		} else {
			os.Remove(fileName)
		}
	}
	if roll {
		err = filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
			if err != nil || info.IsDir() {
				return err
			}
			fileName := info.Name()
			if len(fileName) != len(name)+len(rollFormat)+1 {
				return nil
			}
			if !strings.HasPrefix(fileName, name+".") {
				return nil
			}
			_, err = time.Parse(rollFormat, fileName[len(name):])
			if err == nil {
				w.hisNames = append(w.hisNames, info.Name())
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
		sort.Strings(w.hisNames)
		w.clean()
	}
	if err = w.open(); err != nil {
		return nil, err
	}
	if w.conf.ASync {
		w.asynch = make(chan []byte, w.conf.BufferSize)
		w.wg.Add(1)
		go func() {
			w.asyncWrite()
		}()
	}
	return w, nil
}

func (r *RollFileWriter) Close() error {
	if !r.conf.ASync {
		r.syncL.Lock()
		defer r.syncL.Unlock()
		return r.file.Close()
	}
	defer recover()
	r.asynch <- nil
	r.wg.Wait()
	return nil
}

func (r *RollFileWriter) Write(bs []byte) (wrote int, err error) {
	if bs == nil {
		return
	}
	defer func() {
		if e := recover(); e != nil {
			if e1, ok := e.(error); ok {
				err = e1
			} else {
				err = fmt.Errorf("%v", e1)
			}
			wrote = 0
		}
	}()
	if !r.conf.ASync {
		return r.syncWrite(bs)
	}
	if e := r.lastErr.Load(); e != nil {
		err = e.(error)
		return
	}
	r.asynch <- bs
	wrote = len(bs)
	return
}

func (r *RollFileWriter) WriteString(s string) (int, error) {
	return r.Write([]byte(s))
}

func (r *RollFileWriter) checkAndRoll() error {
	var roll bool
	if r.rollSize != 0 && r.size > r.rollSize {
		roll = true
	}
	if r.conf.RollPerDay && time.Now().Day() != r.curDay {
		roll = true
	}
	if !roll {
		return nil
	}
	r.file.Close()
	r.roll()
	r.clean()
	return r.open()
}

func (r *RollFileWriter) open() error {
	fs, err := os.OpenFile(r.conf.Path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)
	if err != nil {
		return err
	}
	fi, err := fs.Stat()
	if err != nil {
		return err
	}
	r.file = fs
	r.size = fi.Size()
	r.curDay = time.Now().Day()
	return nil
}

func (r *RollFileWriter) roll() error {
	backupPath := r.conf.Path + "." + time.Now().Format(rollFormat)
	err := os.Rename(r.conf.Path, backupPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "rename file failed", r.conf.Path, err)
		return err
	}
	r.hisNames = append(r.hisNames, backupPath)
	return nil
}

func (r *RollFileWriter) clean() {
	if r.conf.Retain == 0 {
		return
	}
	rem := len(r.hisNames) - r.conf.Retain
	if rem < 1 {
		return
	}
	baseDir := filepath.Dir(r.conf.Path)
	var err error
	for _, name := range r.hisNames[:rem] {
		err = os.Remove(filepath.Join(baseDir, name))
		if err != nil {
			fmt.Println("remove old file failed:", filepath.Join(baseDir, name), err)
		}
	}
	if err == nil {
		r.hisNames = r.hisNames[rem:]
	}
}

func (r *RollFileWriter) write(bs []byte) (wrote int, err error) {
	wrote, err = r.file.Write(bs)
	r.size += int64(wrote)
	return
}

func (r *RollFileWriter) syncWrite(bs []byte) (wrote int, err error) {
	r.syncL.Lock()
	defer r.syncL.Unlock()
	if err = r.checkAndRoll(); err != nil {
		return
	}
	wrote, err = r.write(bs)
	if err == nil {
		r.file.Sync()
	}
	return
}

func (r *RollFileWriter) asyncWrite() {
	defer func() {
		if e := recover(); e != nil {
			if err, ok := e.(error); ok {
				r.lastErr.Store(err)
			} else {
				r.lastErr.Store(fmt.Errorf("%v", e))
			}
		} else {
			r.lastErr.Store(fmt.Errorf("Closed"))
		}
		r.file.Close()
		close(r.asynch)
		r.wg.Done()
	}()
	var n int
	var e error
	for {
		n = 0
	loop:
		for {
			select {
			case s := <-r.asynch:
				if s == nil {
					return
				}
				if e = r.checkAndRoll(); e != nil {
					r.lastErr.Store(e)
					return
				}
				if n, e = r.write(s); e != nil {
					r.lastErr.Store(e)
					return
				}
			default:
				break loop
			}
		}
		if n != 0 {
			r.file.Sync()
		}
		<-time.After(time.Second)
	}
}
