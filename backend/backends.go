// Copyright 2016 Eleme. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package backend

import (
	"bytes"
	"io"
	"log"
	"sync"
	"time"

	"github.com/panjf2000/ants/v2"
)

type Backends struct {
	*HttpBackend
	fb              *FileBackend
	pool            *ants.Pool
	FlushSize       int
	FlushTime       int
	RewriteInterval int

	running          bool
	ticker           *time.Ticker
	ch_write         chan []byte
	buffer           *bytes.Buffer
	ch_timer         <-chan time.Time
	write_counter    int
	rewriter_running bool
	wg               sync.WaitGroup
}

// maybe ch_timer is not the best way.
func NewBackends(cfg *BackendConfig, name string, datadir string) (bs *Backends, err error) {
	bs = &Backends{
		HttpBackend:      NewHttpBackend(cfg),
		FlushSize:        cfg.FlushSize,
		FlushTime:        cfg.FlushTime,
		RewriteInterval:  cfg.RewriteInterval,
		running:          true,
		ticker:           time.NewTicker(time.Millisecond * time.Duration(cfg.RewriteInterval)),
		ch_write:         make(chan []byte, 16),
		rewriter_running: false,
	}
	bs.fb, err = NewFileBackend(name, datadir)
	if err != nil {
		return
	}
	bs.pool, err = ants.NewPool(cfg.ConnPoolSize)
	if err != nil {
		return
	}

	go bs.worker()
	return
}

func (bs *Backends) worker() {
	for bs.running {
		select {
		case p, ok := <-bs.ch_write:
			if !ok {
				// closed
				bs.Flush()
				bs.wg.Wait()
				bs.HttpBackend.Close()
				bs.fb.Close()
				return
			}
			bs.WriteBuffer(p)

		case <-bs.ch_timer:
			bs.Flush()
			if !bs.running {
				bs.wg.Wait()
				bs.HttpBackend.Close()
				bs.fb.Close()
				return
			}

		case <-bs.ticker.C:
			bs.Idle()
		}
	}
}

func (bs *Backends) Write(p []byte) (err error) {
	if !bs.running {
		return io.ErrClosedPipe
	}

	bs.ch_write <- p
	return
}

func (bs *Backends) Close() (err error) {
	bs.running = false
	bs.pool.Release()
	close(bs.ch_write)
	return
}

func (bs *Backends) WriteBuffer(p []byte) {
	bs.write_counter++

	if bs.buffer == nil {
		bs.buffer = &bytes.Buffer{}
	}

	n, err := bs.buffer.Write(p)
	if err != nil {
		log.Printf("error: %s\n", err)
		return
	}
	if n != len(p) {
		err = io.ErrShortWrite
		log.Printf("error: %s\n", err)
		return
	}

	if p[len(p)-1] != '\n' {
		_, err = bs.buffer.Write([]byte{'\n'})
		if err != nil {
			log.Printf("error: %s\n", err)
			return
		}
	}

	switch {
	case bs.write_counter >= bs.FlushSize:
		bs.Flush()
	case bs.ch_timer == nil:
		bs.ch_timer = time.After(time.Millisecond * time.Duration(bs.FlushTime))
	}

	return
}

func (bs *Backends) Flush() {
	if bs.buffer == nil {
		return
	}

	p := bs.buffer.Bytes()
	bs.buffer = nil
	bs.ch_timer = nil
	bs.write_counter = 0

	if len(p) == 0 {
		return
	}

	bs.wg.Add(1)
	bs.pool.Submit(func() {
		defer bs.wg.Done()
		var buf bytes.Buffer
		err := Compress(&buf, p)
		if err != nil {
			log.Printf("compress error: %s", err)
			return
		}

		p = buf.Bytes()

		// maybe blocked here, run in another goroutine
		if bs.HttpBackend.IsActive() {
			err = bs.HttpBackend.WriteCompressed(p)
			switch err {
			case nil:
				return
			case ErrBadRequest:
				log.Printf("bad request, drop all data")
				return
			case ErrNotFound:
				log.Printf("bad backend, drop all data")
				return
			default:
				log.Printf("write http error: %s, length: %d\n", bs.HttpBackend.URL, len(p))
			}
		}

		err = bs.fb.Write(p)
		if err != nil {
			log.Printf("write file error: %s\n", err)
		}
		// don't try to run rewrite loop directly.
		// that need a lock.
	})

	return
}

func (bs *Backends) Idle() {
	if !bs.rewriter_running && bs.fb.IsData() {
		bs.rewriter_running = true
		go bs.RewriteLoop()
	}

	// TODO: report counter
}

func (bs *Backends) RewriteLoop() {
	for bs.fb.IsData() {
		if !bs.running {
			return
		}
		if !bs.HttpBackend.IsActive() {
			time.Sleep(time.Millisecond * time.Duration(bs.RewriteInterval))
			continue
		}
		err := bs.Rewrite()
		if err != nil {
			time.Sleep(time.Millisecond * time.Duration(bs.RewriteInterval))
			continue
		}
	}
	bs.rewriter_running = false
}

func (bs *Backends) Rewrite() (err error) {
	p, err := bs.fb.Read()
	if err != nil {
		return
	}
	if p == nil {
		return
	}

	err = bs.HttpBackend.WriteCompressed(p)

	switch err {
	case nil:
	case ErrBadRequest:
		log.Printf("bad request, drop all data")
		err = nil
	case ErrNotFound:
		log.Printf("bad backend, drop all data")
		err = nil
	default:
		log.Printf("rewrite http error: %s, length: %d\n", bs.HttpBackend.URL, len(p))

		err = bs.fb.RollbackMeta()
		if err != nil {
			log.Printf("rollback meta error: %s\n", err)
		}
		return
	}

	err = bs.fb.UpdateMeta()
	if err != nil {
		log.Printf("update meta error: %s\n", err)
		return
	}
	return
}
