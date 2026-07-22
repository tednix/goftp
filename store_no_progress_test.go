// Copyright 2026. Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package goftp

import (
	"bufio"
	"bytes"
	"fmt"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// stubSinkFTPServer is a minimal FTP server that accepts STOR data but
// persists nothing: every STOR ends in "451 Failure writing to local file"
// and SIZE always reports 0. This mimics a server whose disk is full,
// which historically made Store resume-loop forever (each attempt pushes
// n > 0 bytes over the wire, so it looked like "progress", but the
// server-side file never grew).
type stubSinkFTPServer struct {
	listener     net.Listener
	storAttempts int32
}

func newStubSinkFTPServer(t *testing.T) *stubSinkFTPServer {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	s := &stubSinkFTPServer{listener: ln}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go s.handleControl(conn)
		}
	}()
	return s
}

func (s *stubSinkFTPServer) addr() string {
	return s.listener.Addr().String()
}

func (s *stubSinkFTPServer) close() {
	s.listener.Close()
}

func (s *stubSinkFTPServer) handleControl(conn net.Conn) {
	defer conn.Close()

	var dataLn net.Listener
	defer func() {
		if dataLn != nil {
			dataLn.Close()
		}
	}()

	fmt.Fprintf(conn, "220 stub ready\r\n")

	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		line := scanner.Text()
		cmd := strings.ToUpper(strings.SplitN(line, " ", 2)[0])

		switch cmd {
		case "USER":
			fmt.Fprintf(conn, "331 need password\r\n")
		case "PASS":
			fmt.Fprintf(conn, "230 logged in\r\n")
		case "FEAT":
			fmt.Fprintf(conn, "211-Features:\r\n REST STREAM\r\n SIZE\r\n211 End\r\n")
		case "TYPE":
			fmt.Fprintf(conn, "200 switching type\r\n")
		case "EPSV":
			var err error
			if dataLn != nil {
				dataLn.Close()
			}
			dataLn, err = net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				fmt.Fprintf(conn, "425 can't open data connection\r\n")
				continue
			}
			port := dataLn.Addr().(*net.TCPAddr).Port
			fmt.Fprintf(conn, "229 Entering Extended Passive Mode (|||%d|)\r\n", port)
		case "STOR":
			if dataLn == nil {
				fmt.Fprintf(conn, "425 use EPSV first\r\n")
				continue
			}
			fmt.Fprintf(conn, "150 ok to send data\r\n")
			dc, err := dataLn.Accept()
			dataLn.Close()
			dataLn = nil
			if err != nil {
				fmt.Fprintf(conn, "425 data connection failed\r\n")
				continue
			}
			// drain the upload, then throw it away
			buf := make([]byte, 4096)
			for {
				if _, err := dc.Read(buf); err != nil {
					break
				}
			}
			dc.Close()
			atomic.AddInt32(&s.storAttempts, 1)
			fmt.Fprintf(conn, "451 Failure writing to local file.\r\n")
		case "SIZE":
			// nothing is ever persisted
			fmt.Fprintf(conn, "213 0\r\n")
		case "REST":
			fmt.Fprintf(conn, "350 restarting\r\n")
		case "QUIT":
			fmt.Fprintf(conn, "221 bye\r\n")
			return
		default:
			fmt.Fprintf(conn, "200 ok\r\n")
		}
	}
}

// stubResumeFTPServer fails the first STOR after persisting only part of
// the upload, then lets the resumed transfer (REST + STOR) complete. Used
// to prove the no-progress bail-out does not break genuine resumption.
type stubResumeFTPServer struct {
	listener     net.Listener
	firstFailLen int
	persisted    []byte
	restOffset   int64
	storAttempts int32
}

func newStubResumeFTPServer(t *testing.T, firstFailLen int) *stubResumeFTPServer {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	s := &stubResumeFTPServer{listener: ln, firstFailLen: firstFailLen}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go s.handleControl(conn)
		}
	}()
	return s
}

func (s *stubResumeFTPServer) handleControl(conn net.Conn) {
	defer conn.Close()

	var dataLn net.Listener
	defer func() {
		if dataLn != nil {
			dataLn.Close()
		}
	}()

	fmt.Fprintf(conn, "220 stub ready\r\n")

	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, " ", 2)
		cmd := strings.ToUpper(parts[0])

		switch cmd {
		case "USER":
			fmt.Fprintf(conn, "331 need password\r\n")
		case "PASS":
			fmt.Fprintf(conn, "230 logged in\r\n")
		case "FEAT":
			fmt.Fprintf(conn, "211-Features:\r\n REST STREAM\r\n SIZE\r\n211 End\r\n")
		case "TYPE":
			fmt.Fprintf(conn, "200 switching type\r\n")
		case "REST":
			fmt.Sscanf(parts[1], "%d", &s.restOffset)
			fmt.Fprintf(conn, "350 restarting\r\n")
		case "EPSV":
			var err error
			if dataLn != nil {
				dataLn.Close()
			}
			dataLn, err = net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				fmt.Fprintf(conn, "425 can't open data connection\r\n")
				continue
			}
			port := dataLn.Addr().(*net.TCPAddr).Port
			fmt.Fprintf(conn, "229 Entering Extended Passive Mode (|||%d|)\r\n", port)
		case "STOR":
			attempt := atomic.AddInt32(&s.storAttempts, 1)
			fmt.Fprintf(conn, "150 ok to send data\r\n")
			dc, err := dataLn.Accept()
			dataLn.Close()
			dataLn = nil
			if err != nil {
				fmt.Fprintf(conn, "425 data connection failed\r\n")
				continue
			}

			s.persisted = s.persisted[:s.restOffset]
			s.restOffset = 0
			buf := make([]byte, 4096)
			failed := false
			for {
				n, err := dc.Read(buf)
				s.persisted = append(s.persisted, buf[:n]...)
				if attempt == 1 && len(s.persisted) >= s.firstFailLen {
					// "disk hiccup": keep what we have, fail the transfer
					s.persisted = s.persisted[:s.firstFailLen]
					failed = true
					break
				}
				if err != nil {
					break
				}
			}
			dc.Close()
			if failed {
				fmt.Fprintf(conn, "451 Failure writing to local file.\r\n")
			} else {
				fmt.Fprintf(conn, "226 transfer complete\r\n")
			}
		case "SIZE":
			fmt.Fprintf(conn, "213 %d\r\n", len(s.persisted))
		case "QUIT":
			fmt.Fprintf(conn, "221 bye\r\n")
			return
		default:
			fmt.Fprintf(conn, "200 ok\r\n")
		}
	}
}

// A seekable source uploaded to a server that persists nothing must fail
// after a bounded number of attempts instead of resume-looping forever.
func TestStoreNoServerProgressBailsOut(t *testing.T) {
	server := newStubSinkFTPServer(t)
	defer server.close()

	config := Config{
		User:     "goftp",
		Password: "rocks",
		Timeout:  5 * time.Second,
	}

	c, err := DialConfig(config, server.addr())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	done := make(chan error, 1)
	go func() {
		// bytes.Reader is an io.Seeker, so Store considers this resumable
		done <- c.Store("stuck.msg", bytes.NewReader([]byte("hello, this will never persist")))
	}()

	select {
	case err = <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("Store still looping after 30s: no-progress bail-out is not working")
	}

	if err == nil {
		t.Fatal("expected Store to fail against a server that persists nothing")
	}

	fe, ok := err.(Error)
	if !ok {
		t.Fatalf("Store error wasn't a goftp.Error: %+v", err)
	}
	if fe.Code() != 451 {
		t.Errorf("expected underlying code 451, got %d (%s)", fe.Code(), fe.Message())
	}
	if !strings.Contains(err.Error(), "no upload progress") {
		t.Errorf("expected a no-progress error, got: %v", err)
	}

	attempts := atomic.LoadInt32(&server.storAttempts)
	if attempts != 2 {
		t.Errorf("expected exactly 2 STOR attempts (initial + one resume probe), got %d", attempts)
	}
}

// Genuine resumption must keep working: when the server persists part of
// the upload before failing, Store resumes from the confirmed offset and
// completes.
func TestStoreResumesAfterPartialPersist(t *testing.T) {
	payload := make([]byte, 64*1024)
	randomBytes(payload)

	server := newStubResumeFTPServer(t, 16*1024)
	defer server.listener.Close()

	config := Config{
		User:     "goftp",
		Password: "rocks",
		Timeout:  5 * time.Second,
	}

	c, err := DialConfig(config, server.listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if err := c.Store("resume.bin", bytes.NewReader(payload)); err != nil {
		t.Fatalf("expected resumed upload to succeed, got: %v", err)
	}

	if attempts := atomic.LoadInt32(&server.storAttempts); attempts != 2 {
		t.Errorf("expected 2 STOR attempts (fail + resume), got %d", attempts)
	}

	if !bytes.Equal(server.persisted, payload) {
		t.Errorf("persisted %d bytes, want %d; content mismatch", len(server.persisted), len(payload))
	}
}
