package main

import (
	"net"
	"log"
	"bufio"
	"io"
	"strings"
)

func (f *FS) TextCommand(cmd string) (string, error) {
	switch strings.ToUpper(cmd) {
	case "PING":
		return "PONG", nil
	}

	return "commands: HELP PING", nil
}

func (f *FS) SpawnAdminConsole() error {
	listen, err := net.Listen("tcp", "localhost:2000")
	if err != nil {
		return err
	}

	go func() {
		for {
			conn, err := listen.Accept()
			if err != nil {
				log.Println("admin console accept error:", err)
			} else {
				go handleAdmin(f, conn)
			}
		}
	}()

	return nil
}

func handleAdmin(f *FS, conn net.Conn) {
	defer conn.Close()

	log.Println("admin console opened")

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	for {
		line, err := reader.ReadString('\n')
		if err == io.EOF {
			log.Println("admin console closed")
			return
		}
		if err != nil {
			log.Println("admin console read error:", err)
			return
		}
		line = strings.Trim(line, "\n\r\t ")

		resp, err := f.TextCommand(line)
		if err != nil {
			resp = err.Error()
		}

		_, err = writer.WriteString(resp + "\n")
		if err != nil {
			log.Println("admin console write error:", err)
			return
		}
		err = writer.Flush()
		if err != nil {
			log.Println("admin console write flush error:", err)
			return
		}
	}
}
