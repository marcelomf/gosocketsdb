package main

import (
    "log"
	"net"
	"strconv"
	"strings"
	"fmt"
	"os"
)

const (
	StopCharacter = "\r\n\r\n"
)

func SocketClient(ip string, port int, querys int) {
	addr := strings.Join([]string{ip, strconv.Itoa(port)}, ":")
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		log.Fatalln(err)
	}
	defer conn.Close()


	for i := 1; i <= querys; i++ {
		totalId += 1;
		conn.Write([]byte(strconv.Itoa(totalId)))
		//log.Printf("Send: %d", totalId)
		buff := make([]byte, 1024)
		n, _ := conn.Read(buff)
		log.Printf("Receive: %s", buff[:n])
	}
	conn.Write([]byte(StopCharacter))
}

var totalId int = 0

func main() {
	var (
		ip   = "127.0.0.1"
		port = 3333
	)

	ip = os.Args[1]

	concurrency, err := strconv.Atoi(os.Args[2])
    if err != nil {
        // handle error
        fmt.Println(err)
        os.Exit(2)
	}
	querys, err := strconv.Atoi(os.Args[3])
	if err != nil {
        // handle error
        fmt.Println(err)
        os.Exit(2)
	}

	for i := 1; i <= concurrency; i++ {
		go SocketClient(ip, port, querys)
	}

	fmt.Scanln()
    fmt.Println("done")
}