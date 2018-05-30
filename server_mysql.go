package main

import (
	"encoding/json"
	"fmt"
	"time"
	"bufio"
	"io"
	"regexp"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
)

type CodIngresso struct {
	IdCodIngresso 	int 		`json:"id_cod_ingresso"`
	IdEvento 		int			`json:"id_evento"`
	IdIngresso 		int			`json:"id_ingresso"`
	IdPedido 		int			`json:"id_pedido"`
	CodBar 			string		`json:"cod_bar"`
	DataValidade 	time.Time	`json:"data_validade"`
	St 				string		`json:"st"`
	Fila 			string		`json:"fila"`
	Validado 		time.Time	`json:"validado"`
	Terminal 		int			`json:"terminal"`
	Rede 			int			`json:"rede"`
}

func SocketServer(port int) {

	listen, err := net.Listen("tcp4", ":"+strconv.Itoa(port))
	defer listen.Close()
	if err != nil {
		log.Fatalf("Socket listen port %d failed,%s", port, err)
		os.Exit(1)
	}
	log.Printf("Begin listen port: %d", port)

	for {
		conn, err := listen.Accept()
		if err != nil {
			log.Fatalln(err)
			continue
		}
		go handler(conn)
	}

}

func handler(conn net.Conn) {

	defer conn.Close()

	var (
		buf = make([]byte, 1024)
		r   = bufio.NewReader(conn)
		w   = bufio.NewWriter(conn)
	)

ILOOP:
	for {
		n, err := r.Read(buf)
		data := string(buf[:n])

		switch err {
			case io.EOF:
				break ILOOP
			case nil:
				if isTransportOver(data) {
					break ILOOP
				}
				log.Println("Receive:", data)
				re := regexp.MustCompile("[0-9]+")
				stmtOut, err := db.Prepare("SELECT json_content FROM portaria WHERE cod_bar = ?")
				if err != nil {
					fmt.Println("Db stmt error! "+err.Error())
					continue
				}
				var codIngressoJson string
				err = stmtOut.QueryRow(re.FindString(data)).Scan(&codIngressoJson)
				if err != nil {
					fmt.Println("Db query error! "+err.Error())
					continue
				}
				codIngresso := new(CodIngresso)
				errJson := json.Unmarshal([]byte(codIngressoJson), &codIngresso)
				if errJson != nil {
					fmt.Println("JSON Unmarshal Erro! "+errJson.Error())
					fmt.Println(string(codIngressoJson))
					continue
				}
				codIngresso.Validado = time.Now()
				codIngresso.Terminal = 123
				codIngresso.Rede = 321
				codIngressoJsonB, errJson := json.Marshal(codIngresso)
				if errJson != nil {
					fmt.Println("Marshal json error!")
					continue
				}
				codIngressoJson = string(codIngressoJsonB)
				stmtUpdate, err := db.Prepare("UPDATE portaria SET json_content = ? WHERE cod_bar = ?") // ? = placeholder
				if err != nil {
					fmt.Println("Db stm update Erro! "+err.Error())
					continue
				}
				_, err = stmtUpdate.Exec(codIngressoJson, codIngresso.CodBar)
				if err != nil {
					fmt.Println("Db update Erro! "+err.Error())
					continue
				}

				w.Write([]byte(string(codIngressoJson)))
				w.Flush()
				//break ILOOP
			default:
				log.Fatalf("Receive data failed:%s", err)
				return
		}
	}
}

func isTransportOver(data string) (over bool) {
	over = strings.HasSuffix(data, "\r\n\r\n")
	return
}

var db *sql.DB

func main() {
	var err error
	db, err = sql.Open("mysql", "root:senha123@/portaria")
	if(err != nil) {
		log.Println(err.Error()+"\n")
	}
	defer db.Close()
	if(len(os.Args) > 1) {
		if(os.Args[1] == "populate") {
			PopulateDb()
		}
	}
	
	//CursorDb()
	port := 3333
	SocketServer(port)
}

func PopulateDb() {
	// Prepare statement for inserting data
	stmtIns, err := db.Prepare("INSERT INTO portaria(cod_bar, json_content) VALUES( ?, ? )") // ? = placeholder
	if err != nil {
		panic(err.Error()) // proper error handling instead of panic in your app
	}
	defer stmtIns.Close() // Close the statement when we leave main() / the program terminates
	
	for i := 0; i < 1000000; i++ {
		codIngresso := new(CodIngresso)
		codIngresso.IdCodIngresso = i
		codIngresso.IdEvento = 100
		codIngresso.IdIngresso = 10
		codIngresso.IdPedido = i+10
		codIngresso.CodBar = strconv.Itoa(i)
		codIngresso.DataValidade = time.Now()
		codIngresso.St = "VA"
		codIngresso.Fila = "PRINCIPAL"
		codIngressoJson, err := json.Marshal(codIngresso)
		if err != nil {
			fmt.Println("Marshal json error!")
		}
		_, err = stmtIns.Exec(i, codIngressoJson) // Insert tuples (i, i^2)
		if err != nil {
			panic(err.Error()) // proper error handling instead of panic in your app
		}
		//fmt.Println(string(codIngressoJson));
		// codIngresso.Validado = nil
		// codIngresso.Terminal = nil
		// codIngresso.Rede = nil
	}
}