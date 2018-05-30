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
	"github.com/boltdb/bolt"
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
				tx, err := db.Begin(true)
				if err != nil {
					log.Println("Db Begin error! "+err.Error())
					continue
				}
				codIngressoJson := tx.Bucket([]byte("portaria")).Get([]byte(re.FindString(data)))
				if err != nil {
					log.Println("Db Get error! "+err.Error())
					tx.Rollback()
					continue
				}
				codIngresso := new(CodIngresso)
				errJson := json.Unmarshal(codIngressoJson, &codIngresso)
				if errJson != nil {
					log.Println("JSON Unmarshal Erro! "+errJson.Error())
					log.Println(string(codIngressoJson))
					log.Println("Data: "+re.FindString(data))
					tx.Rollback()
					continue
				}
				codIngresso.Validado = time.Now()
				codIngresso.Terminal = 123
				codIngresso.Rede = 321
				codIngressoJson, errJson = json.Marshal(codIngresso)
				if errJson != nil {
					log.Println("Marshal json error!")
					continue
				}
				b := tx.Bucket([]byte("portaria"))
				errDb := b.Put([]byte(codIngresso.CodBar), codIngressoJson)
				if errDb != nil {
					log.Println("Db Put error!")
					tx.Rollback()
					continue
				}
				if errDb := tx.Commit(); errDb != nil {
					log.Println("Db Commit error!")
					tx.Rollback()
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

var db *bolt.DB

func main() {
	var err error
	db, err = bolt.Open("bolt.db", 0755, nil)
	if(err != nil) {
		fmt.Println(err.Error()+"\n")
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

func PopulateDb() (error) {
	tx, err := db.Begin(true)
	if err != nil {
		fmt.Errorf("begin transaction: %s", err)
		return err
	}
	_, err2 := tx.CreateBucketIfNotExists([]byte("portaria"))
	if err2 != nil {
		fmt.Errorf("create bucket: %s", err2)
		return err2
	}

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
		b := tx.Bucket([]byte("portaria"))
		err = b.Put([]byte(strconv.Itoa(i)), codIngressoJson)
		if err != nil {
			fmt.Println("Db Put error!")
		}
		//fmt.Println(string(codIngressoJson));
		// codIngresso.Validado = nil
		// codIngresso.Terminal = nil
		// codIngresso.Rede = nil
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	fmt.Println("Db populate!!!")
	return nil
}

func CursorDb() {
	tx, err := db.Begin(false)
	if err != nil {
		log.Fatal(err)
	}

	c := tx.Bucket([]byte("portaria")).Cursor()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		fmt.Printf("Key: %s value: %s\n", k, v)
	}

	if err := tx.Rollback(); err != nil {
		log.Fatal(err)
	}

	if err := db.Close(); err != nil {
		log.Fatal(err)
	}
}