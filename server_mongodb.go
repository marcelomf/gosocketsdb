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
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
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

type Portaria struct {
	ID      	bson.ObjectId 	`json:"id" bson:"_id,omitempty"`
	CodBar  	string       	`json:"cod_bar" bson:"cod_bar"`
	JsonContent string        	`json:"json_content" bson:"json_content"`
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

				c := db.C("portaria")
				var portaria Portaria
				err := c.Find(bson.M{"cod_bar": re.FindString(data)}).One(&portaria)
				if err != nil {
					fmt.Println("Db Find error! "+err.Error())
					continue
				}
				codIngressoJson := portaria.JsonContent
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
				codIngressoJsonb, errJson := json.Marshal(codIngresso)
				if errJson != nil {
					fmt.Println("Marshal json error!")
					continue
				}
				portaria.JsonContent = string(codIngressoJsonb)
				err = c.Update(bson.M{"cod_bar": codIngresso.CodBar}, &portaria)

				w.Write([]byte(string(codIngressoJsonb)))
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

var db *mgo.Database

func main() {
	session, err := mgo.Dial("localhost")
	if err != nil {
		log.Fatal(err)
	}
	db = session.DB("portaria")

	defer session.Close()
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
		codIngressoJsonb, err := json.Marshal(codIngresso)
		if err != nil {
			fmt.Println("Marshal json error!")
		}
		portaria := new(Portaria)
		portaria.CodBar = codIngresso.CodBar
		portaria.JsonContent = string(codIngressoJsonb)
		c := db.C("portaria")
		err = c.Insert(portaria)
		if err != nil {
			fmt.Println("Db Insert error!")
		}
		//fmt.Println(string(codIngressoJson));
		// codIngresso.Validado = nil
		// codIngresso.Terminal = nil
		// codIngresso.Rede = nil
	}
}