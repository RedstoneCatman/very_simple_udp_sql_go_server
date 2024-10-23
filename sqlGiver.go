package main

import (
	"database/sql"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"

	_ "github.com/mattn/go-sqlite3"
)

func SQLCommand(data *sql.DB, command string) error {
	tr, err := data.Begin()

	if err != nil {
		return err
	}

	_, err = tr.Exec(command)
	if err != nil {
		tr.Rollback()
		return err
	}

	err = tr.Commit()
	if err != nil {
		return err
	}

	return nil
}

func Giver(text string, conn *net.UDPConn, asker net.Addr) {
	_, err := conn.WriteTo([]byte(text), asker)
	if err != nil {
		log.Panic(err)
	}
}

func main() {
	if len(os.Args) < 3 {
		log.Panic("you need to run it with arguments in the form of a port and the name of the database date \n./sqlgiver some_port some.db")
	}
	port, FileName := os.Args[1], os.Args[2]
	data, err := sql.Open("sqlite3", FileName)

	if err != nil {
		log.Panic(err)
	}

	defer data.Close()

	SQLCommand(data, "CREATE TABLE IF NOT EXISTS box (id PRIMARY KEY, cou);")

	SQLCommand(data, "INSERT OR IGNORE INTO box (id, cou) VALUES (1, 0);")

	fmt.Println(fmt.Sprintf("0.0.0.0:%v", port))
	addr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("0.0.0.0:%v", port))
	if err != nil {
		log.Panic(err)
	}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		log.Panic(err)
	}
	defer conn.Close()

	log.Printf("sqlGiver start as %v", addr)

	for {
		ByteString := make([]byte, 65536)
		var asker net.Addr
		for {
			answ, AskAdr, err := conn.ReadFrom(ByteString)
			if err != nil {
				log.Panic(err)
			}
			if answ > 0 {
				ByteString, asker = ByteString[:answ], AskAdr
				log.Printf("Me <- %v (%v)", string(ByteString), asker)
				break
			}
		}
		command := ""
		_, err := fmt.Sscanf(string(ByteString), "%v", &command)

		if err != nil {
			Giver("error", conn, asker)
			continue
		}
		if command == "get" {
			tr, err := data.Begin()
			if err != nil {
				log.Panic(err)
			}
			var counter int
			getCountSQL := "SELECT cou FROM box WHERE id = 1;"
			err = tr.QueryRow(getCountSQL).Scan(&counter)
			if err != nil {
				tr.Rollback()
				log.Panic(err)
			}
			tr.Commit()
			counterStr := strconv.Itoa(counter)
			Giver(counterStr, conn, asker)
			continue
		}

		if command == "inc" {
			err = SQLCommand(data, "UPDATE box SET cou = cou + 1 WHERE id = 1;")
			if err != nil {
				Giver("fail", conn, asker)
				fmt.Println(err)
			} else {
				Giver("ok", conn, asker)
			}
			continue
		}
		Giver("error", conn, asker)

	}
}
