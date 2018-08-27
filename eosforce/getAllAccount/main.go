package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/go-gorp/gorp"
	_ "github.com/mattn/go-sqlite3"
)

//const _URL = "http://132.232.22.135/v1/chain/get_table_rows"
const (
	_URL    = "https://w2.eosforce.cn/v1/chain/get_table_rows"
	dbparam = "./account.db"
)

var dbmap *gorp.DbMap

func charToVal(ch byte) uint8 {
	if ch >= 'a' && ch <= 'z' {
		return ch - 'a' + 6
	}
	if ch >= '1' && ch <= '5' {
		return ch - '1' + 1
	}
	return 0
}

func strToName(strName string) uint64 {
	name := uint64(0)
	idx := 0
	for ; idx < len(strName) && idx < 12; idx++ {
		name |= uint64(charToVal(strName[idx])&0x1f) << uint(64-5*(idx+1))
	}
	if idx == 12 && len(strName) > 12 {
		name |= uint64(charToVal(strName[12])) & 0xf
	}
	return name
}

// AccountInfo :
type AccountInfo struct {
	Account  string
	Amount   uint64
	Notified bool
}

type respGetAccounts struct {
	More bool `json:"more"`
	Rows []struct {
		Available string `json:"available"`
		Name      string `json:"name"`
	} `json:"rows"`
}

func getAccounts(account string) (respAcc *respGetAccounts, err error) {
	params := fmt.Sprintf(`{"json":true, "scope": "eosio", "code": "eosio", "table": "accounts", "limit": 500, "lower_bound":"%d"}`, strToName(account))

	resp, err := http.Post(_URL,
		"application/json",
		strings.NewReader(params))

	if nil != err {
		log.Printf("http.PostForm failed : %v", err)
		return nil, err
	}

	buf, err := ioutil.ReadAll(resp.Body)
	if nil != err {
		log.Printf("ioutil.ReadAll failed : %v", err)
		return nil, err
	}

	var result respGetAccounts
	if err = json.Unmarshal(buf, &result); nil != err {
		log.Printf("json.Unmarshall failed : %v", err)
		fmt.Printf("\n%s\n", string(buf))
		return nil, err
	}
	respAcc = &result
	return respAcc, nil
}

func getAllAccount() (err error) {
	account := ""

	for {
		retry := 0
		var respAcc *respGetAccounts
		for ; retry < 10; retry++ {
			respAcc, err = getAccounts(account)
			if nil != err {
				log.Printf("getAllAccount - getAccounts failed : %v", err)
				continue
			}
			break
		}
		if retry >= 10 {
			log.Printf("getAllAccount - getAccounts failed too many times, abort!")
			return fmt.Errorf("getAllAccount failed too many times")
		}

		for idx := range respAcc.Rows {
			info := &respAcc.Rows[idx]
			avls := strings.Split(info.Available, " ")
			lots, err := strconv.ParseFloat(avls[0], 64)
			if err != nil {
				log.Printf("getAllAccount - strconv.ParseFloat(%s, 64) failed : %v", avls[0], err)
				return err
			}
			amount := uint64(lots * 10000)
			log.Printf("%-12s [%8d EOS] (%s)", info.Name, amount, info.Available)
			if _, err = dbmap.Exec("INSERT OR REPLACE INTO AccountInfo (Account, Amount, Notified) VALUES (?,?,?)",
				info.Name, amount, false); nil != err {
				log.Printf("getAllAccount - dbmap.Exec failed : %v", err)

			}
		}
		if !respAcc.More {
			log.Printf("getAllAccount - done")
			return nil
		}
		account = respAcc.Rows[len(respAcc.Rows)-1].Name
		log.Printf("getAllAccount - from %s", account)
	}
}

func initDB() (err error) {
	db, err := sql.Open("sqlite3", dbparam)
	if nil != err {
		log.Printf("AccountManager.Init - open sqlite failed : %v", err)
		return err
	}
	dbmap = &gorp.DbMap{Db: db, Dialect: gorp.SqliteDialect{}}
	if _, err = dbmap.Exec("PRAGMA synchronous=NORMAL"); nil != err {
		log.Printf("AccountManager.Init - 'PRAGMA synchronous=NORMAL' failed : %v", err)
	}
	if _, err = dbmap.Exec("PRAGMA page_size=8192"); nil != err {
		log.Printf("AccountManager.Init - 'PRAGMA page_size=8192' failed : %v", err)
	}
	if _, err = dbmap.Exec("PRAGMA cache_size=204800"); nil != err {
		log.Printf("AccountManager.Init - 'PRAGMA cache_size=204800' failed : %v", err)
	}
	if _, err = dbmap.Exec("PRAGMA temp_store=MEMORY"); nil != err {
		log.Printf("AccountManager.Init - 'PRAGMA temp_store=MEMORY' failed : %v", err)
	}

	dbmap.AddTableWithName(AccountInfo{}, "AccountInfo").SetKeys(false, "Account")
	if err = dbmap.CreateTablesIfNotExists(); nil != err {
		log.Printf("AccountManager.Init - CreateTablesIfNotExists failed : %v", err)
	}
	return err
}

func main() {
	var err error
	if err = initDB(); nil != err {
		log.Printf("main - initDB failed : %v", err)
		os.Exit(1)
	}

	if err = getAllAccount(); nil != err {
		log.Printf("main - getAllAccount failed : %v", err)
		os.Exit(2)
	}

	log.Printf("main - done")
	os.Exit(0)
}
