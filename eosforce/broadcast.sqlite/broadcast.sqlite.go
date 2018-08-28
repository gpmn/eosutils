package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"sync"
	"time"

	"github.com/go-gorp/gorp"
)

// AccountInfo :
type AccountInfo struct {
	Account  string
	Amount   uint64
	Notified bool
}

func sendMessage(from, to string, advCont string) error {
	cmd := exec.Command("/usr/local/bin/cleos", "--wallet-url", "http://127.0.0.1:8900", "-u", "https://w1.eosforce.cn", "transfer", from, to, "0.0000 EOS", advCont)
	stdoutStderr, err := cmd.CombinedOutput()
	if err != nil {
		log.Printf("sendMessage from %s -> %s failed, err : %v\n", from, to, err)
		return err
	}
	fmt.Printf("%s\n", stdoutStderr)
	return nil
}

func initDB(dbPath string) (dbmap *gorp.DbMap, err error) {
	db, err := sql.Open("sqlite3", dbPath)
	if nil != err {
		log.Printf("main.Init - open sqlite %s failed : %v", dbPath, err)
		return nil, err
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
		return nil, err
	}
	return dbmap, nil
}

func sendRoutine(from, advCont string, accChan chan string, wg *sync.WaitGroup, dbmap *gorp.DbMap) {
	defer wg.Done()
	for {
		select {
		case account := <-accChan:
			var retry int
			for retry = 0; retry < 10; retry++ {
				err := sendMessage(from, account, advCont)
				if nil == err {
					break
				}
				log.Printf("sendMessage %s -> %s failed %d times : %v", from, account, retry, err)
			}
			if retry >= 10 {
				log.Printf("sendMessage failed 10 times, exit program")
				os.Exit(10)
			}
			if _, err := dbmap.Exec("UPDATE AccountInfo SET Notified=1 WHERE Account=?", account); nil != err {
				log.Printf("sendMessage - dbmap.Exec failed to update : %v", err)
				os.Exit(11)
			}
		case <-time.After(10 * time.Second):
			return
		}
	}
}

func main() {
	from := flag.String("from", "", "由谁发送")
	dbPath := flag.String("db", "", "db文件路径")
	valve := flag.Uint64("valve", 100, "只向持仓大于valve的账号发送广告.")
	advCont := flag.String("adv", `免费赚10万EOSC，微信搜索小程序“链圈挖钻助手”创建领地赚EOSC，由链圈超级节点打造DAPP小程序版本。`, "自定义广告词")

	flag.Parse()

	if *dbPath == "" {
		flag.Usage()
		fmt.Fprintf(os.Stderr, "db param missed.\n")
		os.Exit(1)
	}

	if *from == "" {
		flag.Usage()
		fmt.Fprintf(os.Stderr, "from param missed.\n")
		os.Exit(2)
	}

	dbmap, err := initDB(*dbPath)
	if nil != err {
		log.Printf("main - initDB failed : %v", err)
		os.Exit(3)
	}

	var accounts []AccountInfo
	accChan := make(chan string, 40)

	var wg sync.WaitGroup
	wg.Add(40)
	for idx := 0; idx < 40; idx++ {
		go sendRoutine(*from, *advCont, accChan, &wg, dbmap)
	}
	dbmap.Select(&accounts, "SELECT * FROM AccountInfo WHERE Amount>=? AND Notified=0", *valve*10000)
	for _, account := range accounts {
		accChan <- account.Account
	}
	wg.Wait()
}
