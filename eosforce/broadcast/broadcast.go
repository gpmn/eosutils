package main

import (
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"strconv"
	"time"
)

type sendHistoryItem struct {
	Account string
	Sent    bool
}

type sendHistory map[string]sendHistoryItem

func (his *sendHistory) load(path string) error {
	buf, err := ioutil.ReadFile(path)
	if nil != err {
		fmt.Fprintf(os.Stderr, "load history file %s failed : %v\n", path, err)
		return err
	}
	err = json.Unmarshal(buf, his)
	if nil != err {
		fmt.Fprintf(os.Stderr, "json.unmarshal failed : %v\n", err)
		return err
	}
	return nil
}

func (his *sendHistory) save(path string) error {
	buf, err := json.Marshal(*his)
	if nil != err {
		fmt.Fprintf(os.Stderr, "json.Marshal failed : %s\n", err.Error())
		return err
	}
	return ioutil.WriteFile(path, buf, 0666)
}

const advertise = `EosForce is the first DPOS chain based on EOS that voter can share revenue with BP,It's far more fair than original one.It comply with genesis snapshot.So we are waiting for U come back eargly @ eosforce.io,and please vote imlianquan eosshuimu miduoduo.`

func sendMessage(account string, adv string) error {
	cmd := exec.Command("/usr/local/bin/cleos", "--wallet-url", "http://127.0.0.1:8900", "-u", "http://mainnet.eoscalgary.io", "transfer", "guytiobzguge", account, "0.0001 EOS", adv)
	stdoutStderr, err := cmd.CombinedOutput()
	if err != nil {
		log.Printf("sendMessage to %s failed, err : %v\n", account, err)
		return err
	}
	fmt.Printf("%s\n", stdoutStderr)
	return nil
}

func main() {
	hisPath := flag.String("his", "", "历史记录路径.")
	snapPath := flag.String("snap", "", "创世快照文件路径.")
	valve := flag.Float64("valve", 100.0, "只向持仓大于valve的账号发送广告.")
	adv := flag.String("adv", "", "自定义广告词")
	flag.Parse()

	if *hisPath == "" {
		flag.Usage()
		fmt.Fprintf(os.Stderr, "snap param missed.\n")
		os.Exit(1)
	}
	history := make(sendHistory)
	if err := history.load(*hisPath); nil != err {
		fmt.Fprintf(os.Stderr, "load history failed : %v, used default.\n", err)
	}

	if *snapPath == "" {
		flag.Usage()
		fmt.Fprintf(os.Stderr, "snap param missed.\n")
		os.Exit(2)
	}

	if *adv == "" {
		*adv = advertise
	}

	file, err := os.Open(*snapPath)
	if nil != err {
		fmt.Fprintf(os.Stderr, "open file %s failed : %v.\n", *snapPath, err)
		os.Exit(3)
	}

	reader := csv.NewReader(file)
	defer history.save(*hisPath)
	cnt := 0
	for {
		line, err := reader.Read()
		if err == io.EOF {
			os.Exit(0)
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "reader.Read failed : %s.\n", err.Error())
			os.Exit(4)
		}
		log.Printf("%v", line)
		account := line[1]
		quantity, err := strconv.ParseFloat(line[3], 64)
		if nil != err {
			fmt.Fprintf(os.Stderr, "strconv.ParseFloat(%s,64) failed : %s.\n", line[3], err.Error())
			os.Exit(5)
		}
		if quantity < *valve {
			continue
		}
		if his, ok := history[account]; ok || his.Sent {
			continue
		}
		for {
			if err = sendMessage(account, *adv); nil == err {
				cnt++
				history[account] = sendHistoryItem{
					Account: account,
					Sent:    true,
				}
				if cnt%10 == 0 {
					history.save(*hisPath)
				}
				break
			}
			time.Sleep(5 * time.Second)
		}
	}
}
