package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-gorp/gorp"
	_ "github.com/mattn/go-sqlite3"
)

// VoteInfo :
type VoteInfo struct {
	SeqNum                uint64    // 排序用
	BlockNum              uint64    // 用作查询终止条件
	Quantity              uint64    // 投票额度
	BlockTime             time.Time // 用作查询终止条件
	Voter, BPName, Symbol string    // 投票人、BP、投票品种
}

// RespDataDetail :
//
// "data": "0000000080eeac7bc0a6db8e61384dcb01000000010002e0d3aed0eb7bf73a6c8fed28781c348dfa78ed985a4996cff4631650ec2410d10100000001000000010002e0d3aed0eb7bf73a6c8fed28781c348dfa78ed985a4996cff4631650ec2410d101000000"
// OR :
// "data": {
//            "voter": "hezdonzshege",
//            "bpname": "jiqix"
//          },
type RespDataDetail interface{}

// ResponseGetActions :
type ResponseGetActions struct {
	Actions []struct {
		AccountActionSeq int `json:"account_action_seq"`
		ActionTrace      struct {
			Act struct {
				Account       string `json:"account"`
				Authorization []struct {
					Actor      string `json:"actor"`
					Permission string `json:"permission"`
				} `json:"authorization"`
				Data    RespDataDetail
				HexData string `json:"hex_data"`
				Name    string `json:"name"`
			} `json:"act"`
			Console      string        `json:"console"`
			CPUUsage     int           `json:"cpu_usage"`
			Elapsed      int           `json:"elapsed"`
			InlineTraces []interface{} `json:"inline_traces"`
			Receipt      struct {
				AbiSequence    int             `json:"abi_sequence"`
				ActDigest      string          `json:"act_digest"`
				AuthSequence   [][]interface{} `json:"auth_sequence"`
				CodeSequence   int             `json:"code_sequence"`
				GlobalSequence int             `json:"global_sequence"`
				Receiver       string          `json:"receiver"`
				RecvSequence   int             `json:"recv_sequence"`
			} `json:"receipt"`
			TotalCPUUsage int    `json:"total_cpu_usage"`
			TrxID         string `json:"trx_id"`
		} `json:"action_trace"`
		BlockNum        uint64 `json:"block_num"`
		BlockTime       string `json:"block_time"`
		GlobalActionSeq int    `json:"global_action_seq"`
	} `json:"actions"`
	LastIrreversibleBlock int `json:"last_irreversible_block"`
}

// curl --request POST \
//   --url 'https://w1.eosforce.cn/v1/history/get_actions' \
//   --header 'Content-Type: application/json' \
//   --data '{
//   "account_name": "jiqix",
//   "pos": "0",
//   "offset": "0"
// }'

// VoteArray :
type VoteArray []*VoteInfo

// Len is part of sort.Interface.
func (va VoteArray) Len() int {
	return len(va)
}

// Swap is part of sort.Interface.
func (va VoteArray) Swap(i, j int) {
	va[i], va[j] = va[j], va[i]
}

// Less :
func (va VoteArray) Less(i, j int) bool {
	return va[i].BlockTime.Before(va[j].BlockTime)
}

var dbmap *gorp.DbMap

func main() {
	log.SetFlags(log.Ltime | log.Ldate | log.Lshortfile)
	beginNum := flag.Uint64("begin_num", 0, "回溯到哪个block number. 0表示回溯到最早的一个节点。")
	fromPos := flag.Uint64("from_pos", 0, "从哪个位置开始回溯，不是block number，0表示从最新的节点开始回溯。参见pos : https://documenter.getpostman.com/view/4394576/RWEnobze#4cc4d825-2bad-4677-a7f3-d8971e7cb89a")
	beginStr := flag.String("begin_time", "2018-06-01 00:00:00", "只统计在begin_time之后的Block。")
	endStr := flag.String("end_time", "2200-01-01 00:00:00", "只统计在不晚于end_time的Block")
	server := flag.String("server", "w1.eosforce.cn", "接入点，可填 w1.eosforce.cn, w2.eosforce.cn, w3.eosforce.cn")
	bp := flag.String("bp", "", "查询的BP名字，不能为空.")
	db := flag.String("db", "", "sqlite3文件名，建议加上.csv后缀.为空字符串则不保存。")
	ondup := flag.String("ondup", "query", "如果db已有重复SeqNum记录，是继续、还是退出、还是询问,即 goon/term/query 三个选项。")
	flag.Parse()

	if *bp == "" {
		flag.Usage()
		log.Printf("missing bp param")
		return
	}

	tmBegin, err := time.Parse("2006-01-02 15:04:05", *beginStr)
	if nil != err {
		flag.Usage()
		log.Printf("begin_time '%s' invalid, should be like '2006-01-02 15:04:05'", *beginStr)
		return
	}

	tmEnd, err := time.Parse("2006-01-02 15:04:05", *endStr)
	if nil != err {
		flag.Usage()
		log.Printf("end_time '%s' invalid, should be like '2006-01-02 15:04:05'", *endStr)
		return
	}

	voteInfos := make(map[string]*VoteInfo) // voter -> block producer

	if *db != "" {
		if dbmap, err = initDB(*db); nil != err {
			log.Printf("initDB(%s) failed : %v", *db, err)
			return
		}
	}

	ondupSelection := byte(0)

	offset := uint64(100)
	//var respActions ResponseGetActions
outloop:
	for pos := *fromPos; ; pos += offset {
		log.Printf("pos %d, offset %d", pos, offset)
		params := fmt.Sprintf(`{"account_name": "%s", "pos": "%d", "offset": "%d"}`, *bp, pos, offset)

		URL := fmt.Sprintf("https://%s/v1/history/get_actions", *server)
		resp, err := http.Post(URL,
			"application/json",
			strings.NewReader(params))
		if nil != err {
			log.Printf("http.PostForm failed : %v", err)
			return
		}

		buf, err := ioutil.ReadAll(resp.Body)
		if nil != err {
			log.Printf("ioutil.ReadAll failed : %v", err)
			return
		}
		var tmpActions ResponseGetActions
		if err = json.Unmarshal(buf, &tmpActions); nil != err {
			log.Printf("json.Unmarshall failed : %v", err)
			fmt.Printf("\n%s\n", string(buf))
			return
		}
		// 不限制的话，就全部读完
		if *beginNum == 0 && len(tmpActions.Actions) == 0 {
			log.Printf("no more actions")
			break
		}

		for idx := 0; idx < len(tmpActions.Actions); idx++ {
			act := &tmpActions.Actions[idx]
			// 限制的话，读到指定位置
			if *beginNum > 0 && act.BlockNum < *beginNum {
				log.Printf("act.BlockNum:%d less than begin_num:%d, terminate backtrace", act.BlockNum, *beginNum)
				break outloop
			}
			blockTime, err := time.Parse("2006-01-02T15:04:05", act.BlockTime)
			if nil != err {
				log.Printf("time.Parse(%s, %s) failed : %v", "2006-01-02T15:04:05", act.BlockTime, err)
				continue
			}

			if tmBegin.After(blockTime) {
				log.Printf("block time '%s', before limit begin time '%s', terminate.",
					blockTime.Format("2006-01-02 15:04:05"), tmBegin.Format("2006-01-02 15:04:05"))
				break outloop
			}

			if tmEnd.Before(blockTime) {
				log.Printf("block time '%s', after end time '%s', ignore.",
					blockTime.Format("2006-01-02 15:04:05"), tmEnd.Format("2006-01-02 15:04:05"))
				continue
			}

			name := act.ActionTrace.Act.Name
			if name == "newaccount" || name == "claim" || name == "unfreeze" || name == "transfer" || name == "updatebp" {
				continue
			}
			if name != "vote" {
				log.Printf("WARNING :: unknown action %s", name)
				continue
			}

			info := act.ActionTrace.Act.Data.((map[string]interface{}))
			//log.Printf("tmpActions[%d] %s -> %v", idx, act.ActionTrace.Act.Name, info)
			stake := info["stake"].(string)
			ss := strings.Split(stake, " ")
			if ss[1] != "EOS" {
				log.Printf("Symbol '%s' is not 'EOS'", ss[1])
				continue
			}
			quant, err := strconv.ParseFloat(ss[0], 64)
			if nil != err {
				log.Printf("strconv.ParseFloat(%s, 64) failed : %v", ss[0], err)
				continue
			}
			voterName := info["voter"].(string)
			if voterName == "" {
				panic("no voterName")
			}

			infoPtr := &VoteInfo{
				SeqNum:    uint64(act.GlobalActionSeq),
				BlockNum:  act.BlockNum,
				Quantity:  uint64(quant),
				BlockTime: blockTime,
				Voter:     voterName,
				BPName:    info["bpname"].(string),
				Symbol:    ss[1],
			}
			if dbmap != nil {
				cnt, _ := dbmap.SelectInt("SELECT count(*) FROM VoteInfo WHERE SeqNum=?", infoPtr.SeqNum)
				if cnt > 0 {
					switch *ondup {
					case "query":
						if ondupSelection == 'g' {
							break
						}
						fmt.Printf(`found duplicated vote @ SeqNum %d. press o/i/t/g to continue.
overwrite this time(o)/ignore this time(i)/term for all(t)/goon for all(g)
`, infoPtr.BlockNum)
						if _, err = fmt.Scanf("%c", &ondupSelection); nil != err {
							log.Printf("fmt.Scanf failed : %v", err)
							return
						}

						if ondupSelection == 't' {
							log.Printf("terminate by dup reaction")
							break outloop
						}
						if ondupSelection == 'i' {
							break // continue
						}
					case "goon":
						break
					case "term":
						break outloop
					default:
					}
				}
			}
			if err = saveVoteInfo(dbmap, infoPtr); nil != err {
				log.Printf("saveVoteInfo failed : %v", err)
				return
			}

			if old, ok := voteInfos[voterName]; ok {
				if old.SeqNum > uint64(act.GlobalActionSeq) { // 以后面的为准
					continue
				}
			}

			voteInfos[voterName] = infoPtr
		}
	}

	log.Printf("%-12s -> %-12s  %-12s EOS @ %s", "VOTER", "BP", "QUANTITY", "LAST VOTE DATE")

	var voteList VoteArray
	for _, v := range voteInfos {
		voteList = append(voteList, v)
	}

	sort.Sort(voteList)
	for _, v := range voteList {
		log.Printf("%-12s -> %-12s  %-12d EOS @ %s", v.Voter, v.BPName, v.Quantity, v.BlockTime.Format("2006-01-02 15:04:05"))
	}

}

func initDB(path string) (*gorp.DbMap, error) {
	db, err := sql.Open("sqlite3", path)
	if nil != err {
		log.Printf("initDB - open %s failed : %v", path, err)
		return nil, err
	}
	dbmap := &gorp.DbMap{Db: db, Dialect: gorp.SqliteDialect{}}
	if _, err = dbmap.Exec("PRAGMA synchronous=NORMAL"); nil != err {
		log.Printf("initDB - 'PRAGMA synchronous=NORMAL' failed : %v", err)
	}
	if _, err = dbmap.Exec("PRAGMA page_size=8192"); nil != err {
		log.Printf("initDB - 'PRAGMA page_size=8192' failed : %v", err)
	}
	if _, err = dbmap.Exec("PRAGMA cache_size=204800"); nil != err {
		log.Printf("initDB - 'PRAGMA cache_size=204800' failed : %v", err)
	}
	if _, err = dbmap.Exec("PRAGMA temp_store=MEMORY"); nil != err {
		log.Printf("initDB - 'PRAGMA temp_store=MEMORY' failed : %v", err)
	}

	dbmap.AddTableWithName(VoteInfo{}, "VoteInfo").SetKeys(false, "SeqNum")

	if err = dbmap.CreateTablesIfNotExists(); nil != err {
		log.Printf("initDB - CreateTablesIfNotExists failed : %v", err)
	}
	return dbmap, err
}

func saveVoteInfo(dbmap *gorp.DbMap, info *VoteInfo) error {
	if dbmap == nil {
		return nil
	}
	sql := "INSERT OR REPLACE INTO VoteInfo (SeqNum,BlockNum,Quantity,BlockTime,Voter,BPName,Symbol) VALUES (?,?,?,?,?,?,?)"
	_, err := dbmap.Exec(sql, info.SeqNum, info.BlockNum, info.Quantity, info.BlockTime, info.Voter, info.BPName, info.Symbol)
	return err
}
