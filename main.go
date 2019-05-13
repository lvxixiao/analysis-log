package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis"
	"github.com/sirupsen/logrus"
)

const handleDig = "/dig?"
const endDig = " HTTP/"
const handleMovie = "/movie/"
const handleList = "/list/"

type cmdParams struct {
	logFilePath string
	routineNum  int
}

type urlData struct {
	data digData
	node urlNode
	uid  string
}

type digData struct {
	time  string
	url   string
	refer string
	ua    string
}

type urlNode struct {
	unType string // 详情页、列表页或者首页
	unRid  int    // Resouce ID 资源ID
	unURL  string // 当前这个页面的url
	unTime string // 当前访问这个页面的时间
}

type storageBlock struct {
	counterType string
	unode       urlNode
}

var log = logrus.New()
var client *redis.Client
var f = make(chan struct{}) // 用于日志读取完成的通知

func init() {
	log.Out = os.Stdout
	log.SetLevel(logrus.DebugLevel)
	client = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})
}

func main() {
	// 关闭redis链接
	defer client.Close()
	// 获取参数
	logFilePath := flag.String("logFilePath", "./log.txt", "log file path")
	routineNum := flag.Int("rutineNume", 3, "consumer number by gorutine")
	l := flag.String("l", "./temp/log.txt", "this program runtime log path")
	flag.Parse()
	params := cmdParams{*logFilePath, *routineNum}

	// 打日志
	logFd, err := os.OpenFile(*l, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err == nil {
		log.Out = logFd
		defer logFd.Close()
	}
	if err != nil {
		fmt.Println(err)
	}
	log.Infoln("Exec start.")
	log.Infof("Params: logFilePath=%s, routineNum=%d", params.logFilePath, params.routineNum)

	// 初始化channel
	var logChannel = make(chan string, params.routineNum*3)
	var pvChannel = make(chan urlData, params.routineNum)
	var uvChannel = make(chan urlData, params.routineNum)
	var storageChannel = make(chan storageBlock, params.routineNum)

	// 日志消费者
	go readFileLineByLine(params, logChannel)

	// 创建一组日志处理
	for i := 0; i < params.routineNum; i++ {
		go logConsumer(logChannel, pvChannel, uvChannel)
	}

	// 创建 pv uv统计器
	go pvCounter(pvChannel, storageChannel)
	go uvCounter(uvChannel, storageChannel)

	// 创建 存储器
	go dataStorage(storageChannel)

	// 等待其他gorutine处理完毕
	<-f
}

// readFileLineByLine读取日志
func readFileLineByLine(params cmdParams, logChannel chan<- string) error {
	// 打开日志文件
	fd, err := os.Open(params.logFilePath)
	if err != nil {
		log.Warningf("readFileLineByLine can't open the file %s", params.logFilePath)
		return err
	}
	defer fd.Close()
	// 使用bufio读取
	bufferReader := bufio.NewReader(fd)
	count := 0
	for {
		// 日志格式，每一行结尾为换行符
		line, err := bufferReader.ReadString('\n')
		logChannel <- line
		// 日志记录读取的行数
		count++
		if count%100 == 0 {
			log.Infof("readFileLineByLine line:%d", count)
		}

		if err != nil {
			if err == io.EOF {
				log.Infof("%s read complete", params.logFilePath)
				time.Sleep(2 * time.Second) // 等待其他gorutine完成
				f <- struct{}{}             // 通知主函数，读取结束
				return nil
			}
			log.Warningf("%s can't read", params.logFilePath)
		}
	}
}

// logConsumer将分析后的数据放入chan
func logConsumer(logChannel <-chan string, pvChannel, uvChannel chan<- urlData) {
	// 切割日志,获取数据，封装到urlData
	for logData := range logChannel {
		data := cutLogFetchData(logData)
		// 放入channel
		pvChannel <- data
		uvChannel <- data
	}
}

// cutLogFetchData对读取的日志行进行分析
func cutLogFetchData(logStr string) urlData {
	start := strings.Index(logStr, handleDig)
	start += len(handleDig)
	end := strings.Index(logStr, endDig)
	if start == -1 || end == -1 { // 该行不符合预定的规则
		return urlData{}
	}
	d := logStr[start:end]

	// url.Parse方法需要一个url
	urlInfo, err := url.Parse("http://localhost/?" + d)
	if err != nil {
		return urlData{}
	}
	data := urlInfo.Query()
	// 获得urlNode
	uNode := formatURL(data.Get("url"), data.Get("time"))
	return urlData{
		digData{
			data.Get("time"),
			data.Get("url"),
			data.Get("refer"),
			data.Get("ua"),
		},
		uNode,
		data.Get("uid"),
	}
}

// 格式化url
func formatURL(url, time string) urlNode {
	pos1 := strings.Index(url, handleMovie)
	if pos1 != -1 { // 详情页
		pos1 += len(handleMovie)
		rid, _ := strconv.Atoi(url[pos1:])
		return urlNode{
			"movie",
			rid,
			url,
			time,
		}
	} else {
		pos1 = strings.Index(url, handleList)
		if pos1 != -1 { // 列表页
			pos1 += len(handleList)
			rid, _ := strconv.Atoi(url[pos1:])
			return urlNode{
				"movie",
				rid,
				url,
				time,
			}
		}
		return urlNode{"home", 1, url, time} // 主页
	}
}

// pvCounter pv统计
func pvCounter(pvChannel <-chan urlData, storageChannel chan<- storageBlock) {
	// 遍历urlData，创建storageBlock后放入storageChannel
	for data := range pvChannel {
		storage := storageBlock{"PV", data.node}
		storageChannel <- storage
	}
}

// uvCounter uv统计
func uvCounter(uvChannel chan urlData, storageChannel chan storageBlock) {
	// 遍历urlData
	for data := range uvChannel {
		// 借助redis的hyperloglog去重
		hyperLogLogKey := "uv_hpll_" + getTime(data.node.unTime, "day")
		ret, err := client.PFAdd(hyperLogLogKey).Result()
		if err != nil {
			log.Warningln("uvCounter check redis hyperloglog failed ,", err)
		}
		if ret != 1 {
			continue
		}
		storage := storageBlock{"uv", data.node}
		storageChannel <- storage
	}
}

// getTime将时间进行格式化
func getTime(logTime, timeType string) string {
	var item string
	switch timeType {
	case "day":
		item = "2006-01-02"
	case "hour":
		item = "2006-01-02 15"
	case "min":
		item = "2006-01-02 15:04"
	default:
		item = "2006-01-02"
	}
	t, _ := time.Parse(logTime, time.Now().Format(item))
	return strconv.FormatInt(t.Unix(), 10)
}

// dataStorage将统计结果存入redis
func dataStorage(storageChannel <-chan storageBlock) {
	// 遍历storageBlock
	for block := range storageChannel {
		log.Infoln(block.unode)
		prefix := block.counterType + "_"
		// 维度: 天-小时-分
		// 层级: 定级-大分类-小分类-终极页面
		setKeys := []string{
			prefix + "day_" + getTime(block.unode.unTime, "day"),
			prefix + "hour_" + getTime(block.unode.unTime, "hour"),
			prefix + "min_" + getTime(block.unode.unTime, "min"),
			prefix + block.unode.unType + "_day_" + getTime(block.unode.unTime, "day"),
			prefix + block.unode.unType + "_hour_" + getTime(block.unode.unTime, "hour"),
			prefix + block.unode.unType + "_min_" + getTime(block.unode.unTime, "min"),
		}

		rowID := block.unode.unRid // 资源id

		// 写入redis，采用sorted set
		for _, key := range setKeys {
			ret, err := client.ZIncrBy(key, 1, strconv.Itoa(rowID)).Result()
			if ret <= 0 || err != nil {
				log.Errorln("dataStorage redis storage error.", "INCRBY", key, rowID)
			}
		}
	}
}
