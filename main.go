package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"math"
	"math/rand"
	"os"
	"sync"
	"time"
)

var queries = []string{
	`SELECT
    playback_id as deviceType,
    count(distinct session_id) as view_count
FROM playbackLogs
where fromUnixTimestamp64Milli(timestamp) > now() - toIntervalSecond(300)
group by playback_id`,
	`SELECT
    playback_id as deviceType,
    count(distinct session_id) as view_count
FROM playbackLogs
where fromUnixTimestamp64Milli(timestamp) > now() - toIntervalSecond(300)
and playback_id='qrstuvwx-8'
group by playback_id`,
	`SELECT
    deviceType as deviceType,
    count(distinct session_id) as view_count
FROM playbackLogs
where fromUnixTimestamp64Milli(timestamp) > now() - toIntervalSecond(300)
group by deviceType`,
	`SELECT
    playback_country_name as deviceType,
    count(distinct session_id) as view_count
FROM playbackLogs
where fromUnixTimestamp64Milli(timestamp) > now() - toIntervalSecond(300)
group by playback_country_name`,
	`SELECT
    playback_continent_name as deviceType,
    count(distinct session_id) as view_count
FROM playbackLogs
where fromUnixTimestamp64Milli(timestamp) > now() - toIntervalSecond(300)
group by playback_continent_name`,
}

const simpleQuery = `
select * from my_first_table;;
`

func main() {
	ctx := context.Background()
	done := make(chan bool)

	var sumMu, mu sync.RWMutex
	var count, sum, min, max, totalViewCount uint64
	durs := make(chan time.Duration)
	sums := make(chan uint64)

	go func() {
		for s := range sums {
			sumMu.Lock()
			totalViewCount += s
			sumMu.Unlock()
		}
	}()

	go func() {
		for i := range durs {
			mu.Lock()
			ui := uint64(i)
			sum += ui
			count++
			if ui > max {
				max = ui
			}
			if ui < min {
				min = ui
			}
			mu.Unlock()
		}
	}()

	go func() {
		for {
			time.Sleep(5 * time.Second)
			mu.Lock()
			sumMu.Lock()
			var avg time.Duration
			var avgViewCount uint64
			if count != 0 {
				avg = time.Duration(float64(sum) / float64(count))
				avgViewCount = totalViewCount / count
			}
			fmt.Printf("avg: %v, min: %v, max: %v, count: %d, view count: %d\n", avg, time.Duration(min), time.Duration(max), count, avgViewCount)
			count = 0
			sum = 0
			min = math.MaxUint64
			max = 0
			totalViewCount = 0
			sumMu.Unlock()
			mu.Unlock()
		}
	}()

	var conns []driver.Conn
	for i := 0; i < 20; i++ {
		conn, err := connect()
		if err != nil {
			panic((err))
		}
		conns = append(conns, conn)
	}

	for i := 0; i < 100; i++ {
		go func() {
			n := rand.Intn(len(queries))
			time.Sleep(time.Duration(rand.Intn(5000)) * time.Millisecond)
			for {
				start := time.Now()
				sum := makeQueries(ctx, conns[i%(len(conns))], n)
				end := time.Now()
				dur := end.Sub(start)
				fmt.Printf("Took: %s\n", dur)
				sums <- sum
				durs <- dur
				time.Sleep((5 * time.Second) - dur)
			}
		}()
	}

	<-done
}

func makeQueries(ctx context.Context, conn driver.Conn, n int) uint64 {

	_, err := conn.Query(ctx, queries[n])
	//_, err := conn.Query(ctx, queries[n])
	//_, err := conn.Query(ctx, simpleQuery)
	if err != nil {
		//log.Fatal(err)
	}

	//sum += len(rows)

	var sum uint64
	//for rows.Next() {
	//	var (
	//		device_type string
	//		view_count  uint64
	//	)
	//	if err := rows.Scan(
	//		&device_type,
	//		&view_count,
	//	); err != nil {
	//		log.Fatal(err)
	//	}
	//	sum += view_count
	//	//log.Printf("device_type: %s, view_count: %v", device_type, view_count)
	//}
	//log.Printf("sum: %d", sum)
	return sum
}

func connect() (driver.Conn, error) {
	hostname, _ := os.LookupEnv("CLICKHOUSE_HOSTNAME")
	username, _ := os.LookupEnv("CLICKHOUSE_USER")
	password, _ := os.LookupEnv("CLICKHOUSE_PASSWORD")
	var (
		ctx       = context.Background()
		conn, err = clickhouse.Open(&clickhouse.Options{
			Addr: []string{fmt.Sprintf("%s:9440", hostname)},
			Auth: clickhouse.Auth{
				Database: "default",
				Username: username,
				Password: password,
			},
			ClientInfo: clickhouse.ClientInfo{
				Products: []struct {
					Name    string
					Version string
				}{
					{Name: "an-example-go-client", Version: "0.1"},
				},
			},

			Debugf: func(format string, v ...interface{}) {
				fmt.Printf(format, v)
			},
			TLS: &tls.Config{
				InsecureSkipVerify: true,
			},
		})
	)

	if err != nil {
		return nil, err
	}

	if err := conn.Ping(ctx); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			fmt.Printf("Exception [%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		}
		return nil, err
	}
	return conn, nil
}
