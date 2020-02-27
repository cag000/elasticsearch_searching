package main

import (
	"bytes"
	_ "compress/gzip"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	_ "io"
	"log"
	"os"
	_ "reflect"
	_ "sync"

	"github.com/elastic/go-elasticsearch/v6"
)

func main() {
	//costum formatting
	log.SetFlags(0)

	var (
		r map[string]interface{}
		link_store []interface{}
		// wg sync.WaitGroup
	)
	//context obj for api calls
	ctx := context.Background()

	//es config
	cfg := elasticsearch.Config{
		Addresses: []string{
			"http://127.0.0.1:9200", //fill with ip
		},
	}

	es, err := elasticsearch.NewClient(cfg)

	if err != nil {
		log.Println(err)
	}
	
	var buf bytes.Buffer
	query := map[string]interface{}{
		"_source": []interface{}{
			"link", "title", "created_at",
		},
		// "size": 10,
		"sort": []interface{}{
			map[string]interface{}{
				"created_at": map[string]interface{}{
					"order": "desc",
				},
			},
		},
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"must": []interface{}{
					map[string]interface{}{
						"range": map[string]interface{}{
							"created_at": map[string]interface{}{
								"gte": "13/02/2020 00:00:00",
								"lte": "25/02/2020 23:59:59",
								"format": "dd/MM/yyyy HH:mm:ss",
								"time_zone": "+07:00",
							},
						},
					},
				},
			},
		},
	}

	if err := json.NewEncoder(&buf).Encode(query); err != nil {
		log.Fatal("json.NewEncoder() ERROR:", err)
	}else{
		fmt.Println("json.NewEnconder encoded query:", query)
	}

	res, _ := es.Search(
		es.Search.WithContext(ctx),
		es.Search.WithIndex("logging-online-news-invalid"),
		es.Search.WithTrackTotalHits(true),
		es.Search.WithBody(&buf),
		es.Search.WithSize(10000),
	)

	defer res.Body.Close()
	
	if res.IsError() {
		var m map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&m); err != nil {
			log.Fatalf("error: %s", err)
		} else {
			//print response status
			log.Fatalf(
				"[%s] %s: %s",
				res.Status(),
				m["error"].(map[string]interface{})["type"],
				m["error"].(map[string]interface{})["reaseon"],
			)
		}
	}

	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		log.Fatalf("error: %s", err)
	}

	//print respon status
	log.Printf(
		"[%s] %d hits; took: %dms",
		res.Status(),
		int(r["hits"].(map[string]interface{})["total"].(map[string]interface{})["value"].(float64)),
		int(r["took"].(float64)),
	)

	//print id and doc source
	for _, hit := range r["hits"].(map[string]interface{})["hits"].([]interface{}) {
		// log.Printf("%d", hit...)
		// log.Printf("%s, %s", hit.(map[string]interface{})["_id"], hit.(map[string]interface{})["_source"].(map[string]interface{})["link"])
		link_store = append(link_store,hit.(map[string]interface{})["_source"].(map[string]interface{})["link"])
	}

	// fmt.Print(len(link_store))
	
	//change interface to string
	change := make([][]string, 1)
	for _, v := range link_store {
		change = append(change, []string{fmt.Sprint(v)})
	}

	//make csv
	file, err := os.Create("me.csv")
	if err != nil {
		log.Fatalf("%s",err)
	}
	writeMe := csv.NewWriter(file)
	for _, v := range change{
		_ = writeMe.Write(v)
	}
	writeMe.Flush()
	file.Close()

}