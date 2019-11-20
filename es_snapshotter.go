package main

import (
    "flag"
    "fmt"
    "log"
    "time"
    "errors"
    "strings"
    "context"
    "os"
    "io/ioutil"
    "github.com/ghodss/yaml"
    "github.com/elastic/go-elasticsearch"
    "github.com/elastic/go-elasticsearch/esapi"
)

type Config struct {
    Ip string `json:"elastic_ip"`
    Port  string `json:"elastic_port"`
    Repos []string `json:"list_of_indice_sets"`
}

var pathError *os.PathError

func main() {
    c, err := parseConfig()
    if err != nil {
        log.Fatalf("ERROR: %s", err)
        return
    }

    // repoName, cleanupFlag := parseFlags(c)
    repoName := parseFlags(c)

    for i := range c.Repos {
        if *repoName == c.Repos[i] {
            es := createESClient(c)
            // if *cleanupFlag {
            //     cleanup(*repoName)
            // }
            err := createSnapshot(*repoName, es)
            if err != nil {
               log.Fatalf("ERROR: %s", err)
            }
            return
        }
    }
    flag.Usage()

}


func createSnapshot(r string, es *elasticsearch.Client) error {
    date := todayDate();
    snapshotName := r + date
    var bodyString string
    if r == "all" {
        bodyString = `{"ignore_unavailable" : true, "include_global_state" : false}`
    } else {
        bodyString = `{"indices":"`+ r +`*", "ignore_unavailable" : true, "include_global_state" : false}`
    }

    var WFC bool = true

    req := esapi.SnapshotCreateRequest{
        Body:       strings.NewReader(bodyString),
        Repository: r,
        Snapshot:   snapshotName,
        WaitForCompletion: &WFC,
        Pretty:     true,
    }

    res, err := req.Do(context.Background(), es)
    if err != nil {
        log.Fatalf("Error getting response: %s", err)
    }
    defer res.Body.Close()
    if res.IsError() {
        return errors.New(res.String())
    }
    return nil
}

func todayDate() string {
    return time.Now().Format("_02_01_2006")
}

func cleanup(r string) {
    fmt.Println("cleaned up: " + r)
}

func createESClient(c Config) *elasticsearch.Client {
    address := "http://" + c.Ip + ":" + c.Port
    cfg := elasticsearch.Config{
      Addresses: []string{
        address,
      },
    }

    es, err := elasticsearch.NewClient(cfg)
    if err != nil {
        log.Fatalf("ERROR: %s", err)
    }
    return es
}

func parseFlags(c Config) (*string) {
// func parseFlags(c Config) (*string, *bool) {
    repoNameHelpInfo := "repo name, available: "
    for i := range c.Repos {
        repoNameHelpInfo += (`"` + c.Repos[i] + `" `)
    }
    repoName := flag.String("r", "", repoNameHelpInfo)
    // cleanupFlag := flag.Bool("cleanup", false, "cleanup repo first")
    flag.Parse()
    return repoName
    // return repoName, cleanupFlag
}


func parseConfig() (Config, error) {
    var c Config
    rawConfig, err := ioutil.ReadFile("/etc/es-snapshotter/config.yml")
    if err != nil {
        if errors.As(err, &pathError) {
            return c, errors.New("Please create '/etc/es-snapshotter/config.yml'\nExample:\n   elastic_ip: '1.1.1.1'\n   elastic_port: 9200\n   list_of_indice_sets: [payment, all]\n    *'all' stands for everything in elasticsearch")
        }
        return c, err
    }
    err = yaml.Unmarshal(rawConfig, &c)
    if err != nil {
        return c, err
    }
    return c, nil
}