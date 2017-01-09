package main

import (
	"flag"
	"fmt"
	"github.com/larspensjo/config"
	"runtime"
	"os"
	"io/ioutil"
	"bytes"
	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"path"
	"strings"
	"time"
	"strconv"
	"errors"
)

var (
	configFile = flag.String("configfile", "config.ini", "General configuration file")
)

var configList = make(map[string]string)
var syncPerPage int = 1000

const (
	configCommonSection = "common"
	configSourceSection = "source"
	configDestSection = "dest"
	configDefaultDownloadDir = "./download"
	lastMarkerFile = "./lastMarker"
)

func PathExists(path string) bool {
	if _, err := os.Stat(path); err == nil {
		return true
	}
	return false
}

type ossPackage struct {
	SourceBucket *oss.Bucket
	DestBucket   *oss.Bucket
	objectList   []oss.ObjectProperties
	Config       map[string]string
	errorObject  []string
	lastMarker   string
}

func (o *ossPackage) getObjectList(marker string) ([]oss.ObjectProperties, string, error) {
	objectResult := make([]oss.ObjectProperties, 0)
	lor, err := o.SourceBucket.ListObjects(oss.MaxKeys(syncPerPage), oss.Prefix(o.Config["srcPrefix"]), oss.Marker(marker))
	if err != nil {
		return nil, "", err
	}
	for _, v := range lor.Objects {
		objectResult = append(objectResult, v)
	}
	return objectResult, lor.NextMarker, nil
}

func (o *ossPackage) writeLastMarkerFile(content string) error {
	if content == "" {
		return nil
	}
	err := ioutil.WriteFile(lastMarkerFile, []byte(content), os.ModePerm)
	return err
}

func (o *ossPackage) ProcessObjectList() error {
	marker := ""
	lastMarkerContent, err := ioutil.ReadFile(lastMarkerFile)
	if err == nil {
		marker = string(lastMarkerContent)
		if marker != "" {
			fmt.Println("marker reset:", marker)
		}
	}
	backMarker := ""
	for {
		objectResult, nextMarker, err := o.getObjectList(marker)
		if err != nil {
			return err
		}
		if len(objectResult) == 0 {
			return errors.New("ossList is empty!")
		}

		for _, v := range objectResult {
			body, err := o.SourceBucket.GetObject(v.Key)
			if err != nil {
				o.writeLastMarkerFile(backMarker)
				return err
			}
			defer body.Close()
			data, err := ioutil.ReadAll(body)
			if err != nil {
				o.writeLastMarkerFile(backMarker)
				return err
			}

			if o.Config["syncMode"] == "2" {
				dir, file := path.Split(o.Config["downloadDir"] + "/" + v.Key)
				if PathExists(dir) == false {
					os.MkdirAll(dir, 0777)
				}
				err := ioutil.WriteFile(dir + file, data, os.ModePerm)
				if err != nil {
					o.writeLastMarkerFile(backMarker)
					return err
				}
			}

			err = o.DestBucket.PutObject(v.Key, bytes.NewReader(data))
			if err != nil {
				o.writeLastMarkerFile(backMarker)
				return err
			}
			backMarker = v.Key

			fmt.Println("synced:", v.Key)
		}
		if nextMarker == "" {
			break
		}
		marker = nextMarker
	}
	return nil
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	flag.Parse()
	startTime := time.Now().Unix()

	//set config file std
	cfg, err := config.ReadDefault(*configFile)
	if err != nil {
		fmt.Println("Fail to load", *configFile)
		return
	}
	if cfg.HasSection(configCommonSection) == false {
		fmt.Println("Fail to load common section")
		return
	}
	if cfg.HasSection(configSourceSection) == false {
		fmt.Println("Fail to load source section")
		return
	}
	if cfg.HasSection(configDestSection) == false {
		fmt.Println("Fail to load dest section")
		return
	}
	commonSection, err := cfg.SectionOptions(configCommonSection)
	for _, v := range commonSection {
		options, err := cfg.String(configCommonSection, v)
		if err == nil {
			configList[v] = options
		}
	}
	sourceSection, err := cfg.SectionOptions(configSourceSection)
	for _, v := range sourceSection {
		options, err := cfg.String(configSourceSection, v)
		if err == nil {
			configList[v] = options
		}
	}
	destSection, err := cfg.SectionOptions(configDestSection)
	for _, v := range destSection {
		options, err := cfg.String(configDestSection, v)
		if err == nil {
			configList[v] = options
		}
	}

	sourceClient, err := oss.New(configList["srcEndpoint"], configList["srcAccessKey"], configList["srcSecretKey"])
	if err != nil {
		fmt.Println("source client error:", err.Error())
		return
	}
	// Get Bucket
	sourceBucket, err := sourceClient.Bucket(configList["srcBucket"])
	if err != nil {
		fmt.Println("source Bucket error:", err.Error())
		return
	}

	destClient, err := oss.New(configList["destEndpoint"], configList["destAccessKey"], configList["destSecretKey"])
	if err != nil {
		fmt.Println("dest client error:", err.Error())
		return
	}
	// Get Bucket
	destBucket, err := destClient.Bucket(configList["destBucket"])
	if err != nil {
		fmt.Println("dest Bucket error:", err.Error())
		return
	}
	if syncMode, ok := configList["syncMode"]; ok {
		fmt.Printf("Sync Mode: %v\n", configList["syncMode"])
		if syncMode == "2" {
			if downloadDir, ok := configList["downloadDir"]; ok {
				configList["downloadDir"] = strings.TrimRight(downloadDir, "/")
				if configList["downloadDir"] == "" {
					configList["downloadDir"] = configDefaultDownloadDir
				}
				fmt.Println("source will be downloaded to: ", configList["downloadDir"])
			} else {
				fmt.Println("Fail load downloadDir option!")
				return
			}
		}
	} else {
		fmt.Println("Fail load syncMode option!")
		return
	}

	if maxKeys, ok := configList["maxKeys"]; ok {
		maxKeys, _ := strconv.Atoi(maxKeys)
		if maxKeys < syncPerPage {
			syncPerPage = maxKeys
		}
	}

	fmt.Println("Max keys: ", syncPerPage)
	fmt.Println("sync start...")

	ossPackage := &ossPackage{SourceBucket:sourceBucket, DestBucket:destBucket, Config:configList}
	err = ossPackage.ProcessObjectList()
	if err != nil {
		fmt.Println(err.Error())
	} else {
		os.Remove(lastMarkerFile)
	}
	endTime := time.Now().Unix()
	fmt.Printf("runTime: %v seconds", (endTime - startTime))
}