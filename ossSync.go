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
	configFile = flag.String("f", "config.ini", "General configuration file")
)

var configList = make(map[string]string)
var syncPerPage int = 1000
var thread int = 2
var errorObject chan oss.ObjectProperties = make(chan oss.ObjectProperties)
var errorObjectChan chan string = make(chan string)

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
	sourceBucket *oss.Bucket
	destBucket   *oss.Bucket
	objectList   []oss.ObjectProperties
	config       map[string]string
	errorObjects []oss.ObjectProperties
	processChan  chan string
}

func (o *ossPackage) getObjectList(marker string) ([]oss.ObjectProperties, string, error) {
	objectResult := make([]oss.ObjectProperties, 0)
	lor, err := o.sourceBucket.ListObjects(oss.MaxKeys(syncPerPage), oss.Prefix(o.config["srcPrefix"]), oss.Marker(marker))
	if err != nil {
		return nil, "", err
	}
	for _, v := range lor.Objects {
		objectResult = append(objectResult, v)
	}
	return objectResult, lor.NextMarker, nil
}

func goProcess(o *ossPackage, objects []oss.ObjectProperties) {
	for _, v := range objects {
		body, err := o.sourceBucket.GetObject(v.Key)
		if err != nil {
			errorObject <- v
			continue
		}
		defer body.Close()
		data, err := ioutil.ReadAll(body)
		if err != nil {
			errorObject <- v
			continue
		}

		if o.config["downLoadFile"] == "1" {
			dir, file := path.Split(o.config["downloadDir"] + "/" + v.Key)
			if PathExists(dir) == false {
				os.MkdirAll(dir, 0777)
			}
			err := ioutil.WriteFile(dir + file, data, os.ModePerm)
			if err != nil {
				errorObject <- v
				continue
			}
		}

		err = o.destBucket.PutObject(v.Key, bytes.NewReader(data))
		if err != nil {
			errorObject <- v
			continue
		}
		fmt.Println("synced:", v.Key)
	}
	<-o.processChan
}

func goProcessErrorObjects(o *ossPackage) {
	for _, v := range o.errorObjects {
		body, err := o.sourceBucket.GetObject(v.Key)
		if err != nil {
			fmt.Println(err.Error())
			continue
		}
		defer body.Close()
		data, err := ioutil.ReadAll(body)
		if err != nil {
			fmt.Println(err.Error())
			continue
		}

		if o.config["downLoadFile"] == "1" {
			dir, file := path.Split(o.config["downloadDir"] + "/" + v.Key)
			if PathExists(dir) == false {
				os.MkdirAll(dir, 0777)
			}
			err := ioutil.WriteFile(dir + file, data, os.ModePerm)
			if err != nil {
				fmt.Println(err.Error())
				continue
			}
		}

		err = o.destBucket.PutObject(v.Key, bytes.NewReader(data))
		if err != nil {
			fmt.Println(err.Error())
			continue
		}
		errorObjectChan <- "resynced: " + v.Key
	}
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
	go func() {
		for {
			select {
			case res := <-errorObject:
				o.errorObjects = append(o.errorObjects, res)
			}
		}
	}()
	for {
		o.processChan <- "start"
		objectResult, nextMarker, err := o.getObjectList(marker)
		if err != nil {
			return err
		}
		if len(objectResult) == 0 {
			return errors.New("ossList is empty!")
		}
		go goProcess(o, objectResult)
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
	if downLoadFile, ok := configList["downLoadFile"]; ok {
		fmt.Printf("downLoadFile: %v\n", configList["downLoadFile"])
		if downLoadFile == "1" {
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
		fmt.Println("Fail load downLoadFile option!")
		return
	}

	if maxKeys, ok := configList["maxKeys"]; ok {
		maxKeys, err := strconv.Atoi(maxKeys)
		if err == nil || maxKeys < syncPerPage && maxKeys > 0 {
			syncPerPage = maxKeys
		}
	}
	fmt.Println("Max keys: ", syncPerPage)
	if _thread, ok := configList["thread"]; ok {
		newThread, err := strconv.Atoi(_thread)
		if err == nil && newThread > 0 {
			thread = newThread
		}
	}
	fmt.Println("Threads: ", thread)
	fmt.Println("sync start...")

	ossPackage := &ossPackage{sourceBucket:sourceBucket, destBucket:destBucket, config:configList, processChan:make(chan string, thread)}
	err = ossPackage.ProcessObjectList()
	if err != nil {
		fmt.Println(err.Error())
	} else {
		os.Remove(lastMarkerFile)
	}
	errorObjectsLen := len(ossPackage.errorObjects)
	if errorObjectsLen > 0 {
		fmt.Println("errorObject Len:", errorObjectsLen)
		go goProcessErrorObjects(ossPackage)
		i := 0
		for i < errorObjectsLen {
			select {
			case res := <-errorObjectChan:
				fmt.Println(res)
				i++
			}
		}
	}

	endTime := time.Now().Unix()
	fmt.Printf("runTime: %v seconds", (endTime - startTime))
}