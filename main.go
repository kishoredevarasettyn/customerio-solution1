package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"syscall"

	"github.com/customerio/homework/stream"
)

// structure to hold the User Attributes
type Attr struct {
	value     string
	timestamp int64
}

type attrMap map[string]Attr
type eventMap map[string]map[string]bool

type User struct {
	attrMap  attrMap
	eventMap eventMap
}

var output = flag.String("out", "data.csv", "output file")
var input = flag.String("in", "", "input file")
var verify = flag.String("verify", "", "verification file")

func main() {
	ctx, cancel := context.WithCancel(context.Background())

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		cancel()
	}()

	flag.Parse()

	fileName := input
	verifyFile := verify
	outputFile := output

	f, err := os.Open(*fileName)
	content := io.ReadSeeker(f)
	if err != nil {
		log.Fatalf("open file error: %v", err)
		return
	}
	defer f.Close()

	log.Println("Start processing the file")

	// Process the file to get user map: Aggregated by attributes and events
	result := processRecords(ctx, content)
	log.Println("User Map created")
	// sort the records based on the User id and write to file
	sortAndWriteOutput(outputFile, result)
	log.Println("output csv written")

	// validate the output with the verify file
	err2:= validate(*outputFile, *verifyFile)
	if(err2 != nil){
		fmt.Println(err2)
	}else{
		log.Println("successfully validated")
	}

	if err := ctx.Err(); err != nil {
		log.Fatal(err)
	}
}

// Sort the User ids of the map and sort attributes and events alphbaetically
func sortAndWriteOutput(outputFile *string, result1 userMap) {
	fw, err := os.Create(*outputFile)

	if err != nil {
		log.Fatal(err)
	}

	defer fw.Close()

	for _, k := range result1.sort() {

		user := result1[k]
		sb := k
		for _, i := range user.attrMap.sort() {
			attr := user.attrMap[i]
			sb = sb + "," + i + "=" + attr.value
		}
		for _, i := range user.eventMap.sort() {
			evnt := user.eventMap[i]
			sb = sb + "," + i + "=" + strconv.Itoa(len(evnt))
		}
		_, err2 := fw.WriteString(sb + "\n")

		if err2 != nil {
			log.Fatal(err2)
		}

	}
}

// Read the stream of records and create the user map aggregated by user id. 
// Attributes latest value will be put
// Events maintain count of unique uuids
func processRecords(ctx context.Context, content io.ReadSeeker) userMap {

	ch, err := stream.Process(ctx, content)
	if err != nil {
		log.Fatal(err)
	}
	aggregatedUserMap := make(userMap)
	for rec := range ch {
		if rec.UserID == "" {
			continue
		}
		user, isPresent := aggregatedUserMap[rec.UserID]
		if !isPresent {
			user = User{make(attrMap), make(map[string]map[string]bool)}
		}
		aggregatedUserMap[rec.UserID] = updateUserMap(rec, user)

	}
	return aggregatedUserMap
}

func (m attrMap) sort() (index []string) {
	for k, _ := range m {
		index = append(index, k)
	}
	sort.Strings(index)
	return
}


func (m eventMap) sort() (index []string) {
	for k, _ := range m {
		index = append(index, k)
	}
	sort.Strings(index)
	return
}

type userMap map[string]User

func (m userMap) sort()(index []string){
	for k, _ := range m {
		index = append(index, k)
	}
	sort.Strings(index)
	return
}

func updateUserMap(record *stream.Record, user User) User {
	attrMap := user.attrMap
	eventMap := user.eventMap

	if record.Type == "attributes" {
		for newKey, newValue := range record.Data {
			val := attrMap[newKey]
			if val.value != "" {
				if val.timestamp < record.Timestamp {
					attr := Attr{newValue, record.Timestamp}
					attrMap[newKey] = attr
				}
			} else {
				attr := Attr{newValue, record.Timestamp}
				attrMap[newKey] = attr
			}
		}
	}else{
		idMap:= eventMap[record.Name]
		if idMap == nil{
			idMap = make(map[string]bool)
		}
		idMap[record.ID] = true;
		eventMap[record.Name] = idMap
	}
	user.attrMap = attrMap
	user.eventMap = eventMap

	return user 
}

// Quick validation of expected and received input.
func validate(have, want string) error {
	f1, err := os.Open(have)
	if err != nil {
		return err
	}
	defer f1.Close()

	f2, err := os.Open(want)
	if err != nil {
		return err
	}
	defer f2.Close()

	s1 := bufio.NewScanner(f1)
	s2 := bufio.NewScanner(f2)
	for s1.Scan() {
		if !s2.Scan() {
			return fmt.Errorf("want: insufficient data")
		}
		t1 := s1.Text()
		t2 := s2.Text()
		if t1 != t2 {
			return fmt.Errorf("have/want: difference\n%s\n%s", t1, t2)
		}
	}
	if s2.Scan() {
		return fmt.Errorf("have: insufficient data")
	}
	if err := s1.Err(); err != nil {
		return err
	}
	if err := s2.Err(); err != nil {
		return err
	}
	return nil
}
