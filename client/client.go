package main

import (
	"bufio"
	"dfs/cl"
	"fmt"
	"log"
	"os"
	"path"
	"strings"
)

// -h
func usage(dir string) {
	fmt.Printf(`
  Distributed File / Computation System Actions
-----------------------------------
--MapReduce: glean information from text files on the server
--> mr {job_name} {optional: names of files to consider}
    Note: need a matching [job_name].go file in
    %s with code similar to examples like
	word_count.go in the program documentation.

--Store: upload a file to the server
--> store {path/file_name} {optional: chunk_size in MiB}

--Retrieve: download a file from the server
--> retrieve {file_name} {optional: download_directory}

--Delete: delete a file in the server
--> delete {file_name}

--List: list files stored in the system
--> ls

--Activity: get stats on the server and its nodes
--> activity

--Info: get info on a certain node
--> info {node_number}

--Exit
--> exit

`, dir)
}

func main() {
	if len(os.Args) < 4 {
		log.Printf("Insufficient command-line arguments\n  Needed:   ./client [controller_hostname] [cmp_manager_hostname] [download_dir]\n  Example:  ./client orion02 orion03 /bigdata/students/ljendrusch\n  Provided: %s\n", os.Args)
		os.Exit(1)
	}

	CTRL_ADDR := fmt.Sprintf("%s:%d", os.Args[1], 14980)
	CMMN_ADDR := fmt.Sprintf("%s:%d", os.Args[2], 14990)
	var DIR string
	DIR = path.Clean(os.Args[3])
	if !path.IsAbs(DIR) {
		wd, _ := os.Getwd()
		DIR = path.Join(wd, DIR)
	}

	err := os.MkdirAll(path.Join(DIR, "job_plugins"), 0755)
	if err != nil {
		log.Fatalln(err)
	}
	err = os.MkdirAll(path.Join(DIR, "job_results"), 0755)
	if err != nil {
		log.Fatalln(err)
	}

	scanner := bufio.NewScanner(os.Stdin)
	usage(DIR)

	for {
		fmt.Printf("\n  Awaiting your command:\n> ")

		scanner.Scan()
		if err := scanner.Err(); err != nil {
			log.Println(err)
			continue
		}

		s := scanner.Text()
		tokens := strings.Fields(s)

		addr := CTRL_ADDR
		for i := 1; i < len(tokens); i++ {
			if tokens[i][0] == '-' && strings.EqualFold(tokens[i][1:], "cm") {
				addr = CMMN_ADDR
				tokens = append(tokens[:i], tokens[i+1:]...)
				break
			}
		}

		switch {
		case strings.EqualFold(tokens[0], "store"):
			cl.Store(CTRL_ADDR, tokens...)

		case strings.EqualFold(tokens[0], "retrieve"):
			cl.Retrieve(CTRL_ADDR, DIR, tokens...)

		case strings.EqualFold(tokens[0], "delete"):
			cl.Delete(CTRL_ADDR, tokens...)

		case strings.HasPrefix(strings.ToLower(tokens[0]), "map") && strings.HasSuffix(strings.ToLower(tokens[0]), "reduce"), strings.EqualFold(tokens[0], "mr"):
			cl.Map_reduce(CMMN_ADDR, DIR, tokens...)

		case strings.EqualFold(tokens[0], "ls"), strings.EqualFold(tokens[0], "list"):
			cl.List(addr, tokens...)

		case strings.EqualFold(tokens[0], "info"):
			cl.Info(addr, tokens...)

		case strings.EqualFold(tokens[0], "activity"), strings.EqualFold(tokens[0], "ac"), strings.EqualFold(tokens[0], "act"):
			cl.Activity(addr)

		case strings.HasSuffix(strings.ToLower(tokens[0]), "help"), strings.EqualFold(tokens[0], "-h"):
			usage(DIR)

		case strings.EqualFold(tokens[0], "exit"), strings.EqualFold(tokens[0], "end"), strings.EqualFold(tokens[0], "quit"):
			fmt.Println("Now exiting")
			os.Exit(0)

		default:
			fmt.Printf("Unknown or malformed command [%s]\n", s)
		}
	}
}
