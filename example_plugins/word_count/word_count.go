package main

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

var R int = 26

func F_map(file_name string, line_number int, line_text string, output *[]string) {
	tokens_a := strings.Fields(strings.ToLower(line_text))
	var tokens []string
	for _, t := range tokens_a {
		tokens = append(tokens, regexp.MustCompile(`[^a-zA-Z0-9]+`).ReplaceAllString(t, ""))
	}

	for _, token := range tokens {
		*output = append(*output, fmt.Sprintln(token))
		*output = append(*output, fmt.Sprintln(1))
	}
}

func F_reduce(keys []string, values []string, output *[]string) {
	reduce_map := make(map[string]int)

	l := len(keys)
	if len(values) < l {
		l = len(values)
	}

	for i := 0; i < l; i++ {
		k := keys[i]
		v, err := strconv.Atoi(values[i])
		if err != nil {
			continue
		}

		reduce_map[k] += v
	}

	for k, v := range reduce_map {
		*output = append(*output, fmt.Sprintf("%-12s :: %4d", k, v))
	}
}
