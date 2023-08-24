package main

import (
	"fmt"
	"strconv"
	"strings"
)

var R int = 10

func F_map(file_name string, line_number int, line_text string, output *[]string) {
	tokens := strings.Fields(line_text)
	if len(tokens) < 4 {
		return
	}

	var crawler, domain, url string

	crawler = tokens[2]

	if strings.HasPrefix(tokens[3], "http://") {
		domain = tokens[3][7:]
	} else if strings.HasPrefix(tokens[3], "https://") {
		domain = tokens[3][8:]
	} else {
		domain = tokens[3]
	}

	if n := strings.Index(domain, "/"); n > 0 {
		url = domain[n+1:]
		domain = domain[:n]
	}

	if domain[0] < '0' || domain[0] > '9' {
		if c := strings.Count(domain, "."); c > 1 {
			domain = domain[strings.Index(domain, ".")+1:]
		}
	} else {
		if n := strings.LastIndex(domain, ":"); n > 0 {
			domain = domain[:n]
		}
	}

	if len(url) > 0 {
		if n := strings.IndexAny(url, "#?"); n > 0 {
			url = url[:n]
		}

		*output = append(*output, fmt.Sprintf("%s %s %s\n", crawler, domain, url))
	} else {
		*output = append(*output, fmt.Sprintf("%s %s\n", crawler, domain))
	}

	*output = append(*output, fmt.Sprintln(1))
}

func F_reduce(keys []string, values []string, output *[]string) {
	crawler_map := make(map[string]int)
	domain_map := make(map[string]int)
	url_map := make(map[string]int)

	l := len(keys)
	if len(values) < l {
		l = len(values)
	}

	for i := 0; i < l; i++ {
		ks := strings.Fields(keys[i])
		v, err := strconv.Atoi(values[i])
		if err != nil {
			continue
		}

		crawler_map[ks[0]] += v
		domain_map[ks[1]] += v
		if len(ks) > 2 {
			url_map[fmt.Sprintf("%s%s", ks[1], ks[2])] += v
		}
	}

	*output = append(*output, fmt.Sprintf("* Unique URLs:    %d", len(url_map)+len(domain_map)))
	*output = append(*output, fmt.Sprintf("* Unique Domains: %d", len(domain_map)))

	top_dms_n := 10
	if len(domain_map) < 10 {
		top_dms_n = len(domain_map)
	}

	var top_dms []string
	for i := 0; i < top_dms_n; i++ {
		max_k := ""
		max_v := -1
		for k, v := range domain_map {
			if v > max_v {
				max_k = k
				max_v = v
			}
		}

		top_dms = append(top_dms, max_k)
		delete(domain_map, max_k)
	}

	top_crs_n := 5
	if len(crawler_map) < 5 {
		top_crs_n = len(crawler_map)
	}

	var top_crs []string
	for i := 0; i < top_crs_n; i++ {
		max_k := ""
		max_v := -1
		for k, v := range crawler_map {
			if v > max_v {
				max_k = k
				max_v = v
			}
		}

		top_crs = append(top_crs, max_k)
		delete(crawler_map, max_k)
	}

	if top_dms_n > 0 {
		*output = append(*output, fmt.Sprintf("* Top %d Websites:", top_dms_n))
		for _, dm := range top_dms {
			*output = append(*output, fmt.Sprintf("    - %s", dm))
		}
	}
	if top_crs_n > 0 {
		*output = append(*output, fmt.Sprintf("* Top %d Crawlers:", top_crs_n))
		for _, cr := range top_crs {
			*output = append(*output, fmt.Sprintf("    - %s", cr))
		}
	}
}
