// Copyright 2012-present Oliver Eilhard. All rights reserved.
// Use of this source code is governed by a MIT-license.
// See http://olivere.mit-license.org/license.txt for details.

package elastic

import (
	"fmt"
	"net"
	"net/url"
	"strings"
)

// canonicalize takes a list of URLs and returns its canonicalized form, i.e.
// remove anything but scheme, userinfo, host, path, and port.
// It also removes all trailing slashes. Invalid URLs or URLs that do not
// use protocol http or https are skipped.
//
// Example:
// http://127.0.0.1:9200/?query=1 -> http://127.0.0.1:9200
// http://127.0.0.1:9200/db1/ -> http://127.0.0.1:9200/db1

// func canonicalize(rawurls ...string) []string {
// 	var canonicalized []string
// 	for _, rawurl := range rawurls {
// 		u, err := url.Parse(rawurl)
// 		if err == nil {
// 			if u.Scheme == "http" || u.Scheme == "https" {
// 				// Trim trailing slashes
// 				for len(u.Path) > 0 && u.Path[len(u.Path)-1] == '/' {
// 					u.Path = u.Path[0 : len(u.Path)-1]
// 				}
// 				u.Fragment = ""
// 				u.RawQuery = ""
// 				canonicalized = append(canonicalized, u.String())
// 			}
// 		}
// 	}
// 	return canonicalized
// }

func canonicalize(rawurls ...string) []string {
	canonicalized := make([]string, 0)
	for _, rawurl := range rawurls {
		u, err := url.Parse(rawurl)
		if err == nil && (u.Scheme == "http" || u.Scheme == "https") {
			u.Fragment = ""
			u.Path = ""
			u.RawQuery = ""
			canonicalized = append(canonicalized, u.String())
		} else if err == nil {
			host := strings.Split(rawurl, ":")
			addrs, err := net.LookupHost(host[0])
			if err == nil {
				addr := fmt.Sprintf("%s:%s", addrs[0], host[1])
				canonicalized = append(canonicalized, fmt.Sprintf("http://%s", addr))
			}
		}
	}
	return canonicalized
}
