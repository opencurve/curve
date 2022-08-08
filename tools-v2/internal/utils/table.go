/*
 *  Copyright (c) 2022 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: CurveCli
 * Created Date: 2022-05-25
 * Author: chengyi (Cyber-SiKu)
 */

package cobrautil

import (
	"sort"

	"golang.org/x/exp/slices"
)

func ListMap2ListSortByKeys(rows []map[string]string, headers []string, keys []string) [][]string {
	var ret [][]string
	for i := range rows {
		var list []string
		for _, j := range headers {
			list = append(list, rows[i][j])
		}
		ret = append(ret, list)
	}
	var keysIndex []int
	for _, key := range keys {
		keyIndex := slices.Index(headers, key)
		if keyIndex != -1 {
			keysIndex = append(keysIndex, keyIndex)
		}
	}
	if len(keysIndex) > 0 {
		sort.SliceStable(ret, func(i, j int) bool {
			for _, keyIndex := range keysIndex {
				if ret[i][keyIndex] < ret[j][keyIndex] {
					return true
				} else if ret[i][keyIndex] > ret[j][keyIndex] {
					return false
				}
			}
			return false
		})
	}
	return ret
}

func GetIndexSlice(source []string, target []string) []int {
	var ret []int
	for _, i := range target {
		index := slices.Index(source, i)
		if index != -1 {
			ret = append(ret, index)
		}
	}
	return ret
}

func Map2List(row map[string]string, headers []string) []string {
	var ret []string
	for _, j := range headers {
		ret = append(ret, row[j])
	}
	return ret
}
