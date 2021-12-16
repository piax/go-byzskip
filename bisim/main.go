package main

import (
	"fmt"
	"math/rand"
)

func main() {
	len := 10000
	for k := 1; k < 10; k++ {
		sum := 0
		for i := 0; i < len; i++ {
			sum += getLen(k)
		}
		fmt.Printf("%d: %f\n", k, float64(sum)/float64(len))
		fmt.Printf("%d: %f\n", k, float64(k)*2.68)
	}
}

func getLen(k int) int {
	count0 := 0
	count1 := 0
	for count0 < k || count1 < k {
		bi := rand.Intn(2)
		if bi == 0 {
			count0++
		} else {
			count1++
		}
		//fmt.Printf("bi: %d\n", bi)
	}
	//	fmt.Printf("len: %d\n", count0+count1)
	return count0 + count1
}
