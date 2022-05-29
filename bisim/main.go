package main

import (
	"fmt"
	"math/rand"
)

func main() {
	len := 1000
	for k := 1; k < 1001; k++ {
		sum := 0
		for i := 0; i < len; i++ {
			sum += getLen4(k)
		}
		fmt.Printf("%d %f\n", k, float64(sum)/float64(len))
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

func getLen4(k int) int {
	count0 := 0
	count1 := 0
	count2 := 0
	for count0 < k || count1 < k || count2 < k {
		bi := rand.Intn(3)
		switch bi {
		case 0:
			count0++
		case 1:
			count1++
		case 2:
			count2++
		}
	}
	//	fmt.Printf("len: %d\n", count0+count1)
	return count0 + count1 + count2
}
