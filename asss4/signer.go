package main

import (
	"runtime"
	"sort"
	"strconv"
	"sync"
)

func ExecutePipeline(jobs ...job) {
	wg := &sync.WaitGroup{}
	ch := make([]chan interface{}, len(jobs)+1)
	for i := 0; i < len(jobs)+1; i++ {
		ch[i] = make(chan interface{}, 6)
	}
	for i, jb := range jobs {
		in := ch[i]
		out := ch[i+1]
		wg.Add(1)
		go func(j job, in, out chan interface{}, wg *sync.WaitGroup) {
			j(in, out)
			close(out)
			wg.Done()
		}(jb, in, out, wg)
	}
	wg.Wait()
}

func SingleHash(in, out chan interface{}) {
	mu := &sync.Mutex{}
	mu2 := &sync.Mutex{}
	wg := &sync.WaitGroup{}
	for input := range in {
		mu2.Lock()
		wg.Add(1)
		go func(wg *sync.WaitGroup) {
			ch2 := make(chan string)
			ch3 := make(chan string)
			datain := input.(int)
			data := strconv.Itoa(datain)
			mu.Lock()
			md5data := DataSignerMd5(data)
			mu.Unlock()

			go fcrc32(ch2, md5data)
			go fcrc32(ch3, data)

			result1 := <-ch3
			result2 := <-ch2
			result := result1 + "~" + result2
			out <- result
			wg.Done()
		}(wg)
		mu2.Unlock()
		runtime.Gosched()
	}
	wg.Wait()
}

func MultiHash(in, out chan interface{}) {
	wg := &sync.WaitGroup{}
	for input := range in {
		wg.Add(1)
		go func(wg *sync.WaitGroup) {
			data := input.(string)
			var answer []string
			ch := make([]chan string, 6)
			for th := 0; th < 6; th++ {
				ch[th] = make(chan string)
				go fcrc32(ch[th], strconv.Itoa(th)+data)
			}
			for th := 0; th < 6; th++ {
				answer = append(answer, <-ch[th])
			}
			ans := ""
			for _, value := range answer {
				ans = ans + value
			}
			out <- ans
			wg.Done()
		}(wg)
		runtime.Gosched()
	}
	wg.Wait()
}

func CombineResults(in, out chan interface{}) {
	var data []string
	for input := range in {
		dataOne := input
		data = append(data, dataOne.(string))
	}
	sort.Strings(data)
	answer := data[0]
	for i := 1; i < len(data); i++ {
		answer = answer + "_" + data[i]
	}
	out <- answer
}

func fcrc32(ch chan<- string, data string) {
	temp := DataSignerCrc32(data)
	ch <- temp
}
