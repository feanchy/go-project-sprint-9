package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"
)

// Generator генерирует числа 1,2,3... и вызывает fn для каждого числа
func Generator(ctx context.Context, ch chan<- int64, fn func(int64)) {
	defer close(ch)

	var count int64 = 1

	for {
		select {
		case <-ctx.Done():
			return
		default:
			ch <- count
			fn(count)
			count++
		}
	}
}

// Worker читает из in и пишет в out
func Worker(in <-chan int64, out chan<- int64) {
	defer close(out)

	for v := range in {
		out <- v
		time.Sleep(time.Millisecond)
	}
}

func main() {
	chIn := make(chan int64)
	var mu sync.Mutex

	// контекст с таймаутом 1 секунда
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	var inputSum int64
	var inputCount int64

	// генератор
	go Generator(ctx, chIn, func(i int64) {
		mu.Lock()
		inputSum += i
		inputCount++
		mu.Unlock()
	})

	const NumOut = 5

	outs := make([]chan int64, NumOut)

	for i := 0; i < NumOut; i++ {
		outs[i] = make(chan int64)
		go Worker(chIn, outs[i])
	}

	amounts := make([]int64, NumOut)
	chOut := make(chan int64, NumOut)

	var wg sync.WaitGroup

	// fan-in
	for i := 0; i < NumOut; i++ {
		wg.Add(1)

		go func(in <-chan int64, i int) {
			defer wg.Done()

			for v := range in {
				mu.Lock()
				amounts[i]++
				mu.Unlock()

				chOut <- v
			}
		}(outs[i], i)
	}

	// закрытие chOut после завершения всех fan-in горутин
	go func() {
		wg.Wait()
		close(chOut)
	}()

	var sum int64
	var count int64

	// читаем результат
	for v := range chOut {
		sum += v
		count++
	}

	fmt.Println("Количество чисел", inputCount, count)
	fmt.Println("Сумма чисел", inputSum, sum)
	fmt.Println("Разбивка по каналам", amounts)

	// проверки
	if inputSum != sum {
		log.Fatalf("Ошибка: суммы не равны %d != %d", inputSum, sum)
	}
	if inputCount != count {
		log.Fatalf("Ошибка: количество не равно %d != %d", inputCount, count)
	}

	var check int64
	for _, v := range amounts {
		check += v
	}
	if check != inputCount {
		log.Fatalf("Ошибка: распределение по каналам неверное")
	}
}
