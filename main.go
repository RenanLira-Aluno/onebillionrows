package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Measurement struct {
	Min   float64
	Max   float64
	Sum   float64
	Count int64
}

func main() {
	start := time.Now()

	wg := sync.WaitGroup{}
	wg.Add(1)

	measurements, err := os.Open("measurements.txt")
	if err != nil {
		panic(err)
	}
	defer measurements.Close()

	dados := make(map[string]*Measurement)
	listNames := make(chan string)
	scanner := bufio.NewScanner(measurements)

	for scanner.Scan() {
		rawData := scanner.Text()

		line := strings.TrimSpace(rawData)

		var location string
		var temp string
		var fimLocation bool
		for _, word := range line {
			if word == ';' {
				fimLocation = true
				continue
			}

			if !fimLocation {
				getRuneAndConcatenate(word, &location)
			} else {
				getRuneAndConcatenate(word, &temp)
			}
		}

		go func() {
			if m, ok := dados[location]; !ok {
				tempFloat, _ := strconv.ParseFloat(temp, 64)
				dados[location] = &Measurement{
					Min:   tempFloat,
					Max:   tempFloat,
					Sum:   tempFloat,
					Count: 1,
				}
				listNames <- location
			} else {
				tempFloat, _ := strconv.ParseFloat(temp, 64)
				m.Min = min(m.Min, tempFloat)
				m.Max = max(m.Max, tempFloat)
				m.Sum += tempFloat
				m.Count++
			}
		}()


	}

	var sortList []string = make([]string, 0, len(dados))

	for name := range listNames {
		if len(sortList) == 0 {
			sortList = append(sortList, name)
		} else {
			sortList = parallelMergeSort(append(sortList, name), &wg)
			wg.Wait()
		}
	}

	fmt.Printf("{")
	for _, location := range sortList {
		m := dados[location]
		fmt.Printf("%s=%.1f/%.1f/%.1f, ", location, m.Min, m.Max, m.Sum/float64(m.Count))
	}
	fmt.Printf("}\n")

	fmt.Println("Tempo de execução: ", time.Since(start))
}

func getRuneAndConcatenate(word rune, str *string) {
	*str += string(word)
}

// merge combina dois slices já ordenados em um único slice ordenado
func merge(left, right []string) []string {
	result := make([]string, 0, len(left)+len(right))
	i, j := 0, 0

	for i < len(left) && j < len(right) {
		if left[i] <= right[j] {
			result = append(result, left[i])
			i++
		} else {
			result = append(result, right[j])
			j++
		}
	}

	// Adiciona os elementos restantes
	result = append(result, left[i:]...)
	result = append(result, right[j:]...)
	return result
}

// parallelMergeSort realiza o Merge Sort com paralelismo
func parallelMergeSort(arr []string, wg *sync.WaitGroup) []string {
	// Finaliza o WaitGroup quando a goroutine termina
	defer wg.Done()

	// Base da recursão: arrays de tamanho 1 já estão ordenados
	if len(arr) <= 1 {
		return arr
	}

	// Divide o array em duas partes
	mid := len(arr) / 2

	// WaitGroups para gerenciar as goroutines
	var leftWg, rightWg sync.WaitGroup
	leftWg.Add(1)
	rightWg.Add(1)

	var left, right []string

	// Lida com cada parte em goroutines
	go func() {
		left = parallelMergeSort(arr[:mid], &leftWg)
	}()
	go func() {
		right = parallelMergeSort(arr[mid:], &rightWg)
	}()

	// Aguarda as goroutines terminarem
	leftWg.Wait()
	rightWg.Wait()

	// Combina os resultados
	return merge(left, right)
}
